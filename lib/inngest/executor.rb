# frozen_string_literal: true

require "digest"
require "json"

module Inngest
  # Runs one call of a function: replays memoized steps, then either finishes
  # the function or stops at the first new step and reports it.
  #
  # This class knows nothing about transports: Connect and the HTTP handler
  # turn the Result into a response. It also runs middleware hooks.
  class Executor
    # status: 200 (function finished), 206 (steps reported) or 500 (error).
    # body: JSON string.
    Result = Struct.new(:status, :body, :no_retry, :retry_after, keyword_init: true)

    # A memoized step result.
    Memo = Struct.new(:step_id, :data, :error, keyword_init: true) do
      def value!
        return data unless error

        raise StepError.new(step_id: step_id, name: error["name"] || "Error",
                            message: error["message"] || "step failed", stack: error["stack"])
      end
    end

    # A step reported to Inngest.
    Op = Struct.new(:id, :op, :name, :display_name, :opts, :userland, keyword_init: true) do
      def to_h
        { "id" => id, "op" => op, "name" => name, "displayName" => display_name, "opts" => opts,
          "userland" => userland }.compact
      end
    end

    # Thrown (not raised) to end a call from deep inside user code, carrying the
    # steps to report. `throw` is used so `rescue` in user code can't swallow it.
    # `step_run` is set when a step's block ran in this call.
    Interrupt = Struct.new(:ops, :step_run)

    # The outcome of running a step's block, before it's reported.
    StepRun = Struct.new(:op, :output, :error)

    # The function raised.
    Failed = Struct.new(:error)

    # Thrown inside step.parallel when Inngest is targeting a step in a
    # different branch.
    SKIP = Object.new.freeze

    # A parallel branch that ran to completion.
    Completed = Struct.new(:value)

    attr_reader :client

    # The exception the function raised, if any (used by Inngest::Testing).
    attr_reader :error

    # @param payload [Hash] the parsed execution request body
    # @param step_id [String, nil] the step Inngest wants to run ("step" or nil
    #   means none in particular)
    # @param failure [Boolean] run the function's #on_failure handler
    # @param request [Object] the transport's request, passed to middleware
    def initialize(client:, function_class:, payload:, step_id: nil, failure: false, request: nil)
      @client = client
      @function_class = function_class
      @payload = payload
      @target_id = step_id == "step" ? nil : step_id
      @failure = failure
      @request = request
      @step_counts = Hash.new(0)
      @in_step = false
      @parallel_depth = 0
      @execution_started = false
      @tag = Object.new
    end

    def app_id
      client.app_id
    end

    def call
      @middleware = (client.middleware + @function_class.middleware_entries).map do |entry|
        Middleware.build(entry, client: client, function: @function_class, request: @request)
      end
      load_request
      start_execution if @memos.empty?

      outcome = run_function
      start_execution
      each_middleware(&:after_execution)

      output = middleware_output(outcome)
      each_middleware { |m| m.transform_output(output) }
      result = build_result(outcome, output)
      each_middleware(&:before_response)
      result
    rescue StandardError => e
      @error ||= e
      error_result(e)
    end

    # Sends events with this call's middleware hooks.
    def send_events(events)
      client.deliver_events(events, @middleware)
    end

    # --- Called by Step ---

    def new_op(user_id, opcode, name: nil, opts: nil)
      raise NestedStepError, "steps can't be used inside step.run (\"#{user_id}\")" if @in_step

      user_id = user_id.to_s
      raise ArgumentError, "step ID can't be empty" if user_id.empty?

      count = (@step_counts[user_id] += 1)
      index = count > 1 ? count - 1 : nil
      hashed = Digest::SHA1.hexdigest(index ? "#{user_id}:#{index}" : user_id)

      Op.new(id: hashed, op: opcode, name: name || user_id, display_name: user_id, opts: opts,
             userland: { "id" => user_id, "index" => index }.compact)
    end

    # Returns the step's Memo if it already ran. Returns nil if a StepRun should
    # run now. Otherwise reports the step to Inngest, which ends this call.
    def resolve(op)
      memo = @memos.delete(op.id)
      if memo
        # Once every stored step has been replayed, the code that follows is new.
        start_execution if @memos.empty?
        return memo
      end

      start_execution

      if @target_id
        if @target_id != op.id
          # In a parallel group the target may be in another branch. Otherwise
          # this step is unexpected.
          throw @tag, SKIP if in_parallel?
          report_not_found
        end
        return op.op == "StepRun" ? nil : report([op])
      end

      # New steps found in parallel are collected and planned together, and
      # after parallel steps Inngest asks for every step to be planned first.
      if op.op == "StepRun" && (in_parallel? || @disable_immediate_execution)
        op.op = "StepPlanned"
        report([op])
      end

      op.op == "StepRun" ? nil : report([op])
    end

    # Runs each callable, collecting the new steps they find instead of
    # stopping at the first. Returns the callables' results once all of them
    # have finished; until then, reports every new step at once so Inngest
    # runs them concurrently.
    def parallel(callables, mode:)
      raise NestedStepError, "step.parallel can't be used inside step.run" if @in_step
      raise ArgumentError, "unknown parallel mode #{mode.inspect}" unless %i[wait race].include?(mode)

      results = []
      ops = []
      skipped = false

      @parallel_depth += 1
      begin
        callables.each do |callable|
          outcome = catch(@tag) { Completed.new(callable.call) }
          case outcome
          when Completed then results << outcome.value
          # A targeted step ran; report it right away.
          when Interrupt then outcome.step_run ? throw(@tag, outcome) : ops.concat(outcome.ops)
          else skipped = true
          end
        end
      ensure
        @parallel_depth -= 1
      end

      if ops.any?
        if mode == :race
          # Ask Inngest to continue as soon as any branch's step finishes.
          ops = ops.map { |op| op.merge("opts" => (op["opts"] || {}).merge("parallelMode" => "race")) }
        end
        throw @tag, Interrupt.new(ops)
      end
      # Inngest targeted a step, but it wasn't in any branch.
      report_not_found if skipped

      results
    end

    # Runs a StepRun's block and reports its result, ending this call.
    def execute(op)
      @in_step = true
      begin
        output = yield
      rescue NonRetriableError, RetryAfterError
        raise
      rescue StandardError => e
        throw @tag, Interrupt.new([op.to_h], StepRun.new(op, nil, e))
      ensure
        @in_step = false
      end

      throw @tag, Interrupt.new([op.to_h], StepRun.new(op, output, nil))
    end

    private

    def run_function
      fn = @function_class.new(event: @event, events: @events, step: Step.new(self), run_id: @run_id,
                               attempt: @attempt, logger: client.logger, context: @context)
      catch(@tag) do
        output = fn.inngest_dispatch(@failure ? :on_failure : :call)
        # Inngest asked for a step we never reached.
        report_not_found if @target_id
        Completed.new(output)
      end
    rescue StandardError => e
      Failed.new(e)
    end

    # Runs after_memoization and before_execution, once, when the function
    # reaches code that hasn't run before.
    def start_execution
      return if @execution_started

      @execution_started = true
      each_middleware(&:after_memoization)
      each_middleware(&:before_execution)
    end

    def each_middleware(&)
      @middleware.each(&)
    end

    def middleware_output(outcome)
      case outcome
      when Completed then Middleware::Output.new(data: outcome.value, finished: true)
      when Failed then Middleware::Output.new(error: outcome.error, finished: true)
      when Interrupt
        run = outcome.step_run
        return Middleware::Output.new unless run

        step = Middleware::StepInfo.new(id: run.op.display_name, hashed_id: run.op.id)
        Middleware::Output.new(data: run.output, error: run.error, step: step)
      else Middleware::Output.new
      end
    end

    # Turns the (possibly transformed) outcome into the response for Inngest.
    def build_result(outcome, output)
      case outcome
      when Completed, Failed
        if output.error
          @error = output.error
          error_result(output.error)
        else
          done(output.data)
        end
      when Interrupt
        run = outcome.step_run
        return steps_result(outcome.ops) unless run

        steps_result([step_run_op(run.op, output)])
      else steps_result([{ "id" => @target_id, "op" => "StepNotFound" }])
      end
    end

    def step_run_op(op, output)
      if output.error
        op.op = last_attempt? ? "StepFailed" : "StepError"
        op.to_h.merge("error" => error_hash(output.error))
      else
        op.to_h.merge("data" => serialize(output.data, "step \"#{op.display_name}\""))
      end
    end

    def load_request
      ctx = @payload["ctx"] || {}
      @run_id = ctx["run_id"]
      @attempt = ctx["attempt"] || 0
      @max_attempts = ctx["max_attempts"]
      @disable_immediate_execution = ctx["disable_immediate_execution"] == true

      events = @payload["events"]
      steps = @payload["steps"] || {}
      if ctx["use_api"]
        # The batch or step state was too big to send inline.
        events = client.api_get("/v0/runs/#{@run_id}/batch")
        steps = client.api_get("/v0/runs/#{@run_id}/actions")
      end

      event = Event.from_h(@payload["event"])
      events = Array(events).map { |e| Event.from_h(e) }
      events = [event].compact if events.empty?

      input = Middleware::Input.new(event: event, events: events, steps: steps.dup, context: {},
                                    function: @function_class, run_id: @run_id, attempt: @attempt)
      each_middleware { |m| m.transform_input(input) }

      @event = input.event
      @events = input.events
      @context = input.context
      @memos = input.steps.to_h { |id, value| [id, parse_memo(id, value)] }
    end

    # Most steps memoize { data } or { error }, but WaitForEvent memoizes the
    # bare event (or null on timeout) and Sleep memoizes null.
    def parse_memo(id, value)
      if value.is_a?(Hash) && !value.empty? && (value.keys - %w[data error]).empty?
        Memo.new(step_id: id, data: value["data"], error: value["error"])
      else
        Memo.new(step_id: id, data: value)
      end
    end

    def last_attempt?
      !@max_attempts.nil? && @attempt + 1 >= @max_attempts
    end

    def in_parallel?
      @parallel_depth.positive?
    end

    def report(ops)
      throw @tag, Interrupt.new(ops.map(&:to_h))
    end

    def report_not_found
      report([{ "id" => @target_id, "op" => "StepNotFound" }])
    end

    def steps_result(ops)
      # A step that failed for good shouldn't be retried.
      no_retry = ops.size == 1 && ops.first["op"] == "StepFailed"
      Result.new(status: 206, body: JSON.generate(ops), no_retry: no_retry)
    end

    def done(output)
      Result.new(status: 200, body: JSON.generate(serialize(output, "function output")), no_retry: false)
    end

    # Round-trips a value through JSON so callers always see what Inngest will
    # replay later.
    def serialize(value, label)
      value = value.as_json if value.respond_to?(:as_json)
      JSON.parse(JSON.generate(value))
    rescue JSON::GeneratorError, JSON::NestingError, TypeError => e
      raise NonRetriableError, "#{label} isn't JSON-serializable: #{e.message}"
    end

    def error_result(error)
      result = Result.new(status: 500, body: JSON.generate(error_hash(error)), no_retry: non_retriable?(error))
      result.retry_after = Duration.to_iso8601(error.retry_after) if error.is_a?(RetryAfterError)
      result
    end

    def non_retriable?(error)
      error.is_a?(NonRetriableError) || error.is_a?(StepError)
    end

    def error_hash(error)
      name = error.is_a?(StepError) ? error.error_name : error.class.name
      stack = error.is_a?(StepError) ? error.error_stack : error.backtrace&.join("\n")
      { "name" => name, "message" => error.message, "stack" => stack }.compact
    end
  end
end
