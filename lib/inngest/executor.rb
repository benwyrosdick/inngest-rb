# frozen_string_literal: true

require "digest"
require "json"

module Inngest
  # Runs one call of a function: replays memoized steps, then either finishes
  # the function or stops at the first new step and reports it.
  #
  # This class knows nothing about transports. Connect (and, later, an HTTP
  # handler) turn the Result into a response.
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

    # Signals the end of a call from deep inside user code. `throw` is used
    # instead of an exception so `rescue => e` in user code can't swallow it.
    Interrupt = Struct.new(:result)

    attr_reader :client

    # @param payload [Hash] the parsed execution request body
    # @param step_id [String, nil] the step Inngest wants to run ("step" or nil
    #   means none in particular)
    # @param failure [Boolean] run the function's #on_failure handler
    def initialize(client:, function_class:, payload:, step_id: nil, failure: false)
      @client = client
      @function_class = function_class
      @payload = payload
      @target_id = step_id == "step" ? nil : step_id
      @failure = failure
      @step_counts = Hash.new(0)
      @in_step = false
      @tag = Object.new
    end

    def app_id
      client.app_id
    end

    def call
      load_request
      fn = @function_class.new(event: @event, events: @events, step: Step.new(self), run_id: @run_id,
                               attempt: @attempt, logger: client.logger)

      catch(@tag) do
        output = fn.inngest_dispatch(@failure ? :on_failure : :call)
        # Inngest asked for a step we never reached.
        report([Op.new(id: @target_id, op: "StepNotFound")]) if @target_id
        done(output)
      end
    rescue StandardError => e
      error_result(e)
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
      return memo if memo

      if @target_id
        # Inngest asked for a different step, so this one is unexpected.
        report([Op.new(id: @target_id, op: "StepNotFound")]) if @target_id != op.id
        return op.op == "StepRun" ? nil : report([op])
      end

      if op.op == "StepRun" && @disable_immediate_execution
        op.op = "StepPlanned"
        report([op])
      end

      op.op == "StepRun" ? nil : report([op])
    end

    # Runs a StepRun's block and reports its result, ending this call.
    def execute(op)
      @in_step = true
      begin
        output = yield
      rescue NonRetriableError, RetryAfterError
        raise
      rescue StandardError => e
        op.op = last_attempt? ? "StepFailed" : "StepError"
        report([op.to_h.merge("error" => error_hash(e))])
      ensure
        @in_step = false
      end

      report([op.to_h.merge("data" => serialize(output, "step \"#{op.display_name}\""))])
    end

    private

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

      @event = Event.from_h(@payload["event"])
      @events = Array(events).map { |e| Event.from_h(e) }
      @events = [@event].compact if @events.empty?
      @memos = steps.to_h { |id, value| [id, parse_memo(id, value)] }
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

    def report(ops)
      throw @tag, Result.new(status: 206, body: JSON.generate(ops.map(&:to_h)), no_retry: false)
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
