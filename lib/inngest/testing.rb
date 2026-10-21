# frozen_string_literal: true

require "inngest"
require "logger"
require "securerandom"

module Inngest
  # Runs functions locally, the way Inngest would, so they can be tested
  # without a server:
  #
  #   result = Inngest::Testing.run(SendWelcomeEmail,
  #     event: { name: "user/signed_up", data: { "user_id" => 1 } },
  #     steps: { "load-user" => { "email" => "ada@example.com" } })
  #
  #   result.completed?         # => true
  #   result.output             # => the function's return value (as JSON)
  #   result.step("send-email") # => the step's result
  #   result.events             # => events sent with step.send_event
  #
  # Steps run for real unless stubbed with `steps:` (keyed by step ID). Sleeps
  # finish immediately. step.wait_for_event needs a stub (an event hash, or
  # nil for a timeout). step.invoke runs the invoked function locally unless
  # stubbed. A stub that is an exception makes the step fail; a Proc is called
  # to produce the value.
  #
  # Failing steps and functions are retried like on Inngest, up to the
  # function's retries (pass max_attempts: 1 to turn retries off). Events sent
  # by functions are recorded, not sent.
  module Testing
    # A step needs a stub to run locally (e.g. wait_for_event).
    class UnstubbedStepError < Inngest::Error; end

    MAX_CALLS = 1_000

    # A step that ran (or was stubbed) during a test run.
    StepRecord = Struct.new(:id, :type, :output, :error, :stubbed, keyword_init: true) do
      def stubbed?
        stubbed
      end
    end

    # The outcome of Testing.run.
    class Result
      # :completed or :failed.
      attr_reader :status, :output, :error, :steps, :events, :calls

      def initialize(status:, output:, error:, steps:, events:, calls:)
        @status = status
        @output = output
        @error = error
        @steps = steps
        @events = events
        @calls = calls
      end

      def completed?
        @status == :completed
      end

      def failed?
        @status == :failed
      end

      # The result of the step with this ID (the last one, if it repeats).
      def step(id)
        record = steps.reverse.find { |s| s.id == id.to_s && s.error.nil? }
        raise KeyError, "no step #{id.inspect} ran (steps: #{step_ids.join(", ")})" unless record

        record.output
      end

      def step_ids
        steps.map(&:id).uniq
      end

      def ran_step?(id)
        step_ids.include?(id.to_s)
      end
    end

    # A client that records events instead of sending them.
    class Client < Inngest::Client
      attr_reader :sent_events

      def initialize(**options)
        super(dev: true, event_key: "test", **options)
        @sent_events = []
      end

      def post_events(events)
        @sent_events.concat(events)
        events.map { "test-event-#{SecureRandom.hex(6)}" }
      end
    end

    module_function

    # Runs a function to completion (or failure).
    #
    # @param function_class [Class] the Inngest::Function to run
    # @param event [Hash, Inngest::Event] the triggering event; defaults to the
    #   function's first event trigger with empty data
    # @param events [Array] for batched functions, all events
    # @param steps [Hash] stubbed step results, keyed by step ID
    # @param client [Inngest::Client] defaults to a test client that uses the
    #   default client's app ID and middleware
    # @param max_attempts [Integer] attempts per step and for the function;
    #   defaults to the function's retries + 1
    # @param failure [Boolean] run #on_failure instead of #call
    # @param error [Exception] with failure: true, the error the function failed with
    def run(function_class, event: nil, events: nil, steps: {}, client: nil, max_attempts: nil, failure: false,
            error: nil)
      client ||= default_client
      events = (events || [event || default_event(function_class)]).map { |e| normalize_event(e) }
      events = [failure_event(function_class, client, events.first, error)] if failure
      Run.new(function_class: function_class, client: client, events: events,
              stubs: steps.transform_keys(&:to_s), failure: failure,
              max_attempts: max_attempts || (function_class.options.fetch("retries", 3) + 1)).call
    end

    def default_client
      base = begin
        Inngest.client
      rescue ConfigError
        nil
      end
      Client.new(app_id: base&.app_id || "test-app", middleware: base&.middleware || [],
                 logger: Logger.new(IO::NULL))
    end

    def default_event(function_class)
      name = function_class.triggers.find { |t| t["event"] }&.fetch("event") || "inngest/scheduled.timer"
      { "name" => name, "data" => {} }
    end

    def normalize_event(event)
      hash = (event.respond_to?(:to_h) ? event.to_h : event).transform_keys(&:to_s)
      hash = JSON.parse(JSON.generate(hash)) # String keys all the way down, like a real event.
      { "id" => SecureRandom.uuid, "data" => {}, "ts" => (Time.now.to_f * 1000).to_i }.merge(hash)
    end

    def failure_event(function_class, client, event, error)
      error ||= StandardError.new("function failed")
      normalize_event(
        "name" => Function::FAILURE_EVENT,
        "data" => {
          "error" => { "name" => error.class.name, "message" => error.message },
          "event" => event,
          "function_id" => function_class.fully_qualified_id(client.app_id),
          "run_id" => "test-run"
        }
      )
    end

    # Plays the part of Inngest's executor for one run.
    class Run
      STEP_TYPES = {
        "StepPlanned" => :run, "StepRun" => :run, "StepError" => :run, "StepFailed" => :run, "Sleep" => :sleep,
        "WaitForEvent" => :wait_for_event, "InvokeFunction" => :invoke
      }.freeze

      def initialize(function_class:, client:, events:, stubs:, failure:, max_attempts:)
        @function_class = function_class
        @client = client
        @events = events
        @stubs = stubs
        @failure = failure
        @max_attempts = max_attempts
        @memos = {}
        @stack = []
        @records = []
        @planned = []
        @attempt = 0
        @calls = 0
      end

      def call
        loop do
          @calls += 1
          raise Error, "function didn't finish after #{MAX_CALLS} calls" if @calls > MAX_CALLS

          target = @planned.shift
          executor = Executor.new(client: @client, function_class: @function_class, payload: payload,
                                  step_id: target, failure: @failure)
          result = executor.call
          body = JSON.parse(result.body)

          case result.status
          when 200 then return finish(:completed, output: body)
          when 206 then handle_ops(body, target)
          else
            error = executor.error || Error.new(body.is_a?(Hash) ? body["message"] : body.to_s)
            return finish(:failed, error: error) if result.no_retry || @attempt + 1 >= @max_attempts

            retry_call(target)
          end
        end
      end

      private

      def payload
        {
          "event" => @events.first,
          "events" => @events,
          "steps" => @memos,
          "ctx" => {
            "run_id" => "test-run",
            "attempt" => @attempt,
            "max_attempts" => @max_attempts,
            # Every step is planned before it runs, so stubs can replace it.
            "disable_immediate_execution" => true,
            "use_api" => false,
            "stack" => { "stack" => @stack, "current" => @stack.size }
          }
        }
      end

      def finish(status, output: nil, error: nil)
        events = @client.respond_to?(:sent_events) ? @client.sent_events : []
        Result.new(status: status, output: output, error: error, steps: @records, events: events, calls: @calls)
      end

      def retry_call(target)
        @attempt += 1
        @planned.unshift(target) if target
      end

      def handle_ops(ops, target)
        ops.each { |op| handle_op(op, target) }
      end

      def handle_op(op, target)
        id = op["id"]
        stub_key = stub_key_for(op)
        return memoize(op, stub_value(stub_key, op), stubbed: true) if stub_key && op["op"] != "StepRun"

        case op["op"]
        when "StepPlanned" then @planned << id
        when "StepRun" then memoize(op, { "data" => op["data"] })
        when "StepError"
          @records << record(op, error: op["error"])
          retry_call(target)
        when "StepFailed" then memoize(op, { "error" => op["error"] })
        when "Sleep" then memoize(op, nil)
        when "WaitForEvent"
          raise UnstubbedStepError, "step.wait_for_event(#{op["displayName"].inspect}) needs a stub: pass " \
                                    "steps: { #{op["displayName"].inspect} => event_hash } (or nil for a timeout)"
        when "InvokeFunction" then memoize(op, invoke(op))
        when "StepNotFound"
          raise Error, "Inngest asked for a step the function didn't reach; is the function deterministic?"
        else raise Error, "can't run #{op["op"]} steps locally; stub #{op["displayName"].inspect}"
        end
      end

      def memoize(op, memo, stubbed: false)
        @memos[op["id"]] = memo
        @stack << op["id"]
        @attempt = 0
        output = memo.is_a?(Hash) && (memo.keys - %w[data error]).empty? ? memo["data"] : memo
        error = memo.is_a?(Hash) && (memo.keys - %w[data error]).empty? ? memo["error"] : nil
        @records << record(op, output: output, error: error, stubbed: stubbed)
      end

      def record(op, output: nil, error: nil, stubbed: false)
        StepRecord.new(id: op["displayName"], type: STEP_TYPES.fetch(op["op"], op["op"]), output: output,
                       error: error, stubbed: stubbed)
      end

      def stub_key_for(op)
        name = op["displayName"].to_s
        index = op.dig("userland", "index")
        [index && "#{name}:#{index}", name].compact.find { |key| @stubs.key?(key) }
      end

      # Builds the stored step result Inngest would have for a stubbed step.
      def stub_value(key, op)
        value = @stubs[key]
        value = value.call if value.is_a?(Proc)
        return { "error" => { "name" => value.class.name, "message" => value.message } } if value.is_a?(Exception)

        value = value.to_h if value.is_a?(Event)
        value = JSON.parse(JSON.generate(value))
        case op["op"]
        when "WaitForEvent" then value && Testing.normalize_event(value)
        when "Sleep" then nil
        else { "data" => value }
        end
      end

      def qualified_id(function_class)
        function_class.fully_qualified_id(@client.app_id)
      rescue ConfigError
        nil # Anonymous classes without an ID can't be invoked.
      end

      def invoke(op)
        function_id = op.dig("opts", "function_id")
        target = Function.registered.find { |fn| qualified_id(fn) == function_id }
        unless target
          raise UnstubbedStepError, "step.invoke(#{op["displayName"].inspect}) calls #{function_id}, which " \
                                    "isn't loaded; stub it with steps: { #{op["displayName"].inspect} => result }"
        end

        event = { "name" => "inngest/function.invoked", "data" => op.dig("opts", "payload", "data") || {} }
        result = Testing.run(target, event: event, client: @client)
        return { "data" => result.output } if result.completed?

        { "error" => { "name" => result.error.class.name, "message" => result.error.message } }
      end
    end

    # Adds run_inngest_function to test cases (RSpec or Minitest).
    module Helpers
      def run_inngest_function(function_class, **options)
        Testing.run(function_class, **options)
      end
    end
  end
end
