# frozen_string_literal: true

module Inngest
  # Base class for middleware: code that runs around function calls and event
  # sends, e.g. for logging, error reporting, tracing or setting up context.
  #
  #   class TenantMiddleware < Inngest::Middleware
  #     def transform_input(input)
  #       input.context[:tenant] = Tenant.find(input.event.data["tenant_id"])
  #     end
  #   end
  #
  #   Inngest.configure(app_id: "shop", middleware: [TenantMiddleware])  # every function
  #   class SyncOrders < Inngest::Function
  #     middleware AuditMiddleware                                        # one function
  #     def call(tenant:) = ...
  #   end
  #
  # A new instance is created for each call of a function (one call per step),
  # so instance variables are safe to use between hooks. Client middleware runs
  # before function middleware, each in the order it was added.
  #
  # Hooks run once per call, in this order:
  #
  # 1. transform_input(input): before the function runs. Can change the event,
  #    the stored step results and add values to the function's context.
  # 2. after_memoization: once stored steps have been replayed and new code is
  #    about to run.
  # 3. before_execution: right before new code runs.
  # 4. after_execution: after the new code stops (the function returned or
  #    raised, a step ran, or new steps were found).
  # 5. transform_output(output): can change the function's or step's result
  #    or error.
  # 6. before_response: just before the result goes back to Inngest.
  #
  # before_send_events(events) and after_send_events(result) run around every
  # event send.
  class Middleware
    # What transform_input can change.
    #
    # - event, events: the triggering event(s)
    # - steps: stored step results, keyed by hashed step ID
    # - context: values passed to the function as `context[...]`, or as keyword
    #   arguments if #call declares them
    Input = Struct.new(:event, :events, :steps, :context, :function, :run_id, :attempt, keyword_init: true)

    # What transform_output can change: `data` and `error` (an exception).
    #
    # - finished? is true when the function itself returned or raised.
    # - step is set when a step ran in this call: StepInfo with the step's ID.
    # - Otherwise the call found new steps to schedule (e.g. a sleep), and
    #   there's nothing to transform.
    class Output
      attr_accessor :data, :error
      attr_reader :step

      def initialize(data: nil, error: nil, step: nil, finished: false)
        @data = data
        @error = error
        @step = step
        @finished = finished
      end

      def finished?
        @finished
      end
    end

    StepInfo = Struct.new(:id, :hashed_id, keyword_init: true)

    # What after_send_events receives. `error` is set if sending failed; some
    # events may still have been sent (their IDs are in `ids`).
    SendResult = Struct.new(:ids, :error, keyword_init: true)

    # Builds a middleware instance from a class (or a callable that takes the
    # same keyword arguments, for middleware that needs configuration).
    def self.build(entry, client:, function: nil, request: nil)
      if entry.is_a?(Class)
        entry.new(client: client, function: function, request: request)
      else
        entry.call(client: client, function: function, request: request)
      end
    end

    # The function class being called (nil when sending events outside a
    # function), and the transport's request: a Rack::Request in serve mode or
    # the gateway's request message with Connect.
    attr_reader :client, :function, :request

    def initialize(client:, function: nil, request: nil)
      @client = client
      @function = function
      @request = request
    end

    def transform_input(input); end

    def after_memoization; end

    def before_execution; end

    def after_execution; end

    def transform_output(output); end

    def before_response; end

    # Receives the events as an array of hashes, which can be changed.
    def before_send_events(events); end

    def after_send_events(result); end
  end
end
