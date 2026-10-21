# frozen_string_literal: true

module Inngest
  # Step tools, available as `step` inside a function.
  #
  # Each step has an ID that must be stable across calls of the function.
  # Reusing an ID within one run is allowed (e.g. in a loop) as long as the
  # function is deterministic.
  #
  # A step's return value is stored as JSON and replayed on later calls, so
  # step.run always returns JSON-compatible data (string keys, no symbols or
  # custom objects), even the first time it runs.
  class Step
    def initialize(execution)
      @execution = execution
    end

    # Runs the block as a retriable, memoized step and returns its result.
    #
    #   user = step.run("load-user") { User.find(id).slice("id", "email") }
    def run(id, &block)
      raise ArgumentError, "step.run requires a block" unless block

      op = @execution.new_op(id, "StepRun")
      memo = @execution.resolve(op)
      return memo.value! if memo

      @execution.execute(op, &block)
    end

    # Pauses the run for a duration: a time string ("2h30m"), a number of
    # seconds, or an ActiveSupport::Duration (1.day).
    def sleep(id, duration)
      time_string = Duration.to_time_string(duration)
      op = @execution.new_op(id, "Sleep", name: time_string, opts: { "duration" => time_string })
      @execution.resolve(op)&.value!
      nil
    end

    # Pauses the run until the given Time.
    def sleep_until(id, time)
      iso = Duration.to_iso8601(time)
      op = @execution.new_op(id, "Sleep", name: iso, opts: { "duration" => iso })
      @execution.resolve(op)&.value!
      nil
    end

    # Waits for a matching event. Returns the Inngest::Event, or nil if the
    # timeout passes first.
    #
    #   approval = step.wait_for_event("wait-for-approval",
    #     event: "invoice/approved",
    #     timeout: 3.days,
    #     if: "async.data.invoice_id == event.data.invoice_id")
    def wait_for_event(id, event:, timeout:, **opts)
      step_opts = { "event" => event.to_s, "timeout" => Duration.to_time_string(timeout), "if" => opts[:if] }.compact
      op = @execution.new_op(id, "WaitForEvent", name: event.to_s, opts: step_opts)
      Event.from_h(@execution.resolve(op)&.value!)
    end

    # Invokes another Inngest function and returns its result. `function` is an
    # Inngest::Function subclass, or a function ID string (with app_id for a
    # function in another app).
    #
    # Raises Inngest::StepError if the invoked function fails.
    def invoke(id, function:, data: {}, v: nil, timeout: nil, app_id: nil)
      function_id = function.is_a?(Class) ? function.id : function.to_s
      opts = {
        "function_id" => "#{app_id || @execution.app_id}-#{function_id}",
        "payload" => { "data" => data, "v" => v }.compact,
        "timeout" => timeout && Duration.to_time_string(timeout)
      }.compact
      op = @execution.new_op(id, "InvokeFunction", opts: opts)
      @execution.resolve(op)&.value!
    end

    # Runs steps in parallel and returns their results, in order.
    #
    #   welcome, profile = step.parallel(
    #     -> { step.run("send-welcome") { Mailer.welcome(id) } },
    #     -> { step.run("build-profile") { Profile.build(id) } }
    #   )
    #
    #   # Fan out over a collection:
    #   results = step.parallel(user_ids.map { |id| -> { step.run("sync-#{id}") { sync(id) } } })
    #
    # Each callable can contain several steps; they run in order within the
    # callable. Step IDs must be unique across callables.
    #
    # With mode: :wait (the default), the function continues once every
    # branch's current step has finished. With mode: :race, it continues as
    # soon as any of them finishes, which lets branches with several steps
    # progress independently (at the cost of more calls to your worker).
    def parallel(*callables, mode: :wait)
      callables = callables.flatten
      if callables.empty? || !callables.all? { |c| c.respond_to?(:call) }
        raise ArgumentError, "step.parallel needs one or more callables"
      end

      @execution.parallel(callables, mode: mode)
    end

    # Sends events as a retriable, memoized step. Returns the event IDs.
    def send_event(id, *events)
      run(id) { @execution.send_events(events) }
    end
  end
end
