# frozen_string_literal: true

# Example functions used by the README and the integration specs.

require "inngest"

class Greet < Inngest::Function
  trigger event: "demo/greet"

  def call
    name = step.run("load-name") { event.data.fetch("name", "world") }
    step.sleep("pause", 1)
    greeting = step.run("build-greeting") { "Hello, #{name}!" }
    { "greeting" => greeting }
  end
end

class WaitForApproval < Inngest::Function
  trigger event: "demo/approval.requested"

  def call(step:, **)
    approval = step.wait_for_event("wait-for-approval",
                                   event: "demo/approval.granted",
                                   timeout: 60,
                                   if: "async.data.request_id == event.data.request_id")
    approval ? "approved by #{approval.data["approver"]}" : "timed out"
  end
end

class Double < Inngest::Function
  trigger event: "demo/double"

  def call
    event.data["value"] * 2
  end
end

class InvokeAndSend < Inngest::Function
  trigger event: "demo/invoke"

  def call
    doubled = step.invoke("double", function: Double, data: { "value" => 21 })
    ids = step.send_event("notify", name: "demo/notified", data: { "value" => doubled })
    { "doubled" => doubled, "sent" => ids.size }
  end
end

# Fails its first attempt, then succeeds on the retry.
class FlakyStep < Inngest::Function
  trigger event: "demo/flaky"
  retries 2

  def call
    step.run("flaky") do
      raise "transient failure" if attempt.zero?

      "succeeded on attempt #{attempt}"
    end
  end
end

class AlwaysFails < Inngest::Function
  trigger event: "demo/fail"
  retries 0

  def call
    raise Inngest::NonRetriableError, "this can't work"
  end

  def on_failure
    logger.info("[example] cleaning up after: #{error["message"]}")
    "cleaned up"
  end
end

# Holds a step open for a while, to exercise lease extension and graceful
# shutdown.
class SlowStep < Inngest::Function
  trigger event: "demo/slow"

  def call
    step.run("slow") do
      Kernel.sleep(event.data.fetch("seconds", 5))
      "slept #{event.data.fetch("seconds", 5)}s"
    end
  end
end

# Runs three slow steps at once, then combines their results.
class ParallelFanOut < Inngest::Function
  trigger event: "demo/parallel"

  def call
    started = step.run("start") { Time.now.to_f }
    squares = step.parallel([1, 2, 3].map do |n|
      lambda do
        step.run("square-#{n}") do
          Kernel.sleep(2)
          n * n
        end
      end
    end)
    elapsed = step.run("finish") { (Time.now.to_f - started).round(1) }
    { "squares" => squares, "elapsed" => elapsed }
  end
end

# Branches with several steps, including a sleep, joined in race mode.
class ParallelBranches < Inngest::Function
  trigger event: "demo/parallel.branches"

  def call
    step.parallel(
      lambda {
        first = step.run("a1") { "a1" }
        step.sleep("a-pause", 1)
        [first, step.run("a2") { "a2" }]
      },
      -> { step.run("b") { "b" } },
      mode: :race
    )
  end
end

# Adds the transport's request class to every function's context.
class RequestInfoMiddleware < Inngest::Middleware
  def transform_input(input)
    input.context[:transport] = request.class.name
  end
end

# Marks a function's result.
class MarkResultMiddleware < Inngest::Middleware
  def transform_output(output)
    output.data = output.data.merge("middleware" => true) if output.finished?
  end
end

class ReportTransport < Inngest::Function
  trigger event: "demo/transport"
  middleware MarkResultMiddleware

  def call
    step.run("noop") { 1 }
    { "transport" => context[:transport] }
  end
end
