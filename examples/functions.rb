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
