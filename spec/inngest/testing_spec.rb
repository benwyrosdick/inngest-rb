# frozen_string_literal: true

require "inngest/rspec"

module TestingSpec
  class Welcome < Inngest::Function
    trigger event: "user/signed_up"

    def call
      user = step.run("load-user") { raise "talks to the database" }
      step.sleep("wait-a-day", 86_400)
      step.run("send-email") { "sent to #{user["email"]}" }
    end
  end

  class Approval < Inngest::Function
    trigger event: "invoice/created"

    def call
      approval = step.wait_for_event("approval", event: "invoice/approved", timeout: 3600)
      approval ? "approved by #{approval.data["by"]}" : "timed out"
    end
  end

  class Double < Inngest::Function
    trigger event: "math/double"

    def call = event.data["n"] * 2
  end

  class Caller < Inngest::Function
    trigger event: "math/call"

    def call = step.invoke("double", function: Double, data: { "n" => 21 })
  end

  class Flaky < Inngest::Function
    trigger event: "test/flaky"
    retries 2

    def call
      step.run("flaky") do
        raise "attempt #{attempt} failed" if attempt < 2

        "worked on attempt #{attempt}"
      end
    end
  end

  class Broken < Inngest::Function
    trigger event: "test/broken"
    retries 1

    def call = step.run("broken") { raise ArgumentError, "always" }

    def on_failure = "cleaned up after #{error["message"]}"
  end

  class Notify < Inngest::Function
    trigger event: "order/paid"

    def call
      step.send_event("notify", name: "order/notified", data: { "order_id" => event.data["order_id"] })
      step.parallel([1, 2].map { |n| -> { step.run("part-#{n}") { n } } })
    end
  end
end

RSpec.describe Inngest::Testing do
  it "runs steps and skips sleeps, with stubs replacing steps" do
    result = run_inngest_function(TestingSpec::Welcome, steps: { "load-user" => { "email" => "ada@example.com" } })

    expect(result).to be_completed
    expect(result.output).to eq("sent to ada@example.com")
    expect(result.step_ids).to eq(%w[load-user wait-a-day send-email])
    expect(result.steps.first).to be_stubbed
    expect(result.step("send-email")).to eq("sent to ada@example.com")
  end

  it "uses the function's first event trigger by default, and accepts events" do
    result = run_inngest_function(TestingSpec::Double, event: { name: "math/double", data: { n: 4 } })
    expect(result.output).to eq(8)
  end

  it "requires stubs for wait_for_event" do
    expect { run_inngest_function(TestingSpec::Approval) }
      .to raise_error(Inngest::Testing::UnstubbedStepError, /wait_for_event\("approval"\) needs a stub/)

    approved = run_inngest_function(TestingSpec::Approval,
                                    steps: { approval: { name: "invoice/approved", data: { by: "ops" } } })
    expect(approved.output).to eq("approved by ops")

    expect(run_inngest_function(TestingSpec::Approval, steps: { "approval" => nil }).output).to eq("timed out")
  end

  it "runs invoked functions locally, unless stubbed" do
    expect(run_inngest_function(TestingSpec::Caller).output).to eq(42)
    expect(run_inngest_function(TestingSpec::Caller, steps: { "double" => 7 }).output).to eq(7)
  end

  it "retries failing steps like Inngest" do
    result = run_inngest_function(TestingSpec::Flaky)

    expect(result.output).to eq("worked on attempt 2")
    expect(result.steps.map(&:error).compact.map { |e| e["message"] }).to eq(["attempt 0 failed", "attempt 1 failed"])
  end

  it "fails once retries run out" do
    result = run_inngest_function(TestingSpec::Broken)

    expect(result).to be_failed
    expect(result.error).to be_a(Inngest::StepError)
    expect([result.error.error_name, result.error.message]).to eq(%w[ArgumentError always])
    expect(result.steps.count(&:error)).to eq(2)

    expect(run_inngest_function(TestingSpec::Broken, max_attempts: 1).steps.count(&:error)).to eq(1)
  end

  it "makes stubbed exceptions fail the step" do
    result = run_inngest_function(TestingSpec::Welcome, steps: { "load-user" => RuntimeError.new("db down") })
    expect(result.error).to have_attributes(message: "db down", error_name: "RuntimeError")
  end

  it "calls Proc stubs" do
    calls = 0
    run_inngest_function(TestingSpec::Welcome, steps: { "load-user" => -> { calls += 1 and { "email" => "x" } } })
    expect(calls).to eq(1)
  end

  it "records sent events and runs parallel steps" do
    result = run_inngest_function(TestingSpec::Notify, event: { name: "order/paid", data: { order_id: 7 } })

    expect(result.output).to eq([1, 2])
    expect(result.events.map { |e| e.slice("name", "data") })
      .to eq([{ "name" => "order/notified", "data" => { "order_id" => 7 } }])
  end

  it "runs failure handlers" do
    result = run_inngest_function(TestingSpec::Broken, failure: true, error: ArgumentError.new("always"))
    expect(result.output).to eq("cleaned up after always")
  end

  it "stubs repeated step IDs by index" do
    fn = Class.new(Inngest::Function) do
      id "loop"
      trigger event: "test/loop"
      def call = 3.times.map { |i| step.run("item") { i } }
    end

    expect(run_inngest_function(fn, steps: { "item:1" => "stubbed" }).output).to eq([0, "stubbed", 2])
  end

  it "runs the client's middleware" do
    mw = Class.new(Inngest::Middleware) do
      def transform_input(input) = input.context[:greeting] = "hi"
    end
    fn = Class.new(Inngest::Function) do
      id "greets"
      trigger event: "test/greet"
      def call(greeting:) = greeting
    end

    client = Inngest::Testing::Client.new(app_id: "test-app", middleware: [mw], logger: Logger.new(IO::NULL))
    expect(run_inngest_function(fn, client: client).output).to eq("hi")
  end
end
