# frozen_string_literal: true

module Billing
  class SendInvoiceReminder < Inngest::Function
    trigger event: "invoice/created"
  end
end

RSpec.describe Inngest::Function do
  it "derives the ID from the class name" do
    expect(Billing::SendInvoiceReminder.id).to eq("billing-send-invoice-reminder")
  end

  it "builds the sync configuration" do
    fn = Class.new(described_class) do
      id "process-order"
      display_name "Process order"
      trigger event: "order/created", if: "event.data.total > 100"
      trigger cron: "0 9 * * *"
      retries 5
      concurrency limit: 10, key: "event.data.account_id"
      throttle limit: 5, period: 60
      rate_limit limit: 1, period: 3600, key: "event.data.user_id"
      debounce period: 30
      batch_events max_size: 100, timeout: 5
      cancel_on event: "order/cancelled", if: "event.data.id == async.data.id"
      priority run: "event.data.priority"
      idempotency "event.data.id"
      timeouts finish: 600
      singleton mode: :skip, key: "event.data.user_id"
    end

    config = fn.to_configs(app_id: "shop", url: "wss://connect").first
    expect(config).to eq(
      "id" => "shop-process-order",
      "name" => "Process order",
      "triggers" => [{ "event" => "order/created", "expression" => "event.data.total > 100" },
                     { "cron" => "0 9 * * *" }],
      "steps" => { "step" => { "id" => "step", "name" => "step",
                               "runtime" => { "type" => "ws",
                                              "url" => "wss://connect?fnId=shop-process-order&stepId=step" },
                               "retries" => { "attempts" => 5 } } },
      "concurrency" => [{ "limit" => 10, "key" => "event.data.account_id" }],
      "throttle" => { "limit" => 5, "period" => "1m" },
      "rateLimit" => { "limit" => 1, "period" => "1h", "key" => "event.data.user_id" },
      "debounce" => { "period" => "30s" },
      "batchEvents" => { "maxSize" => 100, "timeout" => "5s" },
      "cancel" => [{ "event" => "order/cancelled", "if" => "event.data.id == async.data.id" }],
      "priority" => { "run" => "event.data.priority" },
      "idempotency" => "event.data.id",
      "timeouts" => { "finish" => "10m" },
      "singleton" => { "mode" => "skip", "key" => "event.data.user_id" }
    )
  end

  it "adds a failure handler function when #on_failure is defined" do
    fn = Class.new(described_class) do
      id "charge"
      trigger event: "order/created"
      def on_failure = nil
    end

    failure = fn.to_configs(app_id: "shop", url: "wss://connect").last
    expect(failure).to include(
      "id" => "shop-charge-failure",
      "name" => "charge (failure)",
      "triggers" => [{ "event" => "inngest/function.failed",
                       "expression" => "event.data.function_id == 'shop-charge'" }]
    )
    expect(failure.dig("steps", "step", "retries")).to eq({ "attempts" => 0 })
  end

  it "inherits configuration from abstract base classes" do
    base = Class.new(described_class) { retries 1 }
    child = Class.new(base) do
      id "child"
      trigger event: "a/b"
    end

    expect(child.to_configs(app_id: "x", url: "wss://connect").first.dig("steps", "step", "retries"))
      .to eq({ "attempts" => 1 })
    expect(base.options).to eq({ "retries" => 1 })
    expect(described_class.registered).to include(child)
    expect(described_class.registered).not_to include(base)
  end

  it "requires a trigger" do
    fn = Class.new(described_class) { id "lonely" }
    expect { fn.to_configs(app_id: "x", url: "wss://connect") }.to raise_error(Inngest::ConfigError)
  end
end
