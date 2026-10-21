# frozen_string_literal: true

RSpec.describe Inngest::Executor do
  let(:workflow) do
    Class.new(Inngest::Function) do
      id "workflow"
      trigger event: "test/event"

      def call
        user = step.run("load-user") { { name: "Ada", "tags" => [:admin] } }
        step.sleep("nap", 30)
        greeting = step.run("greet") { "hello #{user["name"]}" }
        { greeting: greeting }
      end
    end
  end

  describe "running steps" do
    it "runs the first new step and reports its JSON-round-tripped result" do
      result, body = execute(workflow)

      expect(result.status).to eq(206)
      expect(body).to eq([{
                           "id" => hashed("load-user"),
                           "op" => "StepRun",
                           "name" => "load-user",
                           "displayName" => "load-user",
                           "userland" => { "id" => "load-user" },
                           "data" => { "name" => "Ada", "tags" => ["admin"] }
                         }])
    end

    it "replays memoized steps and reports the next async step" do
      _, body = execute(workflow, steps: { hashed("load-user") => { "data" => { "name" => "Ada" } } })

      expect(body).to match([hash_including("id" => hashed("nap"), "op" => "Sleep", "name" => "30s",
                                            "opts" => { "duration" => "30s" })])
    end

    it "returns the function's output once every step is memoized" do
      steps = {
        hashed("load-user") => { "data" => { "name" => "Ada" } },
        hashed("nap") => nil,
        hashed("greet") => { "data" => "hello Ada" }
      }
      result, body = execute(workflow, steps: steps)

      expect(result.status).to eq(200)
      expect(body).to eq({ "greeting" => "hello Ada" })
    end

    it "gives steps returned in the first run the same shape as replayed ones" do
      seen = []
      fn = Class.new(Inngest::Function) do
        id "shape"
        trigger event: "test/event"
        define_method(:call) do
          seen << step.run("a") { { key: :value } }
          step.run("b") { 1 }
        end
      end

      execute(fn, steps: { hashed("a") => { "data" => { "key" => "value" } } })
      expect(seen).to eq([{ "key" => "value" }])
    end

    it "suffixes repeated step IDs with a counter" do
      fn = Class.new(Inngest::Function) do
        id "loop"
        trigger event: "test/event"
        def call
          3.times { |i| step.run("item") { i } }
        end
      end

      _, body = execute(fn, steps: { hashed("item") => { "data" => 0 } })
      expect(body.first).to include("id" => hashed("item:1"), "data" => 1,
                                    "userland" => { "id" => "item", "index" => 1 })
    end

    it "plans steps instead of running them when immediate execution is disabled" do
      _, body = execute(workflow, disable_immediate_execution: true)

      expect(body).to match([hash_including("id" => hashed("load-user"), "op" => "StepPlanned")])
      expect(body.first).not_to have_key("data")
    end

    it "runs only the targeted step" do
      steps = { hashed("load-user") => { "data" => { "name" => "Ada" } }, hashed("nap") => nil }
      _, body = execute(workflow, steps: steps, step_id: hashed("greet"))

      expect(body).to match([hash_including("id" => hashed("greet"), "op" => "StepRun", "data" => "hello Ada")])
    end

    it "reports StepNotFound when the targeted step isn't reached" do
      _, body = execute(workflow, step_id: hashed("missing"))

      expect(body).to eq([{ "id" => hashed("missing"), "op" => "StepNotFound" }])
    end

    it "treats a step ID of \"step\" as no target" do
      result, = execute(workflow, step_id: "step")
      expect(result.status).to eq(206)
    end

    it "accepts context as keyword arguments" do
      fn = Class.new(Inngest::Function) do
        id "kwargs"
        trigger event: "test/event"
        def call(event:, run_id:, **)
          { "name" => event.name, "data" => event.data, "run_id" => run_id }
        end
      end

      _, body = execute(fn, event: { "name" => "test/event", "data" => { "x" => 1 } })
      expect(body).to eq({ "name" => "test/event", "data" => { "x" => 1 }, "run_id" => "run-123" })
    end
  end

  describe "interrupts" do
    it "can't be swallowed by rescue in user code" do
      fn = Class.new(Inngest::Function) do
        id "rescuer"
        trigger event: "test/event"
        def call
          step.run("a") { 1 }
          "finished"
        rescue Exception # rubocop:disable Lint/RescueException
          "swallowed"
        end
      end

      result, body = execute(fn)
      expect(result.status).to eq(206)
      expect(body.first["op"]).to eq("StepRun")
    end

    it "runs ensure blocks in user code" do
      ensured = []
      fn = Class.new(Inngest::Function) do
        id "ensurer"
        trigger event: "test/event"
        define_method(:call) do
          step.run("a") { 1 }
        ensure
          ensured << true
        end
      end

      execute(fn)
      expect(ensured).to eq([true])
    end
  end

  describe "errors" do
    let(:flaky) do
      Class.new(Inngest::Function) do
        id "flaky"
        trigger event: "test/event"
        def call
          step.run("explode") { raise ArgumentError, "boom" }
        end
      end
    end

    it "reports a failing step as StepError so Inngest retries it" do
      result, body = execute(flaky)

      expect(result.status).to eq(206)
      expect(result.no_retry).to be(false)
      expect(body.first).to include("op" => "StepError", "error" => hash_including("name" => "ArgumentError",
                                                                                   "message" => "boom"))
    end

    it "reports StepFailed on the last attempt" do
      _, body = execute(flaky, attempt: 3, max_attempts: 4)
      expect(body.first["op"]).to eq("StepFailed")
    end

    it "raises a memoized step error as Inngest::StepError, which fails the function without retries" do
      steps = { hashed("explode") => { "error" => { "name" => "ArgumentError", "message" => "boom" } } }
      result, body = execute(flaky, steps: steps)

      expect(result.status).to eq(500)
      expect(result.no_retry).to be(true)
      expect(body).to include("name" => "ArgumentError", "message" => "boom")
    end

    it "lets functions rescue failed steps" do
      fn = Class.new(Inngest::Function) do
        id "recovers"
        trigger event: "test/event"
        def call
          step.run("explode") { raise "boom" }
        rescue Inngest::StepError => e
          "recovered from #{e.error_name}: #{e.message}"
        end
      end

      steps = { hashed("explode") => { "error" => { "name" => "RuntimeError", "message" => "boom" } } }
      result, body = execute(fn, steps: steps)
      expect(result.status).to eq(200)
      expect(body).to eq("recovered from RuntimeError: boom")
    end

    it "fails without retries on NonRetriableError, even inside a step" do
      fn = Class.new(Inngest::Function) do
        id "fatal"
        trigger event: "test/event"
        def call
          step.run("a") { raise Inngest::NonRetriableError, "give up" }
        end
      end

      result, body = execute(fn)
      expect(result.status).to eq(500)
      expect(result.no_retry).to be(true)
      expect(body["message"]).to eq("give up")
    end

    it "sets retry_after for RetryAfterError" do
      at = Time.utc(2030, 1, 1, 12)
      fn = Class.new(Inngest::Function) do
        id "later"
        trigger event: "test/event"
        define_method(:call) { raise Inngest::RetryAfterError.new("rate limited", retry_after: at) }
      end

      result, = execute(fn)
      expect(result.status).to eq(500)
      expect(result.no_retry).to be(false)
      expect(result.retry_after).to eq("2030-01-01T12:00:00.000Z")
    end

    it "retries other function-level errors" do
      fn = Class.new(Inngest::Function) do
        id "raises"
        trigger event: "test/event"
        def call = raise("oops")
      end

      result, body = execute(fn)
      expect([result.status, result.no_retry]).to eq([500, false])
      expect(body).to include("name" => "RuntimeError", "message" => "oops")
    end

    it "rejects steps nested inside step.run" do
      fn = Class.new(Inngest::Function) do
        id "nested"
        trigger event: "test/event"
        def call
          step.run("outer") { step.run("inner") { 1 } }
        end
      end

      result, body = execute(fn)
      expect([result.status, result.no_retry]).to eq([500, true])
      expect(body["name"]).to eq("Inngest::NestedStepError")
    end
  end

  describe "other step types" do
    it "reports wait_for_event and returns the matched event" do
      fn = Class.new(Inngest::Function) do
        id "waiter"
        trigger event: "test/event"
        def call
          approval = step.wait_for_event("approval", event: "test/approved", timeout: 3600, if: "async.data.ok")
          approval&.data
        end
      end

      _, body = execute(fn)
      expect(body.first).to include("op" => "WaitForEvent", "name" => "test/approved",
                                    "opts" => { "event" => "test/approved", "timeout" => "1h",
                                                "if" => "async.data.ok" })

      matched = { "name" => "test/approved", "data" => { "ok" => true }, "id" => "evt", "ts" => 1 }
      _, body = execute(fn, steps: { hashed("approval") => matched })
      expect(body).to eq({ "ok" => true })

      result, body = execute(fn, steps: { hashed("approval") => nil })
      expect([result.status, body]).to eq([200, nil])
    end

    it "reports invoke with the target's fully qualified ID" do
      target = Class.new(Inngest::Function) do
        id "target"
        trigger event: "test/target"
      end
      fn = Class.new(Inngest::Function) do
        id "invoker"
        trigger event: "test/event"
        define_method(:call) { step.invoke("call-target", function: target, data: { "x" => 1 }) }
      end

      _, body = execute(fn)
      expect(body.first).to include("op" => "InvokeFunction",
                                    "opts" => { "function_id" => "test-app-target",
                                                "payload" => { "data" => { "x" => 1 } } })

      _, body = execute(fn, steps: { hashed("call-target") => { "data" => 42 } })
      expect(body).to eq(42)
    end

    it "reports sleep_until with an ISO timestamp" do
      fn = Class.new(Inngest::Function) do
        id "until"
        trigger event: "test/event"
        def call = step.sleep_until("wake", Time.utc(2030, 5, 1))
      end

      _, body = execute(fn)
      expect(body.first).to include("op" => "Sleep", "opts" => { "duration" => "2030-05-01T00:00:00.000Z" })
    end
  end

  describe "failure handlers" do
    it "runs #on_failure with the failure event" do
      fn = Class.new(Inngest::Function) do
        id "with-failure"
        trigger event: "test/event"
        def call = nil
        def on_failure = "cleaned up after #{error["message"]}"
      end

      failure_event = { "name" => "inngest/function.failed", "data" => { "error" => { "message" => "boom" } } }
      result, body = execute(fn, failure: true, event: failure_event)
      expect(result.status).to eq(200)
      expect(body).to eq("cleaned up after boom")
    end
  end
end
