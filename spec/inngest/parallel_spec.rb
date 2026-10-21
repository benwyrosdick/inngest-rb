# frozen_string_literal: true

RSpec.describe "step.parallel" do
  let(:fan_out) do
    Class.new(Inngest::Function) do
      id "fan-out"
      trigger event: "test/event"

      def call
        a, b = step.parallel(
          -> { step.run("a") { "A" } },
          -> { step.run("b") { "B" } }
        )
        step.run("join") { "#{a}+#{b}" }
      end
    end
  end

  it "plans every new step in the group at once" do
    result, body = execute(fan_out)

    expect(result.status).to eq(206)
    expect(body.map { |op| [op["id"], op["op"]] }).to eq([[hashed("a"), "StepPlanned"], [hashed("b"), "StepPlanned"]])
    expect(body).to all(satisfy { |op| !op.key?("data") })
  end

  it "runs only the targeted branch's step" do
    _, body = execute(fan_out, step_id: hashed("b"), disable_immediate_execution: true)
    expect(body).to eq([{ "id" => hashed("b"), "op" => "StepRun", "name" => "b", "displayName" => "b",
                          "userland" => { "id" => "b" }, "data" => "B" }])
  end

  it "reports only the steps that haven't finished" do
    _, body = execute(fan_out, steps: { hashed("a") => { "data" => "A" } }, disable_immediate_execution: true)
    expect(body.map { |op| op["id"] }).to eq([hashed("b")])
  end

  it "returns results in order and plans the next step once the group is done" do
    steps = { hashed("b") => { "data" => "B" }, hashed("a") => { "data" => "A" } }
    _, body = execute(fan_out, steps: steps, disable_immediate_execution: true)
    expect(body).to match([hash_including("id" => hashed("join"), "op" => "StepPlanned")])

    steps[hashed("join")] = { "data" => "A+B" }
    result, body = execute(fan_out, steps: steps, disable_immediate_execution: true)
    expect([result.status, body]).to eq([200, "A+B"])
  end

  it "reports other step types alongside planned runs" do
    fn = Class.new(Inngest::Function) do
      id "mixed"
      trigger event: "test/event"
      def call
        step.parallel(
          -> { step.sleep("nap", 60) },
          -> { step.wait_for_event("signal", event: "test/signal", timeout: 3600) },
          -> { step.run("work") { 1 } }
        )
      end
    end

    _, body = execute(fn)
    expect(body.map { |op| op["op"] }).to eq(%w[Sleep WaitForEvent StepPlanned])
  end

  it "runs the steps within each branch in order" do
    fn = Class.new(Inngest::Function) do
      id "branches"
      trigger event: "test/event"
      def call
        step.parallel(
          -> { [step.run("a1") { 1 }, step.run("a2") { 2 }] },
          -> { step.run("b") { 3 } }
        )
      end
    end

    _, body = execute(fn)
    expect(body.map { |op| op["id"] }).to eq([hashed("a1"), hashed("b")])

    _, body = execute(fn, steps: { hashed("a1") => { "data" => 1 }, hashed("b") => { "data" => 3 } },
                          disable_immediate_execution: true)
    expect(body.map { |op| op["id"] }).to eq([hashed("a2")])
  end

  it "marks steps for race mode" do
    fn = Class.new(Inngest::Function) do
      id "race"
      trigger event: "test/event"
      def call
        step.parallel(-> { step.run("a") { 1 } }, -> { step.sleep("b", 5) }, mode: :race)
      end
    end

    _, body = execute(fn)
    expect(body.map { |op| op["opts"] }).to eq([{ "parallelMode" => "race" },
                                                { "duration" => "5s", "parallelMode" => "race" }])
  end

  it "fans out over a collection" do
    fn = Class.new(Inngest::Function) do
      id "map"
      trigger event: "test/event"
      def call
        step.parallel([1, 2, 3].map { |n| -> { step.run("square-#{n}") { n * n } } })
      end
    end

    steps = (1..3).to_h { |n| [hashed("square-#{n}"), { "data" => n * n }] }
    result, body = execute(fn, steps: steps, disable_immediate_execution: true)
    expect([result.status, body]).to eq([200, [1, 4, 9]])
  end

  it "reports StepNotFound when no branch has the targeted step" do
    _, body = execute(fan_out, step_id: hashed("nope"), disable_immediate_execution: true)
    expect(body).to eq([{ "id" => hashed("nope"), "op" => "StepNotFound" }])
  end

  it "reports a failing targeted step as StepError" do
    fn = Class.new(Inngest::Function) do
      id "parallel-error"
      trigger event: "test/event"
      def call
        step.parallel(-> { step.run("ok") { 1 } }, -> { step.run("bad") { raise "nope" } })
      end
    end

    _, body = execute(fn, step_id: hashed("bad"), disable_immediate_execution: true)
    expect(body).to match([hash_including("id" => hashed("bad"), "op" => "StepError",
                                          "error" => hash_including("message" => "nope"))])
  end

  it "raises failed branches as StepError" do
    fn = Class.new(Inngest::Function) do
      id "parallel-rescue"
      trigger event: "test/event"
      def call
        step.parallel(-> { step.run("ok") { 1 } }, -> { step.run("bad") { 2 } })
      rescue Inngest::StepError => e
        "rescued #{e.message}"
      end
    end

    steps = { hashed("ok") => { "data" => 1 }, hashed("bad") => { "error" => { "name" => "E", "message" => "x" } } }
    _, body = execute(fn, steps: steps, disable_immediate_execution: true)
    expect(body).to eq("rescued x")
  end

  it "can't be used inside step.run" do
    fn = Class.new(Inngest::Function) do
      id "parallel-nested"
      trigger event: "test/event"
      def call
        step.run("outer") { step.parallel(-> { 1 }) }
      end
    end

    result, body = execute(fn)
    expect([result.status, body["name"]]).to eq([500, "Inngest::NestedStepError"])
  end
end
