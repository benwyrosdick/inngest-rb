# frozen_string_literal: true

RSpec.describe Inngest::Middleware do
  # Records every hook call into a shared log.
  def recorder(name, log)
    Class.new(described_class) do
      %i[after_memoization before_execution after_execution before_response].each do |hook|
        define_method(hook) { log << [name, hook] }
      end
      define_method(:transform_input) { |_input| log << [name, :transform_input] }
      define_method(:transform_output) { |_output| log << [name, :transform_output] }
    end
  end

  def client_with(*middleware)
    test_client(middleware: middleware)
  end

  def run(function_class, client:, **payload_opts)
    executor = Inngest::Executor.new(client: client, function_class: function_class,
                                     payload: request_payload(**payload_opts))
    result = executor.call
    [result, JSON.parse(result.body)]
  end

  let(:log) { [] }
  let(:workflow) do
    Class.new(Inngest::Function) do
      id "workflow"
      trigger event: "test/event"
      def call
        a = step.run("a") { "A" }
        step.sleep("nap", 60)
        "done #{a}"
      end
    end
  end

  hooks = %i[transform_input after_memoization before_execution after_execution transform_output before_response]

  {
    "a step runs" => {},
    "a sleep is reported" => { steps: { Digest::SHA1.hexdigest("a") => { "data" => "A" } } },
    "the function finishes" => { steps: { Digest::SHA1.hexdigest("a") => { "data" => "A" },
                                          Digest::SHA1.hexdigest("nap") => nil } }
  }.each do |situation, payload|
    it "calls every hook once, in order, when #{situation}" do
      run(workflow, client: client_with(recorder(:mw, log)), **payload)
      expect(log).to eq(hooks.map { |hook| [:mw, hook] })
    end
  end

  it "calls every hook once when the function raises" do
    fn = Class.new(Inngest::Function) do
      id "raises"
      trigger event: "test/event"
      def call = raise("boom")
    end
    run(fn, client: client_with(recorder(:mw, log)))
    expect(log.map(&:last)).to eq(hooks)
  end

  it "runs client middleware before function middleware, each in order" do
    function_mw = recorder(:function, log)
    fn = Class.new(workflow) { middleware function_mw }
    run(fn, client: client_with(recorder(:client1, log), recorder(:client2, log)))

    expect(log.first(3)).to eq([%i[client1 transform_input], %i[client2 transform_input],
                                %i[function transform_input]])
  end

  it "calls before_execution once stored steps run out" do
    seen = []
    mw = Class.new(described_class) { define_method(:before_execution) { seen << :before_execution } }
    fn = Class.new(Inngest::Function) do
      id "tracks"
      trigger event: "test/event"
      define_method(:call) do
        step.run("a") { 1 }
        seen << :after_a
        step.run("b") { 2 }
      end
    end

    run(fn, client: client_with(mw), steps: { Digest::SHA1.hexdigest("a") => { "data" => 1 } })
    expect(seen).to eq(%i[before_execution after_a])
  end

  describe "transform_input" do
    it "adds context values, available as keyword arguments or through #context" do
      mw = Class.new(described_class) do
        def transform_input(input)
          input.context[:tenant] = "acme"
          input.context["region"] = input.event.data["region"]
        end
      end
      fn = Class.new(Inngest::Function) do
        id "context"
        trigger event: "test/event"
        def call(tenant:, **) = "#{tenant} in #{context["region"]}"
      end

      _, body = run(fn, client: client_with(mw), event: { "name" => "test/event", "data" => { "region" => "eu" } })
      expect(body).to eq("acme in eu")
    end

    it "can change stored step results and the event" do
      mw = Class.new(described_class) do
        def transform_input(input)
          input.steps.transform_values! { |memo| { "data" => memo["data"].upcase } }
          input.event.data["name"] = "changed"
        end
      end
      fn = Class.new(Inngest::Function) do
        id "decrypt"
        trigger event: "test/event"
        def call = [step.run("secret") { "never runs" }, event.data["name"]]
      end

      _, body = run(fn, client: client_with(mw), steps: { Digest::SHA1.hexdigest("secret") => { "data" => "abc" } })
      expect(body).to eq(%w[ABC changed])
    end

    it "receives the function and transport request" do
      seen = nil
      mw = Class.new(described_class) do
        define_method(:transform_input) { |_input| seen = [function, request] }
      end
      Inngest::Executor.new(client: client_with(mw), function_class: workflow, payload: request_payload,
                            request: :rack_request).call
      expect(seen).to eq([workflow, :rack_request])
    end
  end

  describe "transform_output" do
    it "can change a step's result before it's stored" do
      mw = Class.new(described_class) do
        def transform_output(output)
          output.data = output.data.reverse if output.step&.id == "a"
        end
      end
      _, body = run(workflow, client: client_with(mw))
      expect(body.first).to include("data" => "A".reverse, "op" => "StepRun")
    end

    it "can change the function's result" do
      mw = Class.new(described_class) do
        def transform_output(output)
          output.data = { "wrapped" => output.data } if output.finished?
        end
      end
      steps = { Digest::SHA1.hexdigest("a") => { "data" => "A" }, Digest::SHA1.hexdigest("nap") => nil }
      _, body = run(workflow, client: client_with(mw), steps: steps)
      expect(body).to eq({ "wrapped" => "done A" })
    end

    it "can replace or clear errors" do
      replace = Class.new(described_class) do
        def transform_output(output)
          output.error = Inngest::NonRetriableError.new("sanitized") if output.error
        end
      end
      fn = Class.new(Inngest::Function) do
        id "secret-error"
        trigger event: "test/event"
        def call = raise("password=hunter2")
      end
      result, body = run(fn, client: client_with(replace))
      expect([result.no_retry, body["message"]]).to eq([true, "sanitized"])

      clear = Class.new(described_class) do
        def transform_output(output)
          return unless output.error

          output.data = "recovered"
          output.error = nil
        end
      end
      result, body = run(fn, client: client_with(clear))
      expect([result.status, body]).to eq([200, "recovered"])
    end

    it "sees step errors" do
      seen = nil
      mw = Class.new(described_class) { define_method(:transform_output) { |output| seen = output.error } }
      fn = Class.new(Inngest::Function) do
        id "step-error"
        trigger event: "test/event"
        def call = step.run("a") { raise ArgumentError, "bad" }
      end

      _, body = run(fn, client: client_with(mw))
      expect(seen).to be_a(ArgumentError)
      expect(body.first["op"]).to eq("StepError")
    end
  end

  describe "event sending" do
    let(:sent) { [] }
    let(:stamp) do
      Class.new(described_class) do
        def before_send_events(events)
          events.each { |event| event["data"]["source"] = "middleware" }
        end
      end
    end

    before do
      allow(Inngest::HTTP).to receive(:request) do |_method, _url, **opts|
        sent.concat(JSON.parse(opts[:body]))
        Inngest::HTTP::Response.new(status: 200, body: JSON.generate({ "ids" => ["id-1"], "status" => 200 }),
                                    headers: {})
      end
    end

    it "lets client middleware change events and see their IDs" do
      results = []
      after = Class.new(described_class) { define_method(:after_send_events) { |result| results << result } }
      ids = client_with(stamp, after).send_event(name: "app/thing", data: { "x" => 1 })

      expect(ids).to eq(["id-1"])
      expect(sent.first["data"]).to eq({ "x" => 1, "source" => "middleware" })
      expect(results.map(&:to_h)).to eq([{ ids: ["id-1"], error: nil }])
    end

    it "reports send errors to after_send_events" do
      allow(Inngest::HTTP).to receive(:request)
        .and_return(Inngest::HTTP::Response.new(status: 400, body: '{"error":"bad event"}', headers: {}))
      results = []
      after = Class.new(described_class) { define_method(:after_send_events) { |result| results << result } }

      expect { client_with(after).send_event(name: "app/thing") }.to raise_error(Inngest::SendEventError)
      expect(results.first.error).to be_a(Inngest::SendEventError)
    end

    it "runs the function's middleware for step.send_event" do
      fn = Class.new(Inngest::Function) do
        id "sender"
        trigger event: "test/event"
        def call = step.send_event("notify", name: "app/notified", data: {})
      end
      fn.middleware(stamp)

      _, body = run(fn, client: test_client(event_key: "key"))
      expect(body.first["data"]).to eq(["id-1"])
      expect(sent.first["data"]).to eq({ "source" => "middleware" })
    end
  end
end
