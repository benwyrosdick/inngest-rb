# frozen_string_literal: true

require "net/http"

RSpec.describe Inngest::Connect::HealthServer do
  let(:worker) do
    Struct.new(:ready, :alive) do
      def ready? = ready
      def alive? = alive
      def health = { "status" => ready ? "active" : "reconnecting" }
    end.new(true, true)
  end
  let(:server) { described_class.new(worker, port: 0, host: "127.0.0.1").start }

  after { server.stop }

  def request(klass, path)
    Net::HTTP.start("127.0.0.1", server.port, read_timeout: 5) { |http| http.request(klass.new(path)) }
  end

  it "reports readiness" do
    response = request(Net::HTTP::Get, "/ready")
    expect(response.code).to eq("200")
    expect(JSON.parse(response.body)).to eq({ "status" => "active" })

    worker.ready = false
    response = request(Net::HTTP::Get, "/ready")
    expect(response.code).to eq("503")
    expect(JSON.parse(response.body)).to eq({ "status" => "reconnecting" })
  end

  it "reports liveness separately from readiness" do
    worker.ready = false
    expect(request(Net::HTTP::Get, "/live").code).to eq("200")

    worker.alive = false
    expect(request(Net::HTTP::Get, "/live").code).to eq("503")
  end

  it "accepts common probe paths, query strings and HEAD" do
    expect(request(Net::HTTP::Get, "/readyz?verbose=1").code).to eq("200")
    expect(request(Net::HTTP::Get, "/healthz").code).to eq("200")

    head = request(Net::HTTP::Head, "/ready")
    expect([head.code, head.body]).to eq(["200", nil])
  end

  it "rejects other paths and methods" do
    expect(request(Net::HTTP::Get, "/metrics").code).to eq("404")
    expect(request(Net::HTTP::Post, "/ready").code).to eq("405")
  end

  it "keeps answering after a client that never sends a request" do
    silent = TCPSocket.new("127.0.0.1", server.port)
    started = Time.now
    expect(request(Net::HTTP::Get, "/ready").code).to eq("200")
    expect(Time.now - started).to be < described_class::READ_TIMEOUT + 1
  ensure
    silent&.close
  end

  it "fails clearly when the port is taken" do
    taken = TCPServer.new("127.0.0.1", 0)
    other = described_class.new(worker, port: taken.addr[1], host: "127.0.0.1")
    expect { other.start }.to raise_error(Inngest::ConfigError, /can't listen for health checks/)
  ensure
    taken&.close
  end
end

RSpec.describe Inngest::Connect::Worker do
  let(:function) do
    Class.new(Inngest::Function) do
      id "noop"
      trigger event: "test/noop"
    end
  end
  let(:worker) { described_class.new(client: test_client, functions: [function]) }
  let(:connection) { instance_double(Inngest::Connect::Connection, ready?: true, connection_id: "conn-1") }

  def set(**ivars)
    ivars.each { |name, value| worker.instance_variable_set(:"@#{name}", value) }
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  it "is ready only while its connection is open" do
    set(state: :active, active: connection)
    expect([worker.state, worker.ready?]).to eq([:active, true])

    allow(connection).to receive(:ready?).and_return(false)
    expect([worker.state, worker.ready?]).to eq([:reconnecting, false])
  end

  it "isn't alive before it runs, after it stops, or when its ticker stalls or dies" do
    expect(worker.alive?).to be(false)

    ticker = Thread.new { sleep }
    set(state: :active, active: connection, ticker: ticker, last_tick_at: monotonic_now)
    expect(worker.alive?).to be(true)

    set(last_tick_at: monotonic_now - described_class::LIVENESS_TIMEOUT - 1)
    expect(worker.alive?).to be(false)

    set(last_tick_at: monotonic_now)
    ticker.kill.join
    expect(worker.alive?).to be(false)

    set(state: :closed)
    expect(worker.alive?).to be(false)
  end

  it "includes connection details in its health report" do
    set(state: :active, active: connection)
    expect(worker.health).to include("status" => "active", "ready" => true, "connection_id" => "conn-1",
                                     "in_flight" => 0, "app_id" => "test-app")
  end
end
