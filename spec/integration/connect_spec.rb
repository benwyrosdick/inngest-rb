# frozen_string_literal: true

# End-to-end tests against a running Inngest dev server:
#
#   npx inngest-cli@latest dev --no-discovery
#   bundle exec rake spec:integration
#
# Use a fresh dev server: runs for other apps that react to the same events
# can confuse the assertions.

require "net/http"
require_relative "../../examples/functions"

DEV_SERVER = ENV.fetch("INNGEST_DEV_SERVER_URL", "http://127.0.0.1:8288")

def dev_server_running?
  Net::HTTP.get_response(URI(DEV_SERVER)).is_a?(Net::HTTPSuccess)
rescue StandardError
  false
end

RSpec.describe "Connect worker with the dev server", if: ENV.fetch("INNGEST_INTEGRATION", nil) && dev_server_running? do
  before(:all) do
    @client = Inngest::Client.new(app_id: "ruby-integration", dev: DEV_SERVER,
                                  logger: Logger.new($stdout, level: :warn))
    @worker = Inngest::Connect::Worker.new(client: @client, functions: Inngest::Function.registered,
                                           shutdown_signals: [])
    @thread = Thread.new { @worker.run }
    deadline = Time.now + 15
    sleep 0.1 until @worker.ready? || Time.now > deadline
    raise "worker didn't connect" unless @worker.ready?
  end

  after(:all) do
    @worker&.stop
    @thread&.join(30)
  end

  def send_and_wait(name, data = {}, status: "Completed", timeout: 30)
    event_id = @client.send_event(name: name, data: data).first
    wait_for_run(event_id, status: status, timeout: timeout)
  end

  # Other apps on the dev server may also react to the event, so look for the
  # run that finished rather than the first one.
  def wait_for_run(event_id, status:, timeout: 30)
    deadline = Time.now + timeout
    loop do
      # The dev server caches this endpoint for 15s; a unique query bypasses it.
      url = URI("#{DEV_SERVER}/v1/events/#{event_id}/runs?poll=#{Process.clock_gettime(Process::CLOCK_MONOTONIC)}")
      runs = JSON.parse(Net::HTTP.get(url))["data"] || []
      # A run can show as finished a moment before its output is stored.
      finished = runs.find { |run| %w[Completed Failed Cancelled].include?(run["status"]) && run.key?("output") }
      if finished
        raise "run ended as #{finished["status"]}: #{finished["output"].inspect}" if finished["status"] != status

        return finished
      end
      raise "timed out waiting for event #{event_id} (runs: #{runs.inspect})" if Time.now > deadline

      sleep 0.25
    end
  end

  it "runs steps and sleeps" do
    run = send_and_wait("demo/greet", { "name" => "Ruby" })
    expect(run["output"]).to eq({ "greeting" => "Hello, Ruby!" })
  end

  it "waits for a matching event" do
    request_id = SecureRandom.uuid
    event_id = @client.send_event(name: "demo/approval.requested", data: { "request_id" => request_id }).first
    sleep 2 # Let the run reach wait_for_event.
    @client.send_event(name: "demo/approval.granted", data: { "request_id" => request_id, "approver" => "ops" })

    run = wait_for_run(event_id, status: "Completed")
    expect(run["output"]).to eq("approved by ops")
  end

  it "invokes other functions and sends events from steps" do
    run = send_and_wait("demo/invoke")
    expect(run["output"]).to eq({ "doubled" => 42, "sent" => 1 })
  end

  it "retries failed steps" do
    run = send_and_wait("demo/flaky", timeout: 60)
    expect(run["output"]).to eq("succeeded on attempt 1")
  end

  it "fails without retrying on NonRetriableError" do
    run = send_and_wait("demo/fail", status: "Failed")
    expect(run["output"]).to include("message" => "this can't work")
  end
end
