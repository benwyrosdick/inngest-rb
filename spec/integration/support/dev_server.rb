# frozen_string_literal: true

require "net/http"
require "securerandom"
require_relative "../../../examples/functions"

DEV_SERVER = ENV.fetch("INNGEST_DEV_SERVER_URL", "http://127.0.0.1:8288")

def dev_server_running?
  Net::HTTP.get_response(URI(DEV_SERVER)).is_a?(Net::HTTPSuccess)
rescue StandardError
  false
end

def run_integration?
  ENV.fetch("INNGEST_INTEGRATION", nil) && dev_server_running?
end

# With INNGEST_INTEGRATION_SIGNING_KEY and INNGEST_INTEGRATION_EVENT_KEY set,
# the specs run in cloud mode against a self-hosted server, which signs
# requests and checks keys like Inngest Cloud:
#
#   inngest start --signing-key <hex> --event-key <key>
SIGNING_KEY = ENV.fetch("INNGEST_INTEGRATION_SIGNING_KEY", nil)

def integration_client_options
  return { dev: DEV_SERVER } unless SIGNING_KEY

  { dev: false, api_base_url: DEV_SERVER, event_api_base_url: DEV_SERVER, signing_key: SIGNING_KEY,
    event_key: ENV.fetch("INNGEST_INTEGRATION_EVENT_KEY") }
end

# Helpers for checking runs on the dev server.
module DevServerHelpers
  def send_and_wait(name, data = {}, status: "Completed", timeout: 30)
    event_id = integration_client.send_event(name: name, data: data).first
    wait_for_run(event_id, status: status, timeout: timeout)
  end

  # Waits for this app's run for the event to finish. Other apps on the dev
  # server may react to the same event, so runs are matched by function.
  def wait_for_run(event_id, status:, timeout: 30)
    deadline = Time.now + timeout
    loop do
      run = app_runs(event_id).find { |r| %w[Completed Failed Cancelled].include?(r["status"]) && r.key?("output") }
      if run
        raise "run ended as #{run["status"]}: #{run["output"].inspect}" if run["status"] != status

        return run
      end
      raise "timed out waiting for event #{event_id}" if Time.now > deadline

      sleep 0.25
    end
  end

  def app_runs(event_id)
    # The dev server caches this endpoint for 15s; a unique query bypasses it.
    url = URI("#{DEV_SERVER}/v1/events/#{event_id}/runs?poll=#{SecureRandom.hex(4)}")
    runs = JSON.parse(api_request(Net::HTTP::Get.new(url)).body)["data"] || []
    runs.select { |run| app_function_ids.include?(run["function_id"]) }
  end

  def app_function_ids
    @app_function_ids ||= begin
      request = Net::HTTP::Post.new(URI("#{DEV_SERVER}/v0/gql"), "Content-Type" => "application/json")
      request.body = JSON.generate({ query: "{ apps { name functions { id } } }" })
      response = api_request(request)
      app = JSON.parse(response.body).dig("data", "apps").find { |a| a["name"] == integration_client.app_id }
      raise "app #{integration_client.app_id} isn't synced" unless app

      app["functions"].map { |f| f["id"] }
    end
  end

  def api_request(request)
    request["Authorization"] = "Bearer #{Inngest::HTTP.hash_signing_key(SIGNING_KEY)}" if SIGNING_KEY
    Net::HTTP.start(request.uri.host, request.uri.port) { |http| http.request(request) }
  end
end

RSpec.shared_examples "an Inngest app" do
  include DevServerHelpers

  it "runs steps and sleeps" do
    run = send_and_wait("demo/greet", { "name" => "Ruby" })
    expect(run["output"]).to eq({ "greeting" => "Hello, Ruby!" })
  end

  it "waits for a matching event" do
    request_id = SecureRandom.uuid
    event_id = integration_client.send_event(name: "demo/approval.requested",
                                             data: { "request_id" => request_id }).first
    sleep 2 # Let the run reach wait_for_event.
    integration_client.send_event(name: "demo/approval.granted",
                                  data: { "request_id" => request_id,
                                          "approver" => "ops" })

    expect(wait_for_run(event_id, status: "Completed")["output"]).to eq("approved by ops")
  end

  it "invokes other functions and sends events from steps" do
    run = send_and_wait("demo/invoke")
    expect(run["output"]).to eq({ "doubled" => 42, "sent" => 1 })
  end

  it "retries failed steps" do
    run = send_and_wait("demo/flaky", timeout: 60)
    expect(run["output"]).to eq("succeeded on attempt 1")
  end

  it "runs parallel steps concurrently" do
    run = send_and_wait("demo/parallel")
    expect(run["output"]["squares"]).to eq([1, 4, 9])
    # Three 2-second steps; run one after another they'd take over 6 seconds.
    expect(run["output"]["elapsed"]).to be < 5
  end

  it "runs parallel branches with several steps" do
    run = send_and_wait("demo/parallel.branches")
    expect(run["output"]).to eq([%w[a1 a2], "b"])
  end

  it "fails without retrying on NonRetriableError" do
    run = send_and_wait("demo/fail", status: "Failed")
    expect(run["output"]).to include("message" => "this can't work")
  end
end
