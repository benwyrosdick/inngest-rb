# frozen_string_literal: true

require "puma"
require "inngest/serve"
require_relative "support/dev_server"

RSpec.describe "HTTP serve mode with the dev server", if: run_integration? do
  before(:all) do
    @client = Inngest::Client.new(app_id: "ruby-serve-integration", **integration_client_options,
                                  logger: Logger.new($stdout, level: :warn))
    app = Rack::URLMap.new("/api/inngest" => Inngest::Serve.new(client: @client,
                                                                functions: Inngest::Function.registered))
    @server = Puma::Server.new(app, nil, min_threads: 0, max_threads: 16)
    @server.add_tcp_listener("127.0.0.1", 0)
    @server.run
    url = URI("http://127.0.0.1:#{@server.connected_ports.first}/api/inngest")

    # Ask the app to sync itself, as a deploy hook would.
    response = Net::HTTP.start(url.host, url.port) { |http| http.request(Net::HTTP::Put.new(url)) }
    raise "sync failed: #{response.code} #{response.body}" unless response.code == "200"
  end

  after(:all) do
    @server&.stop(true)
  end

  def integration_client = @client

  it_behaves_like "an Inngest app"
end
