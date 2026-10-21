# frozen_string_literal: true

# End-to-end tests against a running Inngest dev server:
#
#   npx inngest-cli@latest dev --no-discovery --no-poll
#   bundle exec rake spec:integration

require_relative "support/dev_server"

RSpec.describe "Connect worker with the dev server", if: run_integration? do
  before(:all) do
    @client = Inngest::Client.new(app_id: "ruby-connect-integration", **integration_client_options,
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

  def integration_client = @client

  it_behaves_like "an Inngest app"
end
