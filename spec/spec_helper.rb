# frozen_string_literal: true

require "inngest"
require "inngest/connect"
require "digest"
require "stringio"

module SpecHelpers
  def test_client(**opts)
    Inngest::Client.new(app_id: "test-app", dev: true, logger: Logger.new(StringIO.new), **opts)
  end

  def hashed(id)
    Digest::SHA1.hexdigest(id)
  end

  # Builds an execution request body like the one Inngest sends.
  def request_payload(event: { "name" => "test/event", "data" => {} }, steps: {}, attempt: 0, max_attempts: nil,
                      disable_immediate_execution: false)
    {
      "event" => event,
      "events" => [event],
      "steps" => steps,
      "ctx" => {
        "run_id" => "run-123",
        "attempt" => attempt,
        "max_attempts" => max_attempts,
        "disable_immediate_execution" => disable_immediate_execution,
        "use_api" => false,
        "stack" => { "stack" => steps.keys }
      }.compact
    }
  end

  def execute(function_class, step_id: nil, failure: false, **payload_opts)
    result = Inngest::Executor.new(client: test_client, function_class: function_class,
                                   payload: request_payload(**payload_opts), step_id: step_id,
                                   failure: failure).call
    [result, JSON.parse(result.body)]
  end
end

RSpec.configure do |config|
  config.include SpecHelpers
  config.example_status_persistence_file_path = ".rspec_status"
  config.disable_monkey_patching!
  config.expect_with(:rspec) { |c| c.syntax = :expect }
end
