# frozen_string_literal: true

# RSpec support: adds run_inngest_function to every example.
#
#   # spec/rails_helper.rb (or spec_helper.rb)
#   require "inngest/rspec"
#
#   RSpec.describe SendWelcomeEmail do
#     it "emails the user" do
#       result = run_inngest_function(described_class, event: { name: "user/signed_up", data: { "user_id" => 1 } })
#       expect(result).to be_completed
#     end
#   end

require "inngest/testing"

RSpec.configure do |config|
  config.include Inngest::Testing::Helpers
end
