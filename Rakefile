# frozen_string_literal: true

require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

desc "Run end-to-end specs against a running dev server"
task "spec:integration" do
  ENV["INNGEST_INTEGRATION"] = "1"
  sh "bundle exec rspec spec/integration"
end

desc "Regenerate Ruby classes from proto/connect.proto (needs protoc)"
task :proto do
  sh "protoc --proto_path=proto --ruby_out=lib/inngest/connect/proto proto/connect.proto"
end

task default: :spec
