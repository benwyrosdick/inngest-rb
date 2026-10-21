# frozen_string_literal: true

# Runs the example functions on a Connect worker against the dev server:
#
#   npx inngest-cli@latest dev   # in another terminal
#   INNGEST_DEV=1 ruby -Ilib examples/worker.rb

$stdout.sync = true

require_relative "functions"
require "inngest/connect"

client = Inngest::Client.new(app_id: "ruby-example")
Inngest::Connect::Worker.new(client: client, functions: Inngest::Function.registered).run
