# frozen_string_literal: true

require "google/protobuf"
require_relative "connect/proto/connect_pb"
require_relative "connect/connection"
require_relative "connect/reply_buffer"
require_relative "connect/worker"

module Inngest
  # Connect runs functions over a persistent, outbound WebSocket to Inngest
  # instead of exposing an HTTP endpoint. See Inngest::Connect::Worker.
  module Connect
  end
end
