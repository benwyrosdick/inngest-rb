# frozen_string_literal: true

require_relative "inngest/version"
require_relative "inngest/errors"
require_relative "inngest/duration"
require_relative "inngest/event"
require_relative "inngest/http"
require_relative "inngest/client"
require_relative "inngest/function"
require_relative "inngest/middleware"
require_relative "inngest/step"
require_relative "inngest/executor"
require_relative "inngest/jcs"
require_relative "inngest/signature"

# Ruby SDK for Inngest (https://www.inngest.com).
module Inngest
  # Loaded on demand so apps that only send events don't load protobuf.
  autoload :Connect, "inngest/connect"
  autoload :CLI, "inngest/cli"
  autoload :Serve, "inngest/serve"

  class << self
    attr_writer :client

    # Configures the default client.
    #
    #   Inngest.configure(app_id: "my-app")
    def configure(**options)
      @client = Client.new(**default_client_options, **options)
    end

    # The default client, built from INNGEST_* environment variables (and
    # Rails defaults, when running in Rails) unless configured.
    def client
      @client ||= Client.new(**default_client_options)
    end

    # Sends events with the default client. Returns the event IDs.
    def send_event(*events)
      client.send_event(*events)
    end
    alias send_events send_event

    # Called before listing functions, to make sure they're loaded. The Railtie
    # sets this to load app/inngest when Rails loads code lazily.
    attr_accessor :function_loader

    # Every loaded Inngest::Function subclass with a trigger.
    def functions
      function_loader&.call
      Function.registered
    end

    # Options merged into the default client; the Railtie fills these in.
    def default_client_options
      @default_client_options ||= { app_id: ENV.fetch("INNGEST_APP_ID", nil) }.compact
    end
  end
end

require_relative "inngest/railtie" if defined?(Rails::Railtie)
