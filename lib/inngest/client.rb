# frozen_string_literal: true

require "json"
require "logger"
require "securerandom"

module Inngest
  # Holds app configuration and talks to the Inngest API (sending events,
  # fetching run data).
  #
  # Configuration falls back to the standard INNGEST_* environment variables,
  # so most apps only need to set an app ID.
  class Client
    DEFAULT_API_ORIGIN = "https://api.inngest.com"
    DEFAULT_EVENT_API_ORIGIN = "https://inn.gs"
    DEV_SERVER_ORIGIN = "http://127.0.0.1:8288"
    DEV_SERVER_EVENT_KEY = "NO_EVENT_KEY_SET"

    attr_reader :app_id, :app_version, :env, :event_key, :signing_key, :signing_key_fallback,
                :api_origin, :event_api_origin, :logger, :middleware

    # @param app_id [String] unique, stable ID for this app
    # @param app_version [String, nil] changes with each deploy (e.g. a git SHA)
    #   so Inngest can handle rolling deploys
    # @param dev [Boolean, String, nil] true (or a dev server URL) to use the
    #   local dev server; defaults to INNGEST_DEV, otherwise Inngest Cloud
    # @param middleware [Array<Class>] Inngest::Middleware subclasses that run
    #   for every function and event send
    def initialize(
      app_id: nil,
      app_version: ENV.fetch("INNGEST_APP_VERSION", nil),
      dev: nil,
      env: ENV.fetch("INNGEST_ENV", nil),
      event_key: ENV.fetch("INNGEST_EVENT_KEY", nil),
      signing_key: ENV.fetch("INNGEST_SIGNING_KEY", nil),
      signing_key_fallback: ENV.fetch("INNGEST_SIGNING_KEY_FALLBACK", nil),
      api_base_url: nil,
      event_api_base_url: nil,
      logger: nil,
      middleware: []
    )
      if app_id.nil? || app_id.to_s.strip.empty?
        raise ConfigError, "app_id is required (set INNGEST_APP_ID or call Inngest.configure(app_id: ...))"
      end

      @app_id = app_id.to_s
      @app_version = app_version
      @env = env
      @signing_key = signing_key
      @signing_key_fallback = signing_key_fallback
      @logger = logger || default_logger
      @middleware = Array(middleware)

      dev_origin = resolve_dev(dev.nil? ? ENV.fetch("INNGEST_DEV", nil) : dev)
      @dev = !dev_origin.nil?

      base_url = ENV.fetch("INNGEST_BASE_URL", nil)
      @api_origin = strip_slash(api_base_url || ENV["INNGEST_API_BASE_URL"] || base_url ||
                                dev_origin || DEFAULT_API_ORIGIN)
      @event_api_origin = strip_slash(event_api_base_url || ENV["INNGEST_EVENT_API_BASE_URL"] || base_url ||
                                      dev_origin || DEFAULT_EVENT_API_ORIGIN)
      @event_key = event_key || (dev? ? DEV_SERVER_EVENT_KEY : nil)
    end

    def dev?
      @dev
    end

    # Sends one or more events. Each event is a Hash (or Inngest::Event) with at
    # least a name. Returns the IDs of the sent events.
    #
    #   client.send_event(name: "user/signed_up", data: { user_id: 1 })
    def send_event(*events)
      deliver_events(events, middleware.map { |entry| Middleware.build(entry, client: self) })
    end
    alias send_events send_event

    # Sends events, running the given middleware instances' send hooks.
    def deliver_events(events, middleware_instances)
      events = events.flatten.map { |e| normalize_event(e) }
      return [] if events.empty?

      middleware_instances.each { |m| m.before_send_events(events) }
      begin
        ids = post_events(events)
      rescue StandardError => e
        result = Middleware::SendResult.new(ids: e.respond_to?(:ids) ? e.ids : [], error: e)
        middleware_instances.each { |m| m.after_send_events(result) }
        raise
      end
      middleware_instances.each { |m| m.after_send_events(Middleware::SendResult.new(ids: ids, error: nil)) }
      ids
    end

    # Posts events to the event API. Returns their IDs.
    def post_events(events)
      raise ConfigError, "event key is required to send events (set INNGEST_EVENT_KEY)" if event_key.nil?

      headers = default_headers.merge("Content-Type" => "application/json")
      response = HTTP.request(:post, "#{event_api_origin}/e/#{event_key}", body: JSON.generate(events),
                                                                           headers: headers)
      body = parse_json(response.body)
      ids = body.is_a?(Hash) ? Array(body["ids"]) : []

      unless response.success?
        message = body.is_a?(Hash) ? body["error"] : nil
        raise SendEventError.new("failed to send events (#{response.status}): #{message || response.body}", ids: ids)
      end

      ids
    end

    # Headers the SDK sends on every outgoing request.
    def default_headers
      headers = { "User-Agent" => SDK_IDENTIFIER, "X-Inngest-SDK" => SDK_IDENTIFIER }
      headers["X-Inngest-Env"] = env if env
      headers
    end

    # GETs a JSON path on the Inngest API, authenticated with the signing key.
    def api_get(path)
      response = HTTP.authed_request(:get, "#{api_origin}#{path}", signing_key: signing_key,
                                                                   signing_key_fallback: signing_key_fallback,
                                                                   headers: default_headers)
      unless response.success?
        raise APIError.new("GET #{path} failed (#{response.status})", status: response.status,
                                                                      body: response.body)
      end

      JSON.parse(response.body)
    end

    private

    def normalize_event(event)
      hash = event.respond_to?(:to_h) ? event.to_h : event
      hash = hash.transform_keys(&:to_s)
      name = hash["name"]
      raise ArgumentError, "event name is required" if name.nil? || name.to_s.empty?

      {
        "id" => hash["id"],
        "name" => name.to_s,
        "data" => hash["data"] || {},
        "user" => hash["user"],
        "ts" => hash["ts"] || (Time.now.to_f * 1000).to_i,
        "v" => hash["v"]
      }.compact
    end

    def resolve_dev(value)
      case value
      when nil, false, "", "0", "false" then nil
      when true, "1", "true" then DEV_SERVER_ORIGIN
      when String
        unless value.match?(%r{\Ahttps?://})
          raise ConfigError,
                "INNGEST_DEV must be a boolean or a URL, got #{value.inspect}"
        end

        value
      else raise ConfigError, "dev must be a boolean or URL, got #{value.inspect}"
      end
    end

    def strip_slash(url)
      url.to_s.chomp("/")
    end

    def parse_json(body)
      JSON.parse(body.to_s)
    rescue JSON::ParserError
      nil
    end

    def default_logger
      Logger.new($stdout, progname: "inngest", level: ENV.fetch("INNGEST_LOG_LEVEL", "info"))
    end
  end
end
