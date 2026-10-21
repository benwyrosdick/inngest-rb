# frozen_string_literal: true

require "digest"
require "json"
require "rack"

module Inngest
  # A Rack app that serves functions over HTTP: Inngest calls it to sync
  # functions (PUT), run them (POST) and check its health (GET).
  #
  #   # config/routes.rb
  #   mount Inngest::Serve.new => "/api/inngest"
  #
  #   # config.ru (any Rack framework)
  #   map("/api/inngest") { run Inngest::Serve.new(functions: [SendWelcomeEmail]) }
  #
  # Use this when you can't run a long-lived Connect worker (e.g. on
  # serverless platforms). Each step is a separate request to your app, so
  # long steps hold a web server thread for their duration.
  class Serve
    SCHEMA_VERSION = "2024-05-24"
    CAPABILITIES = { "in_band_sync" => "v1", "trust_probe" => "v1" }.freeze
    REQUEST_VERSION = "2"

    # @param client [Inngest::Client] defaults to Inngest.client
    # @param functions [Array<Class>, nil] functions to serve; defaults to every
    #   loaded Inngest::Function subclass, looked up on each request
    # @param serve_origin [String] public origin Inngest should call, e.g.
    #   "https://example.com", when it differs from the request's
    # @param serve_path [String] public path Inngest should call, when it
    #   differs from the request's
    def initialize(client: nil, functions: nil, serve_origin: ENV.fetch("INNGEST_SERVE_ORIGIN", nil),
                   serve_path: ENV.fetch("INNGEST_SERVE_PATH", nil), framework: nil)
      @client = client
      @functions = functions
      @serve_origin = serve_origin
      @serve_path = serve_path
      @framework = framework || (defined?(::Rails::Railtie) ? "rails" : "rack")
    end

    def client
      @client || Inngest.client
    end

    def call(env)
      request = Rack::Request.new(env)
      # Read the body once; under Rack 3 the input stream may not be rewindable.
      body = request.body&.read.to_s
      query = request.GET
      case request.request_method
      when "GET" then introspect(request)
      when "PUT" then sync(request, body, query)
      when "POST" then query["probe"] == "trust" ? trust_probe(request, body) : execute(request, body, query)
      else respond(405, { "message" => "method not allowed" })
      end
    rescue StandardError => e
      client.logger.error("[inngest] #{e.class}: #{e.message}")
      respond(500, { "name" => e.class.name, "message" => e.message })
    end

    private

    # --- POST: run a function ---

    def execute(request, body, query)
      signing_key = verify(request, body)
      return unauthorized if signing_key == :invalid

      function_id = query["fnId"]
      function_class, failure = functions_by_id[function_id]
      unless function_class
        return respond(500, { "name" => "FunctionNotFoundError",
                              "message" => "function #{function_id.inspect} isn't served by this app" })
      end

      result = Executor.new(client: client, function_class: function_class, payload: JSON.parse(body),
                            step_id: query["stepId"], failure: failure, request: request).call

      headers = { "X-Inngest-No-Retry" => result.no_retry.to_s }
      headers["Retry-After"] = result.retry_after if result.retry_after
      # Non-retriable errors use 400 so Inngest won't retry them.
      status = result.status == 500 && result.no_retry ? 400 : result.status
      respond(status, result.body, headers: headers, signing_key: signing_key)
    end

    def trust_probe(request, body)
      return respond(200, {}) if client.dev?

      signing_key = verify(request, body)
      return unauthorized if signing_key == :invalid

      respond(200, {}, signing_key: signing_key)
    end

    # --- PUT: sync ---

    def sync(request, body, query)
      # Cloud can ask for an in-band sync by sending a signed PUT; the response
      # carries the app's configuration directly.
      return in_band_sync(request, body) if !client.dev? && request.get_header("HTTP_X_INNGEST_SIGNATURE")

      server_kind = request.get_header("HTTP_X_INNGEST_SERVER_KIND")
      if server_kind && server_kind != mode
        message = "sync rejected: the request came from #{server_kind == "dev" ? "a dev server" : "Inngest Cloud"} " \
                  "but this app is configured for #{client.dev? ? "the dev server" : "Inngest Cloud"}"
        return respond(400, { "message" => message, "modified" => false })
      end

      out_of_band_sync(request, query, server_kind)
    end

    def out_of_band_sync(request, query, server_kind)
      deploy_id = query["deployId"]
      url = app_url(request.base_url, request.path, query.except("deployId"))
      payload = {
        "url" => url,
        "deployType" => "ping",
        "appName" => client.app_id,
        "appVersion" => client.app_version,
        "sdk" => SDK_IDENTIFIER,
        "v" => "0.1",
        "framework" => @framework,
        "capabilities" => CAPABILITIES,
        "functions" => function_configs(url)
      }.compact

      register_url = "#{client.api_origin}/fn/register"
      register_url += "?#{URI.encode_www_form(deployId: deploy_id)}" if deploy_id
      headers = client.default_headers.merge("Content-Type" => "application/json")
      headers["X-Inngest-Expected-Server-Kind"] = server_kind if server_kind
      response = HTTP.authed_request(:post, register_url, signing_key: client.dev? ? nil : client.signing_key,
                                                          signing_key_fallback: client.signing_key_fallback,
                                                          headers: headers, body: JSON.generate(payload), timeout: 30)

      result = parse_json(response.body)
      unless response.success?
        message = (result.is_a?(Hash) && result["error"]) || "sync failed with status #{response.status}"
        client.logger.error("[inngest] sync failed: #{message}")
        return respond(500, { "message" => message, "modified" => false })
      end

      modified = result.is_a?(Hash) && result["modified"] == true
      client.logger.info("[inngest] synced #{function_classes.size} functions to #{client.api_origin}")
      respond(200, { "message" => "Successfully synced.", "modified" => modified },
              headers: { "X-Inngest-Sync-Kind" => "out_of_band" })
    end

    def in_band_sync(request, body)
      signing_key = verify(request, body)
      return unauthorized if signing_key == :invalid

      requested_url = parse_json(body)&.dig("url")
      return respond(400, { "message" => "in-band sync request is missing a url" }) unless requested_url.is_a?(String)

      uri = URI(requested_url)
      url = app_url("#{uri.scheme}://#{uri.authority}", uri.path, URI.decode_www_form(uri.query.to_s).to_h)
      response = {
        "app_id" => client.app_id,
        "app_version" => client.app_version,
        "env" => client.env,
        "framework" => @framework,
        "functions" => function_configs(url),
        "inspection" => authenticated_inspection,
        "platform" => nil,
        "sdk_author" => "inngest",
        "sdk_language" => LANGUAGE,
        "sdk_version" => VERSION,
        "url" => url
      }
      respond(200, response, headers: { "X-Inngest-Sync-Kind" => "in_band" }, signing_key: signing_key)
    end

    # --- GET: introspection ---

    def introspect(request)
      header = request.get_header("HTTP_X_INNGEST_SIGNATURE")
      signing_key = header && !client.dev? ? verify(request, "") : nil
      return respond(200, authenticated_inspection) if signing_key && signing_key != :invalid

      respond(200, {
                "authentication_succeeded" => signing_key == :invalid ? false : nil,
                "capabilities" => CAPABILITIES,
                "function_count" => function_classes.size,
                "has_event_key" => !client.event_key.nil?,
                "has_signing_key" => !client.signing_key.nil?,
                "has_signing_key_fallback" => !client.signing_key_fallback.nil?,
                "mode" => mode,
                "schema_version" => SCHEMA_VERSION
              })
    end

    def authenticated_inspection
      {
        "api_origin" => client.api_origin,
        "app_id" => client.app_id,
        "authentication_succeeded" => true,
        "capabilities" => CAPABILITIES,
        "env" => client.env,
        "event_api_origin" => client.event_api_origin,
        "event_key_hash" => client.event_key && Digest::SHA256.hexdigest(client.event_key),
        "framework" => @framework,
        "function_count" => function_classes.size,
        "has_event_key" => !client.event_key.nil?,
        "has_signing_key" => !client.signing_key.nil?,
        "has_signing_key_fallback" => !client.signing_key_fallback.nil?,
        "mode" => mode,
        "schema_version" => SCHEMA_VERSION,
        "sdk_language" => LANGUAGE,
        "sdk_version" => VERSION,
        "serve_origin" => @serve_origin,
        "serve_path" => @serve_path,
        "signing_key_fallback_hash" => key_hash(client.signing_key_fallback),
        "signing_key_hash" => key_hash(client.signing_key)
      }
    end

    # --- Helpers ---

    def key_hash(key)
      key && HTTP.hash_signing_key(key)
    end

    # Returns the signing key that signed the request, nil in dev mode (the dev
    # server doesn't sign requests), or :invalid.
    def verify(request, body)
      return nil if client.dev?

      Signature.verify!(request.get_header("HTTP_X_INNGEST_SIGNATURE"), body,
                        keys: [client.signing_key, client.signing_key_fallback])
    rescue Signature::InvalidError => e
      client.logger.warn("[inngest] rejected request: #{e.message}")
      :invalid
    end

    def mode
      client.dev? ? "dev" : "cloud"
    end

    def function_classes
      list = @functions.respond_to?(:call) ? @functions.call : @functions
      list || Inngest.functions
    end

    def functions_by_id
      function_classes.each_with_object({}) do |fn, index|
        index[fn.fully_qualified_id(client.app_id)] = [fn, false]
        index[fn.failure_function_id(client.app_id)] = [fn, true] if fn.on_failure?
      end
    end

    def function_configs(url)
      function_classes.flat_map { |fn| fn.to_configs(app_id: client.app_id, url: url) }
    end

    # The URL Inngest should call, with any configured origin/path overrides.
    def app_url(origin, path, query)
      if @serve_origin
        origin = @serve_origin.include?("://") ? @serve_origin : "https://#{@serve_origin}"
      end
      path = @serve_path if @serve_path
      url = "#{origin.chomp("/")}#{path}"
      query.empty? ? url : "#{url}?#{URI.encode_www_form(query)}"
    end

    def parse_json(body)
      JSON.parse(body.to_s)
    rescue JSON::ParserError
      nil
    end

    def unauthorized
      [401, { "content-type" => "application/json" }, [JSON.generate({ "message" => "Unauthorized" })]]
    end

    def respond(status, body, headers: {}, signing_key: nil)
      body = JSON.generate(body) unless body.is_a?(String)
      headers = {
        "content-type" => "application/json",
        "x-inngest-sdk" => SDK_IDENTIFIER,
        "x-inngest-req-version" => REQUEST_VERSION,
        "x-inngest-framework" => @framework
      }.merge(headers.transform_keys(&:downcase))
      headers["x-inngest-env"] = client.env if client.env
      # Lets Inngest verify the response came from this app.
      headers["x-inngest-signature"] = Signature.sign(body, signing_key) if signing_key.is_a?(String)
      [status, headers, [body]]
    end
  end
end
