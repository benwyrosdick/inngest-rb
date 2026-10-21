# frozen_string_literal: true

require "net/http"
require "uri"
require "digest"

module Inngest
  # Minimal HTTP helper built on Net::HTTP so the SDK has no HTTP client
  # dependency.
  module HTTP
    Response = Struct.new(:status, :body, :headers, keyword_init: true) do
      def success?
        status.between?(200, 299)
      end
    end

    module_function

    def request(method, url, body: nil, headers: {}, timeout: 10)
      uri = URI(url)
      klass = { get: Net::HTTP::Get, post: Net::HTTP::Post, put: Net::HTTP::Put }.fetch(method)
      req = klass.new(uri)
      headers.each { |k, v| req[k] = v }
      req.body = body if body

      res = Net::HTTP.start(
        uri.host, uri.port,
        use_ssl: uri.scheme == "https",
        open_timeout: timeout, read_timeout: timeout, write_timeout: timeout
      ) { |http| http.request(req) }

      Response.new(status: res.code.to_i, body: res.body, headers: res.to_hash)
    end

    # Sends a request authenticated with the signing key, retrying once with the
    # fallback key on 401/403 (e.g. while rotating keys).
    def authed_request(method, url, signing_key:, signing_key_fallback: nil, headers: {}, **opts)
      keys = [signing_key, signing_key_fallback].compact
      keys = [nil] if keys.empty?

      response = nil
      keys.each do |key|
        auth = key ? { "Authorization" => "Bearer #{hash_signing_key(key)}" } : {}
        response = request(method, url, headers: headers.merge(auth), **opts)
        break unless [401, 403].include?(response.status)
      end
      response
    end

    # Signing keys are sent as the SHA-256 of the key's hex bytes, with the
    # "signkey-<env>-" prefix removed.
    def hash_signing_key(key)
      raw = key.sub(/\Asignkey-\w+-/, "")
      Digest::SHA256.hexdigest([raw].pack("H*"))
    end
  end
end
