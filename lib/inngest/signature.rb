# frozen_string_literal: true

require "openssl"
require "uri"

module Inngest
  # Signs and verifies the X-Inngest-Signature header.
  #
  # The signature is an HMAC-SHA256, keyed with the signing key minus its
  # "signkey-<env>-" prefix, over the body followed by a Unix timestamp:
  # "t=<timestamp>&s=<hex digest>". Inngest signs the JCS-canonical form of
  # request bodies; the SDK signs its response bodies as-is.
  module Signature
    MAX_AGE = 5 * 60

    class InvalidError < Error; end

    module_function

    # Returns the key that signed the request. Raises InvalidError if no key
    # did, or if the signature is missing, malformed or too old.
    def verify!(header, body, keys:, now: Time.now)
      keys = keys.compact
      raise InvalidError, "no signing key is configured" if keys.empty?
      raise InvalidError, "missing X-Inngest-Signature header" if header.nil? || header.empty?

      params = URI.decode_www_form(header).to_h
      timestamp = Integer(params["t"], exception: false)
      signature = params["s"]
      raise InvalidError, "malformed X-Inngest-Signature header" if timestamp.nil? || signature.nil?
      raise InvalidError, "signature has expired" if now.to_i - timestamp > MAX_AGE

      signed_body = canonical(body)
      key = keys.find do |candidate|
        OpenSSL.secure_compare(hmac(candidate, signed_body, timestamp), signature)
      end
      raise InvalidError, "invalid signature" unless key

      key
    end

    # Signs a response body for Inngest to verify.
    def sign(body, key, now: Time.now)
      timestamp = now.to_i
      "t=#{timestamp}&s=#{hmac(key, body, timestamp)}"
    end

    def hmac(key, body, timestamp)
      OpenSSL::HMAC.hexdigest("SHA256", key.sub(/\Asignkey-\w+-/, ""), "#{body}#{timestamp}")
    end

    def canonical(body)
      return "" if body.nil? || body.empty?

      JCS.canonicalize(body)
    rescue JSON::ParserError, ArgumentError
      body # Inngest signs the raw body if it isn't valid JSON.
    end
  end
end
