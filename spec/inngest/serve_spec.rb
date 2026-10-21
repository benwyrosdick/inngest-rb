# frozen_string_literal: true

require "inngest/serve"
require "rack/test"

RSpec.describe Inngest::Serve do
  include Rack::Test::Methods

  let(:signing_key) { "signkey-test-abcd1234" }
  let(:fallback_key) { "signkey-test-ffff0000" }
  let(:dev) { false }
  let(:client) do
    Inngest::Client.new(app_id: "shop", dev: dev, signing_key: signing_key, signing_key_fallback: fallback_key,
                        event_key: "event-key", logger: Logger.new(StringIO.new))
  end
  let(:functions) do
    [
      Class.new(Inngest::Function) do
        id "greet"
        trigger event: "shop/greet"
        def call = step.run("hello") { "hi #{event.data["name"]}" }
      end,
      Class.new(Inngest::Function) do
        id "fatal"
        trigger event: "shop/fatal"
        def call = raise(Inngest::NonRetriableError, "nope")
      end,
      Class.new(Inngest::Function) do
        id "later"
        trigger event: "shop/later"
        def call = raise(Inngest::RetryAfterError.new("busy", retry_after: Time.utc(2030, 1, 1)))
      end
    ]
  end
  let(:serve_options) { {} }
  let(:app) { described_class.new(client: client, functions: functions, **serve_options) }

  def signed_headers(body, key: signing_key, time: Time.now)
    { "HTTP_X_INNGEST_SIGNATURE" => Inngest::Signature.sign(Inngest::Signature.canonical(body), key, now: time) }
  end

  def call_body(name: "shop/greet")
    JSON.generate({ "event" => { "name" => name, "data" => { "name" => "Ada" } }, "steps" => {},
                    "ctx" => { "run_id" => "run-1", "attempt" => 0, "disable_immediate_execution" => false } })
  end

  def json
    JSON.parse(last_response.body)
  end

  def response_signed_by?(key)
    params = URI.decode_www_form(last_response.headers["x-inngest-signature"].to_s).to_h
    params["s"] == Inngest::Signature.hmac(key, last_response.body, params["t"])
  end

  describe "POST (run a function)" do
    it "runs the function when the request is signed, and signs the response" do
      body = call_body
      post "/?fnId=shop-greet&stepId=step", body, signed_headers(body)

      expect(last_response.status).to eq(206)
      expect(json.first).to include("op" => "StepRun", "data" => "hi Ada")
      expect(last_response.headers).to include("x-inngest-sdk" => Inngest::SDK_IDENTIFIER,
                                               "x-inngest-req-version" => "2")
      expect(response_signed_by?(signing_key)).to be(true)
    end

    it "accepts the fallback signing key" do
      body = call_body
      post "/?fnId=shop-greet", body, signed_headers(body, key: fallback_key)

      expect(last_response.status).to eq(206)
      expect(response_signed_by?(fallback_key)).to be(true)
    end

    it "verifies the canonical form of the body" do
      body = "{ \"steps\": {}, \"event\": {\"data\": {\"name\": \"Ada\"}, \"name\": \"shop/greet\"},\n \"ctx\": {} }"
      post "/?fnId=shop-greet", body, signed_headers(body)
      expect(last_response.status).to eq(206)
    end

    it "rejects unsigned, tampered and expired requests" do
      body = call_body
      post "/?fnId=shop-greet", body
      expect(last_response.status).to eq(401)

      post "/?fnId=shop-greet", body.sub("Ada", "Eve"), signed_headers(body)
      expect(last_response.status).to eq(401)

      post "/?fnId=shop-greet", body, signed_headers(body, time: Time.now - 600)
      expect(last_response.status).to eq(401)

      post "/?fnId=shop-greet", body, signed_headers(body, key: "signkey-test-00000000")
      expect(last_response.status).to eq(401)
    end

    it "returns 400 with X-Inngest-No-Retry for non-retriable errors" do
      body = call_body(name: "shop/fatal")
      post "/?fnId=shop-fatal", body, signed_headers(body)

      expect(last_response.status).to eq(400)
      expect(last_response.headers["x-inngest-no-retry"]).to eq("true")
      expect(json).to include("name" => "Inngest::NonRetriableError", "message" => "nope")
    end

    it "returns 500 with Retry-After for RetryAfterError" do
      body = call_body(name: "shop/later")
      post "/?fnId=shop-later", body, signed_headers(body)

      expect(last_response.status).to eq(500)
      expect(last_response.headers).to include("x-inngest-no-retry" => "false",
                                               "retry-after" => "2030-01-01T00:00:00.000Z")
    end

    it "returns 500 for unknown functions" do
      body = call_body
      post "/?fnId=shop-missing", body, signed_headers(body)
      expect(last_response.status).to eq(500)
    end

    context "with the dev server" do
      let(:dev) { true }

      it "doesn't require a signature" do
        post "/?fnId=shop-greet", call_body
        expect(last_response.status).to eq(206)
        expect(last_response.headers).not_to have_key("x-inngest-signature")
      end
    end
  end

  describe "POST ?probe=trust" do
    it "answers signed probes with a signed response" do
      post "/?probe=trust", "", signed_headers("")
      expect(last_response.status).to eq(200)
      expect(response_signed_by?(signing_key)).to be(true)
    end

    it "rejects unsigned probes" do
      post "/?probe=trust"
      expect(last_response.status).to eq(401)
    end
  end

  describe "GET (introspection)" do
    it "returns the unauthenticated view for unsigned requests" do
      get "/"
      expect(json).to eq(
        "authentication_succeeded" => nil,
        "capabilities" => { "in_band_sync" => "v1", "trust_probe" => "v1" },
        "function_count" => 3,
        "has_event_key" => true,
        "has_signing_key" => true,
        "has_signing_key_fallback" => true,
        "mode" => "cloud",
        "schema_version" => "2024-05-24"
      )
    end

    it "reports failed authentication" do
      get "/", {}, { "HTTP_X_INNGEST_SIGNATURE" => "t=1&s=bad" }
      expect(json["authentication_succeeded"]).to be(false)
      expect(json).not_to have_key("app_id")
    end

    it "returns the authenticated view for signed requests" do
      get "/", {}, signed_headers("")
      expect(json).to include("authentication_succeeded" => true, "app_id" => "shop",
                              "signing_key_hash" => Inngest::HTTP.hash_signing_key(signing_key),
                              "event_key_hash" => Digest::SHA256.hexdigest("event-key"))
    end
  end

  describe "PUT (sync)" do
    let(:register_calls) { [] }

    before do
      allow(Inngest::HTTP).to receive(:request) do |method, url, **opts|
        register_calls << { method: method, url: url, headers: opts[:headers], body: JSON.parse(opts[:body]) }
        Inngest::HTTP::Response.new(status: 200, body: '{"ok":true,"modified":true}', headers: {})
      end
    end

    it "registers functions with Inngest" do
      put "https://shop.example.com/api/inngest?deployId=deploy-1&token=abc", "",
          "HTTP_X_INNGEST_SERVER_KIND" => "cloud"

      expect(last_response.status).to eq(200)
      expect(json).to eq("message" => "Successfully synced.", "modified" => true)

      expect(register_calls.size).to eq(1)
      call = register_calls.first
      expect(call[:url]).to eq("https://api.inngest.com/fn/register?deployId=deploy-1")
      expect(call[:headers]).to include("Authorization" => "Bearer #{Inngest::HTTP.hash_signing_key(signing_key)}",
                                        "X-Inngest-Expected-Server-Kind" => "cloud")
      expect(call[:body]).to include("url" => "https://shop.example.com/api/inngest?token=abc",
                                     "appName" => "shop", "deployType" => "ping", "v" => "0.1",
                                     "sdk" => Inngest::SDK_IDENTIFIER)
      expect(call[:body]["functions"].map { |f| f["id"] }).to eq(%w[shop-greet shop-fatal shop-later])
      expect(call[:body]["functions"].first.dig("steps", "step", "runtime")).to eq(
        "type" => "http", "url" => "https://shop.example.com/api/inngest?token=abc&fnId=shop-greet&stepId=step"
      )
    end

    it "rejects syncs from the wrong kind of server" do
      put "/", "", "HTTP_X_INNGEST_SERVER_KIND" => "dev"
      expect(last_response.status).to eq(400)
      expect(register_calls).to be_empty
    end

    it "reports registration errors" do
      allow(Inngest::HTTP).to receive(:request)
        .and_return(Inngest::HTTP::Response.new(status: 400, body: '{"error":"bad config"}', headers: {}))

      put "/"
      expect(last_response.status).to eq(500)
      expect(json).to eq("message" => "bad config", "modified" => false)
    end

    context "with serve_origin and serve_path" do
      let(:serve_options) { { serve_origin: "public.example.com", serve_path: "/inngest" } }

      it "uses them in the synced URL" do
        put "http://10.0.0.5:3000/api/inngest"
        expect(register_calls.first[:body]["url"]).to eq("https://public.example.com/inngest")
      end
    end

    it "responds to signed PUTs with an in-band sync" do
      body = JSON.generate({ "url" => "https://shop.example.com/api/inngest" })
      put "/", body, signed_headers(body)

      expect(last_response.status).to eq(200)
      expect(last_response.headers["x-inngest-sync-kind"]).to eq("in_band")
      expect(response_signed_by?(signing_key)).to be(true)
      expect(json).to include("app_id" => "shop", "url" => "https://shop.example.com/api/inngest",
                              "sdk_language" => "rb")
      expect(json["functions"].size).to eq(3)
      expect(json.dig("inspection", "authentication_succeeded")).to be(true)
      expect(register_calls).to be_empty
    end
  end
end
