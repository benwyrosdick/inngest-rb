# frozen_string_literal: true

RSpec.describe Inngest::Duration do
  it "converts seconds to the largest whole unit" do
    expect(described_class.to_time_string(30)).to eq("30s")
    expect(described_class.to_time_string(90)).to eq("90s")
    expect(described_class.to_time_string(120)).to eq("2m")
    expect(described_class.to_time_string(86_400)).to eq("1d")
    expect(described_class.to_time_string(604_800)).to eq("1w")
    expect(described_class.to_time_string(1.5)).to eq("1500ms")
  end

  it "passes time strings through" do
    expect(described_class.to_time_string("2h30m")).to eq("2h30m")
  end

  it "rejects non-positive durations" do
    expect { described_class.to_time_string(0) }.to raise_error(ArgumentError)
  end
end

RSpec.describe Inngest::HTTP do
  it "hashes signing keys without their prefix" do
    key = "signkey-prod-12345678"
    expect(described_class.hash_signing_key(key)).to eq(Digest::SHA256.hexdigest(["12345678"].pack("H*")))
  end
end

RSpec.describe Inngest::Client do
  around do |example|
    keys = %w[INNGEST_DEV INNGEST_BASE_URL INNGEST_API_BASE_URL INNGEST_EVENT_API_BASE_URL INNGEST_EVENT_KEY]
    saved = keys.to_h { |k| [k, ENV.fetch(k, nil)] }
    keys.each { |k| ENV.delete(k) }
    example.run
  ensure
    saved.each { |k, v| v.nil? ? ENV.delete(k) : ENV[k] = v }
  end

  it "defaults to Inngest Cloud" do
    client = described_class.new(app_id: "app")
    expect(client).not_to be_dev
    expect(client.api_origin).to eq("https://api.inngest.com")
    expect(client.event_api_origin).to eq("https://inn.gs")
  end

  it "uses the dev server when INNGEST_DEV is set" do
    ENV["INNGEST_DEV"] = "1"
    client = described_class.new(app_id: "app")
    expect(client).to be_dev
    expect(client.api_origin).to eq("http://127.0.0.1:8288")
    expect(client.event_key).to eq("NO_EVENT_KEY_SET")
  end

  it "accepts a dev server URL" do
    client = described_class.new(app_id: "app", dev: "http://inngest:8288/")
    expect(client.api_origin).to eq("http://inngest:8288")
  end

  it "requires an app ID" do
    expect { described_class.new(app_id: nil) }.to raise_error(Inngest::ConfigError)
  end
end

RSpec.describe Inngest::Connect::ReplyBuffer do
  it "evicts the oldest replies when full" do
    buffer = described_class.new(max_bytes: 10)
    buffer.add("a", "12345")
    buffer.add("b", "12345")
    buffer.add("c", "1")

    expect(buffer.all.map(&:request_id)).to eq(%w[b c])
  end
end
