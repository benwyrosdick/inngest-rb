# frozen_string_literal: true

module Inngest
  # An event received by a function. Payload keys are always strings because
  # the event arrives as JSON.
  class Event
    attr_reader :id, :name, :data, :user, :ts, :v

    def self.from_h(hash)
      return nil if hash.nil?

      new(
        id: hash["id"],
        name: hash["name"],
        data: hash["data"] || {},
        user: hash["user"],
        ts: hash["ts"],
        v: hash["v"]
      )
    end

    def initialize(name:, data: {}, id: nil, user: nil, ts: nil, v: nil)
      @id = id
      @name = name
      @data = data
      @user = user
      @ts = ts
      @v = v
    end

    def [](key)
      to_h[key.to_s]
    end

    def to_h
      {
        "id" => id,
        "name" => name,
        "data" => data,
        "user" => user,
        "ts" => ts,
        "v" => v
      }.compact
    end
    alias as_json to_h

    def to_json(*args)
      to_h.to_json(*args)
    end

    def ==(other)
      other.is_a?(Event) && other.to_h == to_h
    end
  end
end
