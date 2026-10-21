# frozen_string_literal: true

require "time"

module Inngest
  # Converts Ruby durations and times into the formats the Inngest API expects.
  module Duration
    UNITS = [
      [7 * 24 * 60 * 60 * 1000, "w"],
      [24 * 60 * 60 * 1000, "d"],
      [60 * 60 * 1000, "h"],
      [60 * 1000, "m"],
      [1000, "s"],
      [1, "ms"]
    ].freeze

    module_function

    # Converts a duration to a "time string" such as "30s" or "2d".
    #
    # Accepts a time string (returned as-is), a number of seconds, or anything
    # that responds to #to_f with seconds (e.g. ActiveSupport::Duration).
    def to_time_string(value)
      return value if value.is_a?(String)
      raise ArgumentError, "invalid duration: #{value.inspect}" unless value.respond_to?(:to_f)

      ms = (value.to_f * 1000).round
      raise ArgumentError, "duration must be positive: #{value.inspect}" unless ms.positive?

      size, suffix = UNITS.find { |unit_ms, _| (ms % unit_ms).zero? }
      "#{ms / size}#{suffix}"
    end

    # Converts a Time-like value to an RFC 3339 UTC timestamp with millisecond
    # precision. Strings are returned as-is.
    def to_iso8601(value)
      return value if value.is_a?(String)
      raise ArgumentError, "invalid time: #{value.inspect}" unless value.respond_to?(:to_time)

      value.to_time.utc.iso8601(3)
    end
  end
end
