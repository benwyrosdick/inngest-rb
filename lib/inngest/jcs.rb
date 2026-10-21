# frozen_string_literal: true

require "json"

module Inngest
  # JSON Canonicalization Scheme (RFC 8785). Inngest signs the canonical form of
  # request bodies, so verifying a signature means canonicalizing first.
  module JCS
    ESCAPES = { '"' => '\\"', "\\" => "\\\\", "\b" => "\\b", "\f" => "\\f", "\n" => "\\n", "\r" => "\\r",
                "\t" => "\\t" }.freeze

    module_function

    def canonicalize(json)
      serialize(JSON.parse(json))
    end

    def serialize(value)
      case value
      when Hash
        # Keys are sorted by their UTF-16 code units.
        members = value.keys.map(&:to_s).sort_by { |key| key.encode("UTF-16BE").unpack("n*") }
        "{#{members.map { |key| "#{string(key)}:#{serialize(value.fetch(key))}" }.join(",")}}"
      when Array then "[#{value.map { |item| serialize(item) }.join(",")}]"
      when String then string(value)
      when Numeric then number(value)
      when true then "true"
      when false then "false"
      when nil then "null"
      else raise ArgumentError, "can't canonicalize #{value.class}"
      end
    end

    def string(value)
      escaped = value.gsub(/["\\\u0000-\u001f]/) { |char| ESCAPES[char] || format("\\u%04x", char.ord) }
      "\"#{escaped}\""
    end

    # Formats numbers like JavaScript's Number#toString, which RFC 8785 requires.
    # Every number is treated as an IEEE 754 double.
    def number(value)
      float = value.to_f
      raise ArgumentError, "can't canonicalize #{value}" if float.nan? || float.infinite?
      return "0" if float.zero?

      # Float#to_s gives the shortest round-trip digits, e.g. "1.0e+21" or "0.001".
      mantissa, exponent = float.abs.to_s.split("e")
      whole, fraction = mantissa.split(".")
      digits = whole + fraction.to_s
      point = whole.length + exponent.to_i # Position of the decimal point in `digits`.

      leading = digits[/\A0*/].length
      digits = digits[leading..].sub(/0+\z/, "")
      point -= leading
      count = digits.length

      formatted =
        if point.between?(count, 21)
          digits + ("0" * (point - count))
        elsif point.positive? && point <= 21
          "#{digits[0, point]}.#{digits[point..]}"
        elsif point > -6 && point <= 0
          "0.#{"0" * -point}#{digits}"
        else
          exp = point - 1
          exp_string = exp.negative? ? exp.to_s : "+#{exp}"
          count == 1 ? "#{digits}e#{exp_string}" : "#{digits[0]}.#{digits[1..]}e#{exp_string}"
        end

      float.negative? ? "-#{formatted}" : formatted
    end
  end
end
