# frozen_string_literal: true

module Inngest
  VERSION = "0.1.0"

  # Short language identifier, matching the convention of the other SDKs
  # ("js", "go", "py").
  LANGUAGE = "rb"

  # Value of the X-Inngest-SDK header and the sdk_version Connect field.
  SDK_IDENTIFIER = "inngest-#{LANGUAGE}:v#{VERSION}".freeze
end
