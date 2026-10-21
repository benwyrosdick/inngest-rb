# frozen_string_literal: true

module Inngest
  # Base class for all errors raised by the SDK.
  class Error < StandardError; end

  # Raised when the SDK is misconfigured (missing keys, invalid options, etc.).
  class ConfigError < Error; end

  # Raised by a function (or a step) to stop Inngest from retrying it.
  class NonRetriableError < Error; end

  # Raised by a function (or a step) to retry it no earlier than the given
  # time. `retry_after` may be a Time or a number of seconds from now.
  class RetryAfterError < Error
    attr_reader :retry_after

    def initialize(message = nil, retry_after:)
      super(message)
      @retry_after = retry_after.is_a?(Numeric) ? Time.now + retry_after : retry_after
    end
  end

  # Raised inside a function when a step has failed after exhausting its
  # retries. The original exception can't be rebuilt from memoized data, so this
  # carries the original error's name, message and stack.
  #
  # If left unhandled, the function fails without being retried.
  class StepError < Error
    attr_reader :step_id, :error_name, :error_stack

    def initialize(step_id:, name:, message:, stack: nil)
      super(message)
      @step_id = step_id
      @error_name = name
      @error_stack = stack
    end
  end

  # Raised when a step is used inside another step's block.
  class NestedStepError < NonRetriableError; end

  # Raised when the function hits a step that doesn't match what Inngest asked
  # to run. This usually means the function isn't deterministic.
  class StepUnexpectedError < NonRetriableError; end

  # Raised when sending events fails.
  class SendEventError < Error
    attr_reader :ids

    def initialize(message, ids: [])
      super(message)
      @ids = ids
    end
  end

  # Raised for non-2xx responses from the Inngest API.
  class APIError < Error
    attr_reader :status, :body

    def initialize(message, status: nil, body: nil)
      super(message)
      @status = status
      @body = body
    end
  end
end
