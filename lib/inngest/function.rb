# frozen_string_literal: true

require "uri"

module Inngest
  # Base class for Inngest functions.
  #
  #   class SendWelcomeEmail < Inngest::Function
  #     trigger event: "user/signed_up"
  #     concurrency limit: 10, key: "event.data.account_id"
  #     retries 3
  #
  #     def call
  #       user = step.run("load-user") { User.find(event.data["user_id"]).slice("id", "email") }
  #       step.sleep("wait-a-day", 1.day)
  #       step.run("send-email") { WelcomeMailer.welcome(user["email"]).deliver_now }
  #     end
  #   end
  #
  # Inside #call (and #on_failure), `event`, `events`, `step`, `run_id`,
  # `attempt` and `logger` are available. #call may also declare any of them
  # as keyword arguments instead: `def call(event:, step:)`.
  #
  # Code outside of steps runs again every time Inngest calls the function, so
  # it must be deterministic. Put side effects and anything non-deterministic
  # (time, randomness, database reads) in steps.
  class Function
    FAILURE_EVENT = "inngest/function.failed"

    # Keywords that #call / #on_failure may declare.
    CONTEXT_KEYWORDS = %i[event events step run_id attempt logger].freeze

    class << self
      # The function ID, unique within the app. Defaults to the dasherized class
      # name (Billing::SendInvoice => "billing-send-invoice"). Changing it
      # creates a new function in Inngest.
      def id(value = nil)
        if value
          @id = value.to_s
        else
          @id || default_id
        end
      end

      # Human-readable name shown in the Inngest dashboard. Defaults to the ID.
      def display_name(value = nil)
        value ? (@display_name = value.to_s) : (@display_name || id)
      end

      # Adds an event or cron trigger. A function can have several.
      #
      #   trigger event: "user/signed_up"
      #   trigger event: "order/created", if: "event.data.total > 100"
      #   trigger cron: "TZ=Europe/Paris 0 9 * * 1-5"
      def trigger(event: nil, cron: nil, **opts)
        raise ArgumentError, "trigger needs exactly one of event: or cron:" if event.nil? == cron.nil?

        triggers << if event
                      { "event" => event.to_s, "expression" => opts[:if] || opts[:expression] }.compact
                    else
                      { "cron" => cron.to_s, "jitter" => opts[:jitter] }.compact
                    end
      end

      def triggers
        @triggers ||= []
      end

      # Number of retries per step (and for the function itself). The default
      # is 3 retries (4 attempts).
      def retries(count)
        options["retries"] = Integer(count)
      end

      # Limits concurrently running steps. Can be called up to twice to combine
      # limits (e.g. per account and per env).
      def concurrency(limit:, key: nil, scope: nil)
        (options["concurrency"] ||= []) << { "limit" => limit, "key" => key, "scope" => scope&.to_s }.compact
      end

      def throttle(limit:, period:, key: nil, burst: nil)
        options["throttle"] = { "limit" => limit, "period" => Duration.to_time_string(period), "key" => key,
                                "burst" => burst }.compact
      end

      def rate_limit(limit:, period:, key: nil)
        options["rateLimit"] = { "limit" => limit, "period" => Duration.to_time_string(period), "key" => key }.compact
      end

      def debounce(period:, key: nil, timeout: nil)
        options["debounce"] = { "period" => Duration.to_time_string(period), "key" => key,
                                "timeout" => timeout && Duration.to_time_string(timeout) }.compact
      end

      def batch_events(max_size:, timeout:, key: nil, **opts)
        options["batchEvents"] = { "maxSize" => max_size, "timeout" => Duration.to_time_string(timeout), "key" => key,
                                   "if" => opts[:if] }.compact
      end

      # Cancels running runs when a matching event arrives.
      #
      #   cancel_on event: "order/cancelled", if: "event.data.order_id == async.data.order_id"
      def cancel_on(event:, timeout: nil, **opts)
        (options["cancel"] ||= []) << { "event" => event.to_s, "if" => opts[:if],
                                        "timeout" => timeout && Duration.to_time_string(timeout) }.compact
      end

      def priority(run:)
        options["priority"] = { "run" => run }
      end

      def idempotency(expression)
        options["idempotency"] = expression.to_s
      end

      def timeouts(start: nil, finish: nil)
        options["timeouts"] = { "start" => start && Duration.to_time_string(start),
                                "finish" => finish && Duration.to_time_string(finish) }.compact
      end

      def singleton(mode:, key: nil)
        options["singleton"] = { "mode" => mode.to_s, "key" => key }.compact
      end

      def options
        @options ||= {}
      end

      # The ID Inngest uses for this function: "<app id>-<function id>".
      def fully_qualified_id(app_id)
        "#{app_id}-#{id}"
      end

      def failure_function_id(app_id)
        "#{fully_qualified_id(app_id)}-failure"
      end

      def on_failure?
        method_defined?(:on_failure)
      end

      # All loaded subclasses that have at least one trigger.
      def registered
        Function.all_subclasses.select { |klass| klass.triggers.any? && current?(klass) }
      end

      def all_subclasses
        subclasses.flat_map { |klass| [klass, *klass.all_subclasses] }
      end

      # Function configuration(s) as sent to Inngest when syncing: the function
      # itself plus its failure handler, if any.
      def to_configs(app_id:, url:)
        raise ConfigError, "#{self} has no triggers" if triggers.empty?

        fqid = fully_qualified_id(app_id)
        config = {
          "id" => fqid,
          "name" => display_name,
          "triggers" => triggers,
          "steps" => root_step(fqid, url, options["retries"])
        }
        options.each { |key, value| config[key] = value unless key == "retries" }

        configs = [config]
        if on_failure?
          failure_id = failure_function_id(app_id)
          configs << {
            "id" => failure_id,
            "name" => "#{display_name} (failure)",
            "triggers" => [{ "event" => FAILURE_EVENT, "expression" => "event.data.function_id == '#{fqid}'" }],
            "steps" => root_step(failure_id, url, 0)
          }
        end
        configs
      end

      private

      def inherited(subclass)
        super
        # Let subclasses inherit configuration from abstract base classes.
        subclass.instance_variable_set(:@options, Marshal.load(Marshal.dump(options)))
        subclass.instance_variable_set(:@triggers, triggers.map(&:dup))
      end

      # After a code reload, replaced classes linger until garbage collected.
      def current?(klass)
        klass.name.nil? || (Object.const_defined?(klass.name) && Object.const_get(klass.name).equal?(klass))
      rescue NameError
        false
      end

      def default_id
        raise ConfigError, "anonymous Inngest functions must set an id" if name.nil?

        name.gsub("::", "-")
            .gsub(/([A-Z]+)([A-Z][a-z])/, '\1-\2')
            .gsub(/([a-z\d])([A-Z])/, '\1-\2')
            .downcase
      end

      def root_step(function_id, url, retries)
        separator = url.include?("?") ? "&" : "?"
        runtime_url = "#{url}#{separator}#{URI.encode_www_form(fnId: function_id, stepId: "step")}"
        type = url.start_with?("ws") ? "ws" : "http"
        step = { "id" => "step", "name" => "step", "runtime" => { "type" => type, "url" => runtime_url } }
        step["retries"] = { "attempts" => retries } if retries
        { "step" => step }
      end
    end

    attr_reader :event, :events, :step, :run_id, :attempt, :logger

    def initialize(event:, events:, step:, run_id:, attempt:, logger:)
      @event = event
      @events = events
      @step = step
      @run_id = run_id
      @attempt = attempt
      @logger = logger
    end

    def call
      raise NotImplementedError, "#{self.class} must implement #call"
    end

    # In #on_failure, the error that made the function fail.
    def error
      event&.data&.dig("error")
    end

    # Runs #call or #on_failure, passing whichever context keywords it declares.
    def inngest_dispatch(method_name)
      method = method(method_name)
      params = method.parameters
      return public_send(method_name) if params.empty?

      accepts_all = params.any? { |type, _| type == :keyrest }
      declared = params.filter_map { |type, name| name if %i[key keyreq].include?(type) }
      keys = accepts_all ? CONTEXT_KEYWORDS : CONTEXT_KEYWORDS & declared
      public_send(method_name, **keys.to_h { |key| [key, public_send(key)] })
    end
  end
end
