# frozen_string_literal: true

require "rails/generators"

module Inngest
  module Generators
    # rails generate inngest:function NAME [--event=EVENT] [--cron=SCHEDULE]
    class FunctionGenerator < Rails::Generators::NamedBase
      source_root File.expand_path("templates", __dir__)

      desc "Creates an Inngest function in app/inngest. Without --event or --cron, it's triggered by an " \
           "event named after the function (e.g. app/send-welcome-email)."

      class_option :event, type: :string, desc: %(Event that triggers the function, e.g. "user/signed_up")
      class_option :cron, type: :string, desc: %(Cron schedule that triggers the function, e.g. "0 9 * * *")
      class_option :test_framework, type: :string, desc: "rspec or test_unit (default: the app's test framework)"
      class_option :skip_test, type: :boolean, default: false, desc: "Don't create a test"

      def create_function
        template "function.rb.tt", File.join("app/inngest", class_path, "#{file_name}.rb")
      end

      def create_test
        return if options[:skip_test]

        case test_framework
        when :rspec
          template "function_spec.rb.tt", File.join("spec/inngest", class_path, "#{file_name}_spec.rb")
        when :test_unit
          template "function_test.rb.tt", File.join("test/inngest", class_path, "#{file_name}_test.rb")
        end
      end

      private

      def test_framework
        framework = options[:test_framework]
        if framework.nil? && defined?(::Rails.application) && ::Rails.application
          framework = ::Rails.application.config.generators.options.dig(:rails, :test_framework)
        end
        framework ||= :rspec if Dir.exist?(File.join(destination_root, "spec"))
        framework ||= :test_unit if Dir.exist?(File.join(destination_root, "test"))
        framework&.to_sym
      end

      def spec_helper
        File.exist?(File.join(destination_root, "spec/rails_helper.rb")) ? "rails_helper" : "spec_helper"
      end

      # The event the generated test sends.
      def test_event
        return options[:event] if options[:event]

        options[:cron] ? "inngest/scheduled.timer" : default_event
      end

      def triggers
        lines = []
        lines << "trigger event: #{options[:event].inspect}" if options[:event]
        lines << "trigger cron: #{options[:cron].inspect}" if options[:cron]
        lines << "trigger event: #{default_event.inspect}" if lines.empty?
        lines
      end

      def default_event
        namespace = class_path.empty? ? "app" : class_path.join("-").dasherize
        "#{namespace}/#{file_name.dasherize}"
      end

      # Indents generated code to sit inside the class's modules.
      def nested(code)
        code.gsub(/^(?!$)/, "  " * class_path.size)
      end
    end
  end
end
