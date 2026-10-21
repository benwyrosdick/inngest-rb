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

      def create_function
        template "function.rb.tt", File.join("app/inngest", class_path, "#{file_name}.rb")
      end

      private

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
