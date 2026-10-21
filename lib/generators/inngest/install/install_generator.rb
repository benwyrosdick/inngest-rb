# frozen_string_literal: true

require "rails/generators"

module Inngest
  module Generators
    # rails generate inngest:install [--mode=connect|serve]
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      desc "Sets up Inngest: an initializer, app/inngest for functions, and either a Connect worker " \
           "(the default) or an HTTP endpoint."

      class_option :mode, type: :string, default: "connect", enum: %w[connect serve],
                          desc: "connect: run functions on a worker process; serve: Inngest calls a mounted endpoint"

      def create_initializer
        template "inngest.rb.tt", "config/initializers/inngest.rb"
      end

      def create_functions_directory
        create_file "app/inngest/.keep", ""
      end

      def mount_endpoint
        route %(mount Inngest::Serve.new => "/api/inngest") if serve?
      end

      def add_procfile_entries
        # Production: run the worker next to the web process.
        append_line("Procfile", "inngest: bundle exec inngest-connect") if connect?

        # Development (bin/dev): the worker and the dev server.
        append_line("Procfile.dev", "inngest: bundle exec inngest-connect") if connect?
        append_line("Procfile.dev", "inngest-dev: npx --yes inngest-cli@latest dev #{dev_server_args}")
      end

      def show_next_steps
        say ""
        say "Inngest is installed. Next:", :green
        say "  1. Create a function: bin/rails generate inngest:function SendWelcomeEmail --event user/signed_up"
        if File.exist?(File.join(destination_root, "Procfile.dev"))
          say "  2. Run bin/dev; the dev server UI is at http://localhost:8288"
        else
          say "  2. Start the dev server: npx inngest-cli@latest dev #{dev_server_args}"
          say connect? ? "     Run functions: bundle exec inngest-connect" : "     Start your app: bin/rails server"
        end
        say "  3. In production, set INNGEST_SIGNING_KEY and INNGEST_EVENT_KEY#{production_note}"
      end

      private

      def connect?
        options[:mode] == "connect"
      end

      def serve?
        options[:mode] == "serve"
      end

      # Stable app ID used in the initializer. Changing it later creates a new app
      # in Inngest, so it's written out rather than derived at runtime.
      def app_id
        name = defined?(::Rails.application) && ::Rails.application ? ::Rails.application.class.module_parent_name : nil
        name ||= File.basename(destination_root)
        name.to_s.gsub(/([a-z\d])([A-Z])/, '\1-\2').tr("_", "-").downcase
      end

      def dev_server_args
        connect? ? "--no-discovery" : "-u http://localhost:3000/api/inngest --no-discovery"
      end

      def production_note
        if connect?
          ", and run bundle exec inngest-connect alongside your web process"
        else
          ", and sync after each deploy: curl -X PUT https://<your-app>/api/inngest"
        end
      end

      def append_line(file, line)
        path = File.join(destination_root, file)
        return unless File.exist?(path)
        return if File.read(path).lines.any? { |existing| existing.strip == line }

        content = File.read(path).end_with?("\n") || File.empty?(path) ? "#{line}\n" : "\n#{line}\n"
        append_to_file file, content
      end
    end
  end
end
