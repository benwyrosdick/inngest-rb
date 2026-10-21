# frozen_string_literal: true

module Inngest
  # Rails defaults: the app ID comes from the application's name. Override it
  # with Inngest.configure in an initializer.
  class Railtie < Rails::Railtie
    initializer "inngest.defaults", before: :load_config_initializers do |app|
      app_name = app.class.module_parent_name.gsub(/([a-z\d])([A-Z])/, '\1-\2').downcase
      Inngest.default_client_options[:app_id] ||= app_name
    end

    # Functions are found by subclass, so they must be loaded. Without eager
    # loading (e.g. in development), load app/inngest on demand.
    initializer "inngest.function_loader" do |app|
      Inngest.function_loader ||= lambda do
        next if app.config.eager_load

        dir = app.root.join("app/inngest").to_s
        Rails.autoloaders.main.eager_load_dir(dir) if Dir.exist?(dir)
      end
    end
  end
end
