# frozen_string_literal: true

require "optparse"

module Inngest
  # Command-line entry point for `inngest-connect`, which boots the app and
  # runs a Connect worker:
  #
  #   bundle exec inngest-connect --concurrency 10
  class CLI
    def initialize(argv)
      @argv = argv
      @options = {}
    end

    def run
      $stdout.sync = true
      parse_options
      boot_app
      require "inngest/connect"

      functions = Inngest.functions
      raise ConfigError, "no Inngest functions found (subclass Inngest::Function and add a trigger)" if functions.empty?

      Connect::Worker.new(
        client: Inngest.client,
        functions: functions,
        instance_id: @options[:instance_id],
        max_worker_concurrency: concurrency,
        shutdown_timeout: @options.fetch(:shutdown_timeout, 25),
        execution_wrapper: rails_execution_wrapper
      ).run
    end

    private

    def parse_options
      OptionParser.new do |opts|
        opts.banner = "Usage: inngest-connect [options]"
        opts.on("-r", "--require PATH", "File to load (default: config/environment.rb if present)") do |path|
          @options[:require] = path
        end
        opts.on("-c", "--concurrency N", Integer,
                "Functions to run at once (default: the database pool size, or 10)") do |n|
          @options[:concurrency] = n
        end
        opts.on("-i", "--instance-id ID", "Stable worker ID (default: hostname)") do |id|
          @options[:instance_id] = id
        end
        opts.on("-t", "--shutdown-timeout SECONDS", Float, "Seconds to wait for running steps (default: 25)") do |s|
          @options[:shutdown_timeout] = s
        end
        opts.on("-v", "--version", "Print the version") do
          puts "inngest #{VERSION}"
          exit
        end
      end.parse!(@argv)
    end

    def boot_app
      path = @options[:require] || ("config/environment.rb" if File.exist?("config/environment.rb"))
      raise ConfigError, "nothing to load: pass --require with the file that defines your functions" unless path

      if path.end_with?("config/environment.rb")
        # Load the Railtie before the app initializes so it can set defaults.
        require "rails"
        require "inngest/railtie"
      end
      require File.expand_path(path)
      # Functions are found by subclass, so they all need to be loaded.
      Rails.application.eager_load! if defined?(Rails.application) && Rails.application
    end

    # Each execution runs inside the Rails reloader, like a Sidekiq job: it
    # returns database connections to the pool and picks up code changes in
    # development.
    def rails_execution_wrapper
      return unless defined?(Rails.application) && Rails.application

      ->(&block) { Rails.application.reloader.wrap(&block) }
    end

    # Each running function may hold a database connection, so without an
    # explicit setting, concurrency defaults to the Active Record pool size.
    def concurrency
      explicit = @options[:concurrency] || ENV["INNGEST_CONNECT_MAX_WORKER_CONCURRENCY"]&.then { |v| Integer(v) }
      pool = database_pool_size
      return explicit || pool unless explicit && pool && explicit > pool

      Inngest.client.logger.warn(
        "[inngest] concurrency (#{explicit}) is larger than the database pool (#{pool}); " \
        "raise the pool size in config/database.yml or lower --concurrency"
      )
      explicit
    end

    def database_pool_size
      return unless defined?(ActiveRecord::Base)

      ActiveRecord::Base.connection_pool.size
    rescue StandardError
      nil
    end
  end
end
