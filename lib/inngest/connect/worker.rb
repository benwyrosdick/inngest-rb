# frozen_string_literal: true

require "etc"
require "json"
require "rbconfig"
require "socket"

module Inngest
  module Connect
    # A long-running worker that runs Inngest functions over an outbound
    # WebSocket connection, similar to a Sidekiq process.
    #
    #   client = Inngest::Client.new(app_id: "my-app")
    #   Inngest::Connect::Worker.new(client: client, functions: [SendWelcomeEmail]).run
    #
    # #run blocks until the process receives SIGINT/SIGTERM (or #stop is
    # called), then shuts down gracefully: it stops taking new work, waits for
    # running steps to finish and delivers their results.
    #
    # Threads:
    # - the caller's thread manages the connection (connect, reconnect, drain)
    # - a reader thread per connection parses gateway messages
    # - a ticker thread sends heartbeats, lease extensions and status updates,
    #   and flushes unacknowledged replies over HTTP
    # - `max_worker_concurrency` pool threads run functions
    class Worker
      # Seconds to wait between reconnect attempts.
      BACKOFF = [1, 2, 5, 10, 20, 30, 60, 120, 300].freeze
      HANDSHAKE_TIMEOUT = 20
      # The worker counts as dead if its ticker hasn't run for this long.
      LIVENESS_TIMEOUT = 60
      DEFAULT_INTERVAL = 10
      MAX_FLUSH_ATTEMPTS = 5
      REQUEST_VERSION = 2

      # Errors that should stop the worker instead of reconnecting.
      class FatalError < Inngest::Error; end

      InFlight = Struct.new(:request, :lease_id, :extend_lease, :extend_at)

      attr_reader :client, :instance_id, :max_worker_concurrency

      # @param functions [Array<Class>] Inngest::Function subclasses to serve
      # @param instance_id [String] stable ID for this worker (defaults to the
      #   hostname, which is usually the pod or container name)
      # @param max_worker_concurrency [Integer] how many functions this worker
      #   runs at once; Inngest won't send more than this
      # @param shutdown_timeout [Numeric] seconds to wait for running steps on
      #   shutdown; unfinished steps are retried by Inngest
      # @param execution_wrapper [#call] wraps each execution, e.g. with
      #   Rails.application.executor; called with a block
      # @param rewrite_gateway_endpoint [#call] rewrites the gateway URL (e.g.
      #   for a proxy or self-hosted gateway)
      # @param health_port [Integer, nil] serve /ready and /live on this port
      #   for Kubernetes probes; disabled when nil
      # @param health_host [String] address for the health check server
      def initialize(
        client:,
        functions: Function.registered,
        instance_id: nil,
        max_worker_concurrency: nil,
        shutdown_timeout: 25,
        shutdown_signals: %w[INT TERM],
        execution_wrapper: nil,
        rewrite_gateway_endpoint: nil,
        health_port: ENV.fetch("INNGEST_CONNECT_HEALTH_PORT", nil),
        health_host: ENV.fetch("INNGEST_CONNECT_HEALTH_HOST", "0.0.0.0")
      )
        @client = client
        @logger = client.logger
        @instance_id = instance_id || ENV["INNGEST_CONNECT_INSTANCE_ID"] || Socket.gethostname
        @max_worker_concurrency = Integer(max_worker_concurrency ||
                                          ENV.fetch("INNGEST_CONNECT_MAX_WORKER_CONCURRENCY", 10))
        raise ConfigError, "max_worker_concurrency must be positive" unless @max_worker_concurrency.positive?

        @shutdown_timeout = shutdown_timeout
        @shutdown_signals = shutdown_signals
        @execution_wrapper = execution_wrapper
        @rewrite_gateway_endpoint = rewrite_gateway_endpoint
        @function_classes = functions
        @functions = index_functions(functions)
        if health_port && !health_port.to_s.empty?
          @health_server = HealthServer.new(self, port: health_port, host: health_host, logger: @logger)
        end

        @state = :init
        @events = Queue.new
        @jobs = Queue.new
        @in_flight = {}
        @in_flight_lock = Mutex.new
        @buffer = ReplyBuffer.new(logger: @logger)
        @flush_attempts = Hash.new(0)
        @active = nil
        @stopping = false
        @ever_connected = false
      end

      # Connects and runs functions until shutdown. Blocks.
      def run
        raise ConfigError, "worker has already run" unless @state == :init
        if !client.dev? && client.signing_key.nil?
          raise ConfigError, "a signing key is required to connect to Inngest Cloud (set INNGEST_SIGNING_KEY, " \
                             "or INNGEST_DEV=1 for the dev server)"
        end

        @state = :connecting
        @logger.info("[inngest] starting connect worker #{instance_id} for app \"#{client.app_id}\" " \
                     "(#{@function_classes.size} #{@function_classes.size == 1 ? "function" : "functions"}, " \
                     "concurrency #{max_worker_concurrency}, " \
                     "#{client.dev? ? "dev server" : "cloud"} #{client.api_origin})")
        install_signal_handlers
        start_threads
        @health_server&.start
        connect_loop
      ensure
        @health_server&.stop
        stop_threads
        restore_signal_handlers
        @state = :closed
        @logger.info("[inngest] connect worker stopped")
      end

      # Starts a graceful shutdown. Safe to call from any thread.
      def stop
        @events << [:shutdown]
      end

      # :init, :connecting, :active, :reconnecting, :closing or :closed.
      def state
        # While a draining gateway is being replaced, the worker stays active
        # only as long as the old connection is still open.
        return :reconnecting if @state == :active && !@active&.ready?

        @state
      end

      # True when connected and accepting work. Suitable for a readiness probe.
      def ready?
        state == :active
      end

      # True while the worker is running and its background work hasn't
      # stalled. Suitable for a liveness probe: if this is false, restart the
      # process.
      def alive?
        return false if %i[init closed].include?(@state)

        !!@ticker&.alive? && monotonic_now - @last_tick_at < LIVENESS_TIMEOUT
      end

      # The port of the health check server, if it's running.
      def health_port
        @health_server&.port
      end

      # Worker status, as reported by the health check server.
      def health
        {
          "status" => state.to_s,
          "ready" => ready?,
          "alive" => alive?,
          "app_id" => client.app_id,
          "instance_id" => instance_id,
          "connection_id" => @active&.connection_id,
          "in_flight" => in_flight_count,
          "max_worker_concurrency" => max_worker_concurrency
        }
      end

      private

      # --- Connection management (caller's thread) ---

      def connect_loop
        failures = 0
        exclude_gateways = []

        until @stopping
          begin
            establish(exclude_gateways)
            failures = 0
            exclude_gateways = []
          rescue FatalError
            raise
          rescue StandardError => e
            delay = BACKOFF[[failures, BACKOFF.size - 1].min]
            failures += 1
            @state = :reconnecting if @ever_connected && !@active&.ready?
            @logger.error("[inngest] connection failed: #{e.message}; retrying in #{delay}s")
            break if wait_for_shutdown(delay)

            next
          end

          exclude_gateways = wait_for_reconnect
        end

        graceful_shutdown
      end

      # Waits until the active connection needs replacing. Returns the gateway
      # groups to avoid when reconnecting.
      def wait_for_reconnect
        loop do
          kind, conn, reason = @events.pop
          case kind
          when :shutdown
            @stopping = true
            return []
          when :closed
            next unless conn.equal?(@active)

            @logger.warn("[inngest] connection lost (#{reason}); reconnecting")
            @active = nil
            @state = :reconnecting
            flush_buffer(all: true)
            return []
          when :draining
            next unless conn.equal?(@active)

            # The gateway is shutting down. Connect elsewhere, then close this
            # connection once the new one is ready.
            @logger.info("[inngest] gateway is draining; moving to another gateway")
            return [conn.gateway_group]
          end
        end
      end

      def wait_for_shutdown(seconds)
        deadline = monotonic_now + seconds
        loop do
          remaining = deadline - monotonic_now
          return false if remaining <= 0

          kind, = @events.pop(timeout: remaining)
          if kind == :shutdown
            @stopping = true
            return true
          end
        end
      end

      def establish(exclude_gateways)
        start = start_request(exclude_gateways)
        endpoint = start.gateway_endpoint
        endpoint = @rewrite_gateway_endpoint.call(endpoint) if @rewrite_gateway_endpoint
        raise Error, "invalid gateway endpoint #{endpoint.inspect}" unless endpoint.match?(%r{\Awss?://\S+})

        conn = Connection.new(
          url: endpoint,
          gateway_group: start.gateway_group,
          connection_id: start.connection_id,
          auth_data: Proto::AuthData.new(session_token: start.session_token, sync_token: start.sync_token),
          logger: @logger,
          on_message: method(:handle_message),
          on_close: ->(c, reason) { @events << [:closed, c, reason] }
        )
        conn.open

        unless conn.handshake.pop(timeout: HANDSHAKE_TIMEOUT) == :ready
          conn.close("handshake failed")
          raise Error, "handshake with gateway #{endpoint} failed"
        end

        previous = @active
        @active = conn
        @ever_connected = true
        @state = :active unless @stopping
        @logger.info("[inngest] connected to #{endpoint} (connection #{conn.connection_id})")
        previous&.close("GATEWAY_DRAINING")
      end

      def start_request(exclude_gateways)
        body = Proto::StartRequest.encode(Proto::StartRequest.new(exclude_gateways: exclude_gateways))
        headers = client.default_headers.merge("Content-Type" => "application/protobuf")
        response = api_request(:post, "/v0/connect/start", body: body, headers: headers)

        case response.status
        when 200 then Proto::StartResponse.decode(response.body)
        when 401, 403
          raise FatalError, "Inngest rejected the signing key (#{response.status}); check INNGEST_SIGNING_KEY"
        when 429
          message = "Inngest refused the connection (429): too many connected workers"
          raise(@ever_connected ? Error : FatalError, message)
        else
          raise Error, "connect start request failed (#{response.status}): #{response.body.to_s[0, 200]}"
        end
      end

      def graceful_shutdown
        @state = :closing
        @logger.info("[inngest] shutting down; waiting up to #{@shutdown_timeout}s for running steps")
        send_message(:WORKER_PAUSE)

        deadline = monotonic_now + @shutdown_timeout
        sleep 0.1 while in_flight_count.positive? && monotonic_now < deadline
        if (count = in_flight_count).positive?
          @logger.warn("[inngest] #{count} steps still running at shutdown; Inngest will retry them")
        end

        # Give the gateway a moment to acknowledge the last replies, then flush
        # whatever is left over HTTP.
        ack_deadline = monotonic_now + 2
        sleep 0.05 while !@buffer.empty? && monotonic_now < ack_deadline
        flush_buffer(all: true)

        @active&.close("WORKER_SHUTDOWN")
        @active = nil
      end

      # --- Gateway messages (reader thread) ---

      def handle_message(conn, message)
        case message.kind
        when :GATEWAY_HELLO
          conn.send_message(:WORKER_CONNECT, worker_connect_data(conn))
        when :GATEWAY_CONNECTION_READY
          ready = Proto::GatewayConnectionReadyData.decode(message.payload)
          conn.heartbeat_interval = parse_interval(ready.heartbeat_interval) || DEFAULT_INTERVAL
          conn.extend_lease_interval = parse_interval(ready.extend_lease_interval) || DEFAULT_INTERVAL
          # No (or a zero) status interval means the gateway doesn't want status updates.
          conn.status_interval = parse_interval(ready.status_interval)
          @logger.debug("[inngest] gateway ready: heartbeat #{ready.heartbeat_interval}, " \
                        "lease extension #{ready.extend_lease_interval}, status #{ready.status_interval}")
          conn.ready!
          conn.handshake << :ready
        when :GATEWAY_EXECUTOR_REQUEST
          handle_executor_request(conn, Proto::GatewayExecutorRequestData.decode(message.payload))
        when :WORKER_REQUEST_EXTEND_LEASE_ACK
          handle_lease_ack(Proto::WorkerRequestExtendLeaseAckData.decode(message.payload))
        when :WORKER_REPLY_ACK
          ack = Proto::WorkerReplyAckData.decode(message.payload)
          @buffer.delete(ack.request_id)
          @flush_attempts.delete(ack.request_id)
        when :GATEWAY_CLOSING
          @events << [:draining, conn]
        when :SYNC_FAILED
          @logger.error("[inngest] gateway failed to sync functions: #{sync_error(message.payload)}")
          conn.handshake << :failed
        when :GATEWAY_HEARTBEAT
          nil # Connection#last_message_at is already updated.
        else
          @logger.debug("[inngest] ignoring gateway message #{message.kind}")
        end
      end

      def handle_executor_request(conn, request)
        if @stopping
          @logger.warn("[inngest] ignoring request #{request.request_id} while shutting down")
          return
        end

        ack = Proto::WorkerRequestAckData.new(
          request_id: request.request_id,
          account_id: request.account_id,
          env_id: request.env_id,
          app_id: request.app_id,
          function_slug: request.function_slug,
          system_trace_ctx: request.system_trace_ctx,
          user_trace_ctx: request.user_trace_ctx,
          run_id: request.run_id
        )
        ack.step_id = request.step_id if request.has_step_id?
        conn.send_message(:WORKER_REQUEST_ACK, ack)

        @in_flight_lock.synchronize do
          extend_at = monotonic_now + (conn.extend_lease_interval || DEFAULT_INTERVAL)
          @in_flight[request.request_id] = InFlight.new(request, request.lease_id, true, extend_at)
        end
        @jobs << request
      end

      def handle_lease_ack(ack)
        @in_flight_lock.synchronize do
          entry = @in_flight[ack.request_id]
          next unless entry

          if ack.has_new_lease_id? && !ack.new_lease_id.empty?
            entry.lease_id = ack.new_lease_id
          else
            # The lease expired or another worker took over. Let the function
            # finish, but stop extending.
            entry.extend_lease = false
          end
        end
      end

      def worker_connect_data(conn)
        configs = @function_classes.flat_map { |fn| fn.to_configs(app_id: client.app_id, url: "wss://connect") }
        app = Proto::AppConfiguration.new(app_name: client.app_id, functions: JSON.generate(configs))
        app.app_version = client.app_version if client.app_version

        data = Proto::WorkerConnectRequestData.new(
          connection_id: conn.connection_id,
          instance_id: instance_id,
          auth_data: conn.auth_data,
          capabilities: JSON.generate({ "connect" => "v1", "in_band_sync" => "v1", "trust_probe" => "v1" }),
          apps: [app],
          worker_manual_readiness_ack: false,
          system_attributes: Proto::SystemAttributes.new(cpu_cores: Etc.nprocessors, mem_bytes: total_memory,
                                                         os: os_name),
          framework: "connect",
          sdk_version: VERSION,
          sdk_language: LANGUAGE,
          started_at: Google::Protobuf::Timestamp.new(seconds: @started_at.to_i, nanos: @started_at.nsec),
          max_worker_concurrency: max_worker_concurrency
        )
        data.environment = client.env if client.env
        data
      end

      # --- Executions (pool threads) ---

      def process(request)
        result = execute(request)
        reply = Proto::SDKResponse.new(
          request_id: request.request_id,
          account_id: request.account_id,
          env_id: request.env_id,
          app_id: request.app_id,
          status: { 200 => :DONE, 206 => :NOT_COMPLETED }.fetch(result.status, :ERROR),
          body: result.body.b,
          no_retry: result.no_retry,
          sdk_version: SDK_IDENTIFIER,
          request_version: REQUEST_VERSION,
          system_trace_ctx: request.system_trace_ctx,
          user_trace_ctx: request.user_trace_ctx,
          run_id: request.run_id
        )
        reply.retry_after = result.retry_after if result.retry_after
        bytes = Proto::SDKResponse.encode(reply)

        # Keep the reply until the gateway acknowledges it.
        @buffer.add(request.request_id, bytes)
        send_message(:WORKER_REPLY, reply)
      ensure
        @in_flight_lock.synchronize { @in_flight.delete(request.request_id) }
      end

      def execute(request)
        function_class, failure = @functions[request.function_slug]
        unless function_class
          return error_result("FunctionNotFoundError",
                              "function \"#{request.function_slug}\" isn't served by this worker")
        end

        payload = JSON.parse(request.request_payload)
        step_id = request.has_step_id? ? request.step_id : nil
        wrap_execution do
          # Look the class up again so code reloading (e.g. in Rails development)
          # takes effect.
          function_class = Object.const_get(function_class.name) if function_class.name
          Executor.new(client: client, function_class: function_class, payload: payload, step_id: step_id,
                       failure: failure, request: request).call
        end
      rescue StandardError => e
        @logger.error("[inngest] failed to run #{request.function_slug}: #{e.class}: #{e.message}")
        error_result(e.class.name, e.message)
      end

      def wrap_execution(&)
        @execution_wrapper ? @execution_wrapper.call(&) : yield
      end

      def error_result(name, message)
        Executor::Result.new(status: 500, body: JSON.generate({ "name" => name, "message" => message }),
                             no_retry: false)
      end

      # --- Periodic work (ticker thread) ---

      def tick
        now = monotonic_now
        conn = @active

        if conn&.ready?
          heartbeat_interval = conn.heartbeat_interval || DEFAULT_INTERVAL
          if now - conn.last_message_at > (heartbeat_interval * 2) + 1
            @logger.warn("[inngest] gateway missed 2 heartbeats; reconnecting")
            conn.close("CONSECUTIVE_HEARTBEATS_MISSED")
            return
          end

          if now - (@last_heartbeat_at || 0) >= heartbeat_interval
            send_message(:WORKER_HEARTBEAT)
            @last_heartbeat_at = now
          end

          extend_leases(now, conn.extend_lease_interval || DEFAULT_INTERVAL)

          if conn.status_interval && now - (@last_status_at || 0) >= conn.status_interval
            send_message(:WORKER_STATUS, Proto::WorkerStatusData.new(in_flight_request_ids: in_flight_ids,
                                                                     shutdown_requested: @stopping))
            @last_status_at = now
          end
        end

        flush_buffer
      end

      # Each request's lease is extended every `interval` seconds after it
      # arrives, until it finishes or the gateway refuses an extension.
      def extend_leases(now, interval)
        entries = @in_flight_lock.synchronize do
          @in_flight.values.select { |entry| entry.extend_lease && entry.extend_at <= now }
                    .each { |entry| entry.extend_at = now + interval }
        end
        entries.each do |entry|
          request = entry.request
          data = Proto::WorkerRequestExtendLeaseData.new(
            request_id: request.request_id,
            account_id: request.account_id,
            env_id: request.env_id,
            app_id: request.app_id,
            function_slug: request.function_slug,
            system_trace_ctx: request.system_trace_ctx,
            user_trace_ctx: request.user_trace_ctx,
            run_id: request.run_id,
            lease_id: entry.lease_id
          )
          data.step_id = request.step_id if request.has_step_id?
          @logger.debug("[inngest] extending lease for request #{request.request_id}")
          send_message(:WORKER_REQUEST_EXTEND_LEASE, data)
        end
      end

      # Sends replies the gateway hasn't acknowledged over HTTP.
      def flush_buffer(all: false)
        ttl = all ? 0 : (@active&.extend_lease_interval || DEFAULT_INTERVAL)
        @buffer.older_than(ttl).each do |item|
          # Flushing many replies can take a while; it isn't a stall.
          @last_tick_at = monotonic_now
          response = api_request(:post, "/v0/connect/flush", body: item.bytes,
                                                             headers: client.default_headers.merge(
                                                               "Content-Type" => "application/protobuf"
                                                             ))
          raise Error, "status #{response.status}" unless response.success?

          @buffer.delete(item.request_id)
          @flush_attempts.delete(item.request_id)
        rescue StandardError => e
          attempts = (@flush_attempts[item.request_id] += 1)
          if attempts >= MAX_FLUSH_ATTEMPTS
            @logger.error("[inngest] giving up on delivering reply #{item.request_id}: #{e.message}")
            @buffer.delete(item.request_id)
            @flush_attempts.delete(item.request_id)
          else
            @logger.warn("[inngest] failed to flush reply #{item.request_id}: #{e.message}")
          end
        end
      end

      # --- Helpers ---

      def send_message(kind, payload = nil)
        conn = @active
        return false unless conn&.ready?

        conn.send_message(kind, payload)
      rescue StandardError => e
        @logger.warn("[inngest] failed to send #{kind}: #{e.message}")
        false
      end

      def api_request(method, path, **opts)
        url = "#{client.api_origin}#{path}"
        if client.dev?
          HTTP.request(method, url, timeout: 5, **opts)
        else
          HTTP.authed_request(method, url, signing_key: client.signing_key,
                                           signing_key_fallback: client.signing_key_fallback, timeout: 5, **opts)
        end
      end

      def start_threads
        @started_at = Time.now
        @pool = Array.new(max_worker_concurrency) do |i|
          Thread.new do
            Thread.current.name = "inngest-worker-#{i}"
            loop do
              request = @jobs.pop
              break if request.equal?(:stop)

              begin
                process(request)
              rescue StandardError => e
                @logger.error("[inngest] worker thread error: #{e.class}: #{e.message}")
              end
            end
          end
        end

        @last_tick_at = monotonic_now
        @ticker = Thread.new do
          Thread.current.name = "inngest-ticker"
          until @state == :closed || @ticker_stop
            @last_tick_at = monotonic_now
            begin
              tick
            rescue StandardError => e
              @logger.error("[inngest] ticker error: #{e.class}: #{e.message}")
            end
            sleep 1
          end
        end
      end

      def stop_threads
        @ticker_stop = true
        @ticker&.join(2)
        @pool&.each { @jobs << :stop }
        @pool&.each { |thread| thread.join(1) } # rubocop:disable Style/CombinableLoops
        @active&.close("WORKER_SHUTDOWN")
      end

      def install_signal_handlers
        @previous_traps = {}
        @signal_reader, @signal_writer = IO.pipe
        @shutdown_signals.each do |signal|
          @previous_traps[signal] = Signal.trap(signal) { @signal_writer.write_nonblock(".", exception: false) }
        end

        @signal_thread = Thread.new do
          Thread.current.name = "inngest-signals"
          IO.select([@signal_reader])
          @signal_reader.read_nonblock(64, exception: false)
          @logger.info("[inngest] received shutdown signal")
          stop
        rescue IOError, SystemCallError
          nil # The pipe was closed during shutdown.
        end
      end

      def restore_signal_handlers
        @previous_traps&.each { |signal, handler| Signal.trap(signal, handler || "DEFAULT") }
        @signal_writer&.close
        @signal_reader&.close
        @signal_thread&.join(1)
      end

      def index_functions(functions)
        raise ConfigError, "no functions to serve" if functions.empty?

        functions.each_with_object({}) do |fn, index|
          index[fn.fully_qualified_id(client.app_id)] = [fn, false]
          index[fn.failure_function_id(client.app_id)] = [fn, true] if fn.on_failure?
        end
      end

      def in_flight_count
        @in_flight_lock.synchronize { @in_flight.size }
      end

      def in_flight_ids
        @in_flight_lock.synchronize { @in_flight.keys }
      end

      def sync_error(payload)
        error = Proto::SystemError.decode(payload)
        "#{error.code}: #{error.message}"
      rescue StandardError
        payload.to_s
      end

      # Parses Go-style durations ("10s", "1m30s", "500ms") into seconds. Returns
      # nil for missing, invalid or zero durations.
      def parse_interval(value)
        units = { "ns" => 1e-9, "us" => 1e-6, "µs" => 1e-6, "ms" => 1e-3, "s" => 1, "m" => 60, "h" => 3600 }
        parts = value.to_s.scan(/(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)/)
        seconds = parts.sum { |amount, unit| amount.to_f * units.fetch(unit) }
        seconds.positive? ? seconds : nil
      end

      def total_memory
        if File.readable?("/proc/meminfo")
          File.read("/proc/meminfo")[/MemTotal:\s+(\d+)/, 1].to_i * 1024
        elsif RbConfig::CONFIG["host_os"].include?("darwin")
          `sysctl -n hw.memsize 2>/dev/null`.to_i
        else
          0
        end
      rescue StandardError
        0
      end

      def os_name
        case RbConfig::CONFIG["host_os"]
        when /darwin/ then "darwin"
        when /linux/ then "linux"
        when /mswin|mingw|cygwin/ then "windows"
        else RbConfig::CONFIG["host_os"]
        end
      end

      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
