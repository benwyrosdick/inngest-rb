# frozen_string_literal: true

require "io/wait"
require "json"
require "socket"

module Inngest
  module Connect
    # A minimal HTTP server for Kubernetes probes, built on TCPServer so it
    # needs no dependencies:
    #
    # - GET /ready: 200 while the worker is connected and taking work; 503
    #   while it's connecting, reconnecting or shutting down
    # - GET /live: 200 while the worker's internals are running; 503 if they
    #   have stopped or stalled, in which case the process should be restarted
    #
    # Responses include a small JSON status body for debugging.
    class HealthServer
      READ_TIMEOUT = 2
      MAX_REQUEST_LINE = 8 * 1024

      def initialize(worker, port:, host: "0.0.0.0", logger: nil)
        @worker = worker
        @host = host
        @requested_port = Integer(port)
        @logger = logger
      end

      # The port the server listens on (useful when started with port 0).
      def port
        @server&.addr&.[](1)
      end

      def start
        @server = TCPServer.new(@host, @requested_port)
        @thread = Thread.new do
          Thread.current.name = "inngest-health"
          serve
        end
        @logger&.info("[inngest] health checks on http://#{@host}:#{port} (/ready, /live)")
        self
      rescue SystemCallError => e
        raise ConfigError, "can't listen for health checks on #{@host}:#{@requested_port}: #{e.message}"
      end

      def stop
        @server&.close
        @thread&.join(2)
      rescue IOError
        nil
      end

      private

      def serve
        loop do
          client = @server.accept
          begin
            handle(client)
          rescue StandardError => e
            @logger&.debug("[inngest] health check request failed: #{e.class}: #{e.message}")
          ensure
            client.close
          end
        end
      rescue IOError, SystemCallError
        nil # The server was closed.
      end

      def handle(client)
        line = read_request_line(client)
        return unless line

        method, path = line.split(" ", 3)
        status, body =
          if %w[GET HEAD].include?(method)
            route(path.to_s.split("?", 2).first)
          else
            [405, { "error" => "method not allowed" }]
          end

        payload = JSON.generate(body)
        client.write("HTTP/1.1 #{status} #{reason(status)}\r\n" \
                     "Content-Type: application/json\r\n" \
                     "Content-Length: #{payload.bytesize}\r\n" \
                     "Connection: close\r\n\r\n")
        client.write(payload) unless method == "HEAD"
      end

      def route(path)
        case path
        when "/ready", "/readyz" then [@worker.ready? ? 200 : 503, @worker.health]
        when "/live", "/livez", "/healthz" then [@worker.alive? ? 200 : 503, @worker.health]
        else [404, { "error" => "not found" }]
        end
      end

      # Reads the request line, giving up on slow or oversized requests. The
      # headers aren't needed.
      def read_request_line(client)
        buffer = +""
        until buffer.include?("\n")
          return nil if buffer.bytesize > MAX_REQUEST_LINE
          return nil unless client.wait_readable(READ_TIMEOUT)

          chunk = client.read_nonblock(1024, exception: false)
          return nil if chunk.nil?

          buffer << chunk if chunk.is_a?(String)
        end
        buffer.lines.first.strip
      end

      def reason(status)
        { 200 => "OK", 404 => "Not Found", 405 => "Method Not Allowed", 503 => "Service Unavailable" }[status]
      end
    end
  end
end
