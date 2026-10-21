# frozen_string_literal: true

require "monitor"
require "openssl"
require "socket"
require "uri"
require "websocket/driver"

module Inngest
  module Connect
    # One WebSocket connection to an Inngest gateway, built on websocket-driver
    # (the library ActionCable uses) over a plain TCP/TLS socket.
    #
    # Incoming messages are parsed on a dedicated reader thread and handed to
    # `on_message` as Proto::ConnectMessage objects. #send_message is safe to
    # call from any thread.
    class Connection
      PROTOCOL = "v0.connect.inngest.com"
      MAX_MESSAGE_SIZE = 10 * 1024 * 1024
      CONNECT_TIMEOUT = 10

      attr_reader :url, :gateway_group, :connection_id, :auth_data, :handshake
      attr_accessor :heartbeat_interval, :extend_lease_interval, :status_interval, :last_message_at

      def initialize(url:, gateway_group:, connection_id:, auth_data:, logger:, on_message:, on_close:)
        @url = url
        @gateway_group = gateway_group
        @connection_id = connection_id
        @auth_data = auth_data
        @logger = logger
        @on_message = on_message
        @on_close = on_close
        @lock = Monitor.new
        @opened = Queue.new
        # Receives :ready (or :failed) when the handshake finishes.
        @handshake = Queue.new
        @closed = false
        @ready = false
      end

      def open
        uri = URI(url)
        tcp = Socket.tcp(uri.host, uri.port, connect_timeout: CONNECT_TIMEOUT)
        @socket = uri.scheme == "wss" ? tls_wrap(tcp, uri.host) : tcp

        @driver = WebSocket::Driver.client(self, protocols: [PROTOCOL], max_length: MAX_MESSAGE_SIZE,
                                                 binary_data_format: :string)
        @driver.on(:open) { @opened << true }
        @driver.on(:message) { |event| receive(event.data) }
        @driver.on(:close) { |event| handle_close("closed by gateway (#{event.code}) #{event.reason}") }
        @driver.on(:error) { |event| @logger.error("[inngest] websocket error: #{event.message}") }

        @lock.synchronize { @driver.start }
        @reader = Thread.new { read_loop }
        @reader.name = "inngest-connect-reader"

        opened = @opened.pop(timeout: CONNECT_TIMEOUT)
        raise Error, "timed out opening websocket to #{url}" unless opened && !@closed

        @last_message_at = monotonic_now
        self
      end

      # Called by websocket-driver to write raw bytes.
      def write(data)
        @lock.synchronize { @socket.write(data) }
      end

      def send_message(kind, payload = nil)
        message = Proto::ConnectMessage.new(kind: kind, payload: payload ? payload.to_proto : "")
        @lock.synchronize do
          raise Error, "connection is closed" if @closed

          @driver.binary(Proto::ConnectMessage.encode(message))
        end
        true
      end

      def ready!
        @ready = true
      end

      def ready?
        @ready && !@closed
      end

      def closed?
        @closed
      end

      def close(reason = "WORKER_SHUTDOWN", code = 1000)
        @lock.synchronize do
          return if @closed

          begin
            @driver.close(reason, code)
          rescue StandardError
            nil
          end
        end
        # Give the gateway a moment to acknowledge the close frame.
        @reader&.join(2) unless Thread.current == @reader
        handle_close("closed by worker: #{reason}")
      end

      private

      def receive(data)
        @last_message_at = monotonic_now
        message = Proto::ConnectMessage.decode(data)
        @on_message.call(self, message)
      rescue Google::Protobuf::ParseError => e
        @logger.error("[inngest] failed to parse gateway message: #{e.message}")
      rescue StandardError => e
        @logger.error("[inngest] error handling gateway message: #{e.class}: #{e.message}")
      end

      # Reads and writes share one lock because an OpenSSL socket can't be read
      # and written from different threads at the same time. Reads are
      # non-blocking so the lock is only held while there's data to process.
      def read_loop
        loop do
          chunk = @lock.synchronize do
            data = @socket.read_nonblock(64 * 1024, exception: false)
            @driver.parse(data) if data.is_a?(String)
            data
          end

          case chunk
          when nil then raise EOFError, "end of stream"
          when :wait_readable then IO.select([@socket], nil, nil, 1)
          when :wait_writable then IO.select(nil, [@socket], nil, 1)
          end
          break if @closed
        end
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError => e
        handle_close("socket closed: #{e.class}: #{e.message}")
      rescue StandardError => e
        @logger.error("[inngest] websocket reader crashed: #{e.class}: #{e.message}")
        handle_close("reader error")
      end

      def handle_close(reason)
        first = @lock.synchronize do
          next false if @closed

          @closed = true
        end
        return unless first

        @opened << false
        @handshake << :failed
        begin
          @socket&.close
        rescue StandardError
          nil
        end
        @on_close.call(self, reason)
      end

      def tls_wrap(tcp, host)
        context = OpenSSL::SSL::SSLContext.new
        context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)
        ssl = OpenSSL::SSL::SSLSocket.new(tcp, context)
        ssl.hostname = host
        ssl.sync_close = true
        ssl.connect
        ssl.post_connection_check(host)
        ssl
      end

      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
