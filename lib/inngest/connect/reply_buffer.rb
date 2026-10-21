# frozen_string_literal: true

module Inngest
  module Connect
    # Holds execution replies until the gateway acknowledges them. Replies that
    # stay unacknowledged (e.g. because the connection dropped) are flushed over
    # HTTP instead.
    class ReplyBuffer
      Item = Struct.new(:request_id, :bytes, :added_at)

      def initialize(max_bytes: 500 * 1024 * 1024, logger: nil)
        @max_bytes = max_bytes
        @logger = logger
        @items = {}
        @size = 0
        @lock = Mutex.new
      end

      def add(request_id, bytes)
        @lock.synchronize do
          if bytes.bytesize > @max_bytes
            @logger&.warn("[inngest] reply #{request_id} is too large to buffer")
            return false
          end

          remove(request_id)
          until @size + bytes.bytesize <= @max_bytes || @items.empty?
            oldest = @items.each_key.first
            @logger&.warn("[inngest] evicting unacknowledged reply #{oldest} to make room")
            remove(oldest)
          end
          @items[request_id] = Item.new(request_id, bytes, monotonic_now)
          @size += bytes.bytesize
          true
        end
      end

      def delete(request_id)
        @lock.synchronize { remove(request_id) }
      end

      def older_than(seconds)
        cutoff = monotonic_now - seconds
        @lock.synchronize { @items.values.select { |item| item.added_at <= cutoff } }
      end

      def all
        @lock.synchronize { @items.values }
      end

      def empty?
        @lock.synchronize { @items.empty? }
      end

      private

      def remove(request_id)
        item = @items.delete(request_id)
        @size -= item.bytes.bytesize if item
        !item.nil?
      end

      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
