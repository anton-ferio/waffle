require 'date'
require 'bunny'

module Waffle
  module Transports
    class Rabbitmq < Base
      EXCHANGE = 'events'

      protected
      def publish_impl(flow = 'events', message = '')
        exchange.publish(encoder.encode(message), :key => flow)
      end

      def subscribe_impl(flow = 'events', &block)
        if flow.is_a?(Array)
          flow.each{|f| queue.bind(exchange, :key => f)}
        else
          queue.bind(exchange, :key => flow)
        end

        queue.subscribe(block: true) do |delivery_info, properties, payload|
          block.call(delivery_info.routing_key, encoder.decode(payload))
        end
      end

      def connection_exceptions
        [Bunny::TCPConnectionFailed, Errno::ECONNRESET]
      end

      def channel
        @channel ||= @connection.create_channel
      end

      def exchange
        @exchange ||= channel.direct(config.options['exchange'] || EXCHANGE, durable: true)
      end

      def queue
        @queue ||= channel.queue(config.options['queue'] || '', durable: true, auto_delete: true)
      end

      def do_connect
        @exchange = nil
        @queue = nil
        @channel = nil
        @connection = Bunny.new(config.url)
        @connection.start
      end
    end
  end
end
