require 'bunny'

module Waffle
  module Strategies
    module RabbitMQ
      class Base

        def initialize(url = nil, exchange = 'unknown_exchange')
          @bunny = Bunny.new(url)
          @bunny.start
          @exchange = @bunny.exchange exchange
        end

      end

      class Producer < RabbitMQ::Base

        def initialize(url = nil, exchange = 'unknown_exchnage')
          super url, exchange
        end

        def publish(message = '')
          @exchange.publish message
        end

      end

      class Consumer < RabbitMQ::Base

        def initialize(url = nil, exchange = 'unknown_exchnage')
          super url, exchange
        end

        def subscribe(queue = '')
          # Create queue...
          @queue = @bunny.queue queue, :durable => true, :auto_delete => true

          # ...and attach it to exchange
          @queue.bind @exchange

          @queue.subscribe do |message|
            yield message[:payload]
          end
        end

      end
    end
  end
end
