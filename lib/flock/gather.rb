module Flock
  module Gather
    class << self
      def register(name, clazz)
        gatherers[name] = clazz
      end

      def gatherers
        @gatherers ||= {}
      end

      def run(name)
        require "flock/gather/#{name}"
        gatherers[name].run
      end

      def endpoint
        ENV['FLOCK_GATHER_ENDPOINT'] || 'http://localhost:9292'
      end

      def endpoint_auth
        ENV['FLOCK_GATHER_AUTH'] || 'none'
      end
    end
  end
end
