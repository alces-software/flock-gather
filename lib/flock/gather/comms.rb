require 'faraday'
require 'faraday_middleware'
require 'json'

module Flock
  module Gather
    class Comms
      def initialize(endpoint)
        @endpoint = endpoint
      end

      def set(k, v, auth)
        resp = connection(auth).post('set') do |req|
          req.headers['Content-Type'] = 'application/json'
          req.body = {key: k, value: v}.to_json
        end
        resp.status == 204
      end

      private
      def connection(auth = nil)
        Faraday.new(@endpoint) do |conn|
          conn.response :json, :content_type => /\bjson$/
          conn.basic_auth(Process.euid,auth) unless auth.nil?
          conn.adapter Faraday.default_adapter
        end
      end
    end
  end
end
