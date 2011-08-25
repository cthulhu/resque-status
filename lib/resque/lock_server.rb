require 'resque/status'
require 'resque/lock'

module Resque
  module LockServer
    
    VIEW_PATH = File.join(File.dirname(__FILE__), 'lock_server', 'views')
    
    def self.registered(app)
    
      app.get '/locks' do
        @locks = []
        lock_view(:locks)
      end

    end

    app.tabs << "Locks"
  end
end
Resque::Server.register Resque::LockServer
