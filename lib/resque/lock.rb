require 'resque'
require 'redisk'
require 'uuid'

module Resque
  module Lock
    
    # normal lock
    def lock_key( options = {} )
      klass = options.delete("klass") || self.name
      "lock:#{klass}-#{options.to_a.sort_by(&:first).map{|a| a.join("=") }.join("|")}"
    end
    
    def competitive_lock_key( options = {} )
      klass = options.delete("klass") || self.name
      "competitive-lock:#{klass}-#{options.to_a.sort_by(&:first).map{|a| a.join("=") }.join("|")}"
    end    
    
    def locked?( options )
      Resque.redis.exists( lock_key( options ) )
    end
      
    def lock_uuid( options )
      Resque.redis.get( lock_key( options ) )
    end
    
    # competitive lock
    
    def _extra_locks_list_options options = {}
      if self.respond_to? :extra_locks_list_options
        self.send :extra_locks_list_options, options
      else
        [] 
      end
    end

    def _extra_locks_jobs_list_options options = {}
      if self.respond_to? :extra_locks_jobs_list_options
        self.send :extra_locks_jobs_list_options, options
      else
        [] 
      end
    end    
    
    def locked_by_competitor? options
      Resque.redis.exists( competitive_lock_key( options ) )
    end

    def with_competitive_lock uuid, options
      _extra_locks_list_options( options ).each do | extra_lock_opts |
        Resque.redis.set( competitive_lock_key( extra_lock_opts ), uuid )
      end
      _extra_locks_jobs_list_options( options ).each do | extra_lock_jobs_opts |
        Resque.redis.set( competitive_lock_key( extra_lock_jobs_opts ), uuid )
      end
      begin
        yield
      ensure
        _extra_locks_jobs_list_options( options ).each do | extra_lock_jobs_opts |
          competitive_unlock( extra_lock_jobs_opts )
        end      
        _extra_locks_list_options( options ).each do | extra_lock_opts |
          competitive_unlock( extra_lock_opts )
        end         
      end
    end
    
    # Where the magic happens.
    def enqueue(klass, options = {})
      # Abort if another job added.
      uuid = lock_uuid( options )
      if uuid.nil? || uuid.empty?
        uuid = super(klass, options)
        Resque.redis.set( lock_key( options ), uuid )
      end
      uuid
    end  
    
    def around_perform_lock *args
      unless locked_by_competitor? args[1]
        uuid = lock_uuid( args[1] )
        with_competitive_lock uuid, args[1] do
          yield
        end
        unlock( args[1] ) 
      else
        unlock( args[1] )
        Resque.enqueue(self, *args)
      end
    end
  protected
    def competitive_unlock( options )
      Resque.redis.del( competitive_lock_key( options ) )
    end
    def unlock( options )
      Resque.redis.del( lock_key( options ) )
    end

  end
end
