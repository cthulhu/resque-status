require 'resque'
require 'redisk'
require 'uuid'

module Resque
  module Lock
    
    def lock_key( options = {} )
      klass = options.delete(:klass) || self.name
      "lock:#{klass}-#{options.keys.map(&:to_s).sort.join("|")}-#{options.values.map(&:to_s).sort.join("|")}"
    end
    
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
    
    def locked?( options )
      Resque.redis.exists( lock_key( options ) )
    end
      
    def lock_uuid( options )
      Resque.redis.get( lock_key( options ) )
    end
    
    def unlock_uuid( options )
      Resque.redis.del( lock_key( options ) )
    end
    
    def lock_different_jobs uuid, options
      _extra_locks_jobs_list_options( options ).each do | extra_lock_jobs_opts |
        Resque.redis.set( lock_key( extra_lock_jobs_opts ), uuid )
      end
    end
    
    def unlock_different_jobs options
      _extra_locks_jobs_list_options( options ).each do | extra_lock_jobs_opts |
        unlock_uuid( extra_lock_jobs_opts )
      end
    end    
    
    def lock_same_jobs uuid, options
      _extra_locks_list_options( options ).each do | extra_lock_opts |
        Resque.redis.set( lock_key( extra_lock_opts ), uuid )
      end
    end
    
    def unlock_same_jobs options
      _extra_locks_list_options( options ).each do | extra_lock_opts |
        unlock_uuid( extra_lock_opts )
      end        
    end
    
    # Where the magic happens.
    def enqueue(klass, options = {})
      # Abort if another job added.
      uuid = lock_uuid( options )
      if uuid.blank?
        uuid = super(klass, options)
        Resque.redis.set( lock_key( options ), uuid )
        lock_same_jobs uuid, options
        lock_different_jobs options
      end
      uuid
    end  
    
    def around_perform_lock *args
      begin
        yield
      ensure
        unlock_uuid( args[1] )
        unlock_same_jobs( args[1] )
        unlock_different_jobs( args[1] )
      end
    end
  end
end
