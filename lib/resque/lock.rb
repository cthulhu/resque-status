module Resque
  module Lock
    
    def lock_key( options = {} )
      "lock:#{self.name}-#{options.keys.map(&:to_s).sort.join("|")}-#{options.values.map(&:to_s).sort.join("|")}"
    end
    
    def extra_locks_list_options options = {}
      []
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
    
    # Where the magic happens.
    def enqueue(klass, options = {})
      # Abort if another job added.
      uuid = lock_uuid( options )
      if uuid.blank?
        uuid = super(klass, options)
        Resque.redis.set( lock_key( options ), uuid )
        extra_locks_list_options( options ).each do | extra_lock_opts |
          Resque.redis.set( lock_key( extra_lock_opts ), uuid )
        end
      end
      uuid
    end  
    
    def around_perform_lock *args
      begin
        yield
      ensure
        unlock_uuid( args[1] )
        extra_locks_list_options( args[1] ).each do | extra_lock_opts |
          unlock_uuid( extra_lock_opts )
        end        
      end
    end
  end
end
