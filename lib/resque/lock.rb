module Resque
  module Lock
    
    def lock_key( options = {} )
      "lock:#{self.name}-#{options.keys.map(&:to_s).sort.join("|")}-#{options.values.map(&:to_s).sort.join("|")}"
    end
    
    def _extra_locks_list_options options = {}
      if self.respond_to? :extra_locks_list_options
        self.send :extra_locks_list_options, options
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
    
    # Where the magic happens.
    def enqueue(klass, options = {})
      # Abort if another job added.
      uuid = lock_uuid( options )
      if uuid.blank?
        uuid = super(klass, options)
        Resque.redis.set( lock_key( options ), uuid )
        puts _extra_locks_list_options( options ).inspect
        _extra_locks_list_options( options ).each do | extra_lock_opts |
          puts extra_lock_opts.inspect
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
        _extra_locks_list_options( args[1] ).each do | extra_lock_opts |
          unlock_uuid( extra_lock_opts )
        end        
      end
    end
  end
end
