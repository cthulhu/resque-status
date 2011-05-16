module Resque
  module Lock
    
    def lock(*args)
      "lock:#{name}-#{args.to_s}"
    end
    
    def locked?(*args)
      Resque.redis.exists(lock(*args))
    end
    
    # Where the magic happens.
    def around_perform_lock(*args)
      # Abort if another job has created a lock.
      return unless Resque.redis.setnx(lock(*args), true)

      begin
        yield
      ensure
        # Always clear the lock when we're done, even if there is an
        # error.
        Resque.redis.del(lock(*args))
      end
    end  
  end
end
