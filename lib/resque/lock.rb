module Resque
  module Lock
    
    def lock_key( options = {} )
      "lock:#{self.class.name}-#{options.keys.map(&:to_s).sort.join("|")}-#{options.values.map(&:to_s).sort.join("|")}"
    end
    
    def locked?( options )
      Resque.redis.exists( lock_key( options ) )
    end
    
    def lock_uuid( options )
      Resque.redis.get( lock_key( options ) )
    end
    
    # Where the magic happens.
    def self.enqueue(klass, options = {})
      # Abort if another job added.
      uuid = lock_uuid( options )
      if uuid.blank?
        uuid = super(klass, options = {})
        Resque.redis.set( lock_key( options ), uuid )
      end
      uuid
    end  
  end
end
