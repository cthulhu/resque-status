module Resque
  module Lock
    def lock(*args)
      "lock:#{name}-#{args.to_s}"
    end
    def locked?(*args)
      Resque.redis.exists(lock(*args))
    end
  end
end
