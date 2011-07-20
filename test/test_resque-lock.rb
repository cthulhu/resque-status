require File.expand_path( File.dirname( __FILE__ ) + '/test_helper' )

class TestLockedJob < Resque::JobWithStatus
  extend Resque::Lock
  @queue = :test

  def self.perform
  end
end

$release_worker = false

class TestExtraLockedJob < Resque::JobWithStatus

  extend Resque::Lock
  @queue = :test
  
  def self.extra_locks_list_options options
    [{ "arrg2" => 1 }]
  end
  
  def self.extra_locks_jobs_list_options options
    [ 
      { "klass" => "TestLockedJob", "arrg1" => 1 },
      { "klass" => "TestLockedJob", "arrg2" => 1 },
    ]
  end

  def perform
    while !$release_worker
      sleep 1
    end
  end
end


class TestResqueLock < Test::Unit::TestCase

  context "Resque::Lock" do
    setup do
      Resque.redis.flushall
    end
    context "on enqueue with extra lock" do
      setup do
        @options = { :arrg1 => 1 }
        @uuid    = TestExtraLockedJob.create @options
        @status  = Resque::Status.get( @uuid )
        $release_worker = false
      end
      should "create extra locks during job running" do
        worker = Resque::Worker.new(TestExtraLockedJob.instance_eval{@queue})
        worker_thread = Thread.new { worker.process }
        sleep 1
        assert TestExtraLockedJob.locked?( :arrg1 => 1 )
        assert TestExtraLockedJob.locked_by_competitor?( :arrg2 => 1 )
        assert TestLockedJob.locked_by_competitor?( :arrg1 => 1 )
        assert TestLockedJob.locked_by_competitor?( :arrg2 => 1 )
        assert_equal 1, TestLockedJob.competitive_lock_value( :arrg2 => 1 )
        assert_equal 1, TestLockedJob.competitive_lock_value( :arrg1 => 1 )
        assert !TestLockedJob.locked?( :arrg1 => 1 )
        TestLockedJob.create( :arrg1 => 2 )
        assert TestLockedJob.locked?( :arrg1 => 2 )
        
        TestExtraLockedJob.create({ :arrg2 => 1 })

        assert 2, TestLockedJob.competitive_lock_value( :arrg2 => 1 )
        assert 2, TestLockedJob.competitive_lock_value( :arrg1 => 1 )
        
        $release_worker = true
        worker_thread.join
        assert !TestLockedJob.locked?( :arrg1 => 1 )
        assert TestLockedJob.locked?( :arrg1 => 2 )
        assert 0, TestLockedJob.competitive_lock_value( :arrg2 => 1 )
        assert 0, TestLockedJob.competitive_lock_value( :arrg1 => 1 )
        assert 0, TestExtraLockedJob.competitive_lock_value( :arrg1 => 1 )
        assert 0, TestExtraLockedJob.competitive_lock_value( :arrg2 => 1 )
      end
    end
    context "on enqueue" do
      setup do
        @options = { :arrg1 => 1 }
        @uuid    = TestLockedJob.create @options
        @status  = Resque::Status.get( @uuid )
      end
      should "create a lock with appropriated status uuid" do
        assert TestLockedJob.locked?( @options )
        assert !TestExtraLockedJob.locked?( @options )
        assert @status["status"] == "queued"
      end
      context "and enqueue" do
        setup do
          @uuid2   = TestLockedJob.create @options
          @status2 = Resque::Status.get( @uuid2 )
        end
        should "be locked with the same job id" do
          assert TestLockedJob.locked?( @options )
          assert !TestExtraLockedJob.locked?( @options )
          assert @status2["status"] == "queued"
          assert @status == @status2
          assert @uuid == @uuid2
        end
      end
    end
  end
end
