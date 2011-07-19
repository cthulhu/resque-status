require 'test_helper'

class TestLockedJob < Resque::JobWithStatus
  extend Resque::Lock
  @queue = :test

  def self.perform
  end
end

class TestExtraLockedJob < Resque::JobWithStatus

  extend Resque::Lock
  @queue = :test

  def self.perform
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
