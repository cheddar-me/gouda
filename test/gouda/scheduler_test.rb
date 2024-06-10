# frozen_string_literal: true

require "gouda/test_helper"

class GoudaSchedulerTest < ActiveSupport::TestCase
  include AssertHelper

  setup do
    Gouda::Workload.delete_all
    Gouda::JobFuse.delete_all
  end

  class TestJob < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new

    def perform(regular = "ok", mandatory:, optional: "hidden")
    end
  end

  class FailingJob < ActiveJob::Base
    include Gouda::ActiveJobExtensions::Concurrency
    self.queue_adapter = Gouda::Adapter.new

    class MegaError < StandardError
    end

    gouda_control_concurrency_with(enqueue_limit: 1, key: -> { self.class.to_s })

    retry_on StandardError, wait: :polynomially_longer, attempts: 5
    retry_on Gouda::InterruptError, wait: 0, attempts: 5
    retry_on MegaError, attempts: 3, wait: 0

    def perform
      raise MegaError.new "Kaboom!"
    end
  end

  test "keeps re-enqueueing cron jobs after failed job (also with kwargs)" do
    tab = {
      second_minutely: {
        cron: "*/1 * * * * *", # every second
        class: "GoudaSchedulerTest::FailingJob"
      }
    }

    assert_nothing_raised do
      Gouda::Scheduler.update_schedule_from_config!(tab)
      Gouda::Scheduler.update_scheduled_workloads!
    end

    assert_equal 1, Gouda::Workload.enqueued.count
    Gouda.worker_loop(n_threads: 1, check_shutdown: Gouda::TimerShutdownCheck.new(2))

    refute_empty Gouda::Workload.enqueued
    assert Gouda::Workload.count > 3
  end

  test "re-inserts the next subsequent job after executing the queued one" do
    tab = {
      second_minutely: {
        cron: "*/1 * * * * *", # every second
        class: "GoudaSchedulerTest::TestJob",
        args: ["omg"],
        kwargs: {mandatory: "WOOHOO", optional: "yeah"},
        set: {priority: 150}
      }
    }

    assert_nothing_raised do
      Gouda::Scheduler.update_schedule_from_config!(tab)
    end

    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      3.times do
        Gouda::Scheduler.update_scheduled_workloads!
      end
    end

    job = Gouda::Workload.first
    assert_equal 1, Gouda::Workload.count

    sleep 1

    assert_equal job, Gouda::Workload.checkout_and_lock_one(executing_on: "test")

    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      3.times do
        Gouda::Scheduler.update_scheduled_workloads!
      end
    end

    assert_equal 2, Gouda::Workload.count
    Gouda::Workload.all.each(&:perform_and_update_state!)
    assert_equal 0, Gouda::Workload.errored.count
    workload = Gouda::Workload.find_by(scheduler_key: "second_minutely_*/1 * * * * *_GoudaSchedulerTest::TestJob")
    assert_equal 150, workload.priority
    assert_equal ["omg", {
      "optional" => "yeah",
      "mandatory" => "WOOHOO",
      "_aj_ruby2_keywords" => ["mandatory", "optional"]
    }], workload.serialized_params["arguments"]
  end

  test "accepts crontab with nil args" do
    tab = {
      first_hourly: {
        cron: "@hourly",
        class: "GoudaSchedulerTest::TestJob",
        args: [nil, nil]
      }
    }

    assert_nothing_raised do
      Gouda::Scheduler.update_schedule_from_config!(tab)
    end

    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      Gouda::Scheduler.update_scheduled_workloads!
    end

    assert_equal [nil, nil], Gouda::Workload.first.serialized_params["arguments"]
  end

  test "is able to accept a crontab" do
    tab = {
      first_hourly: {
        cron: "@hourly",
        class: "GoudaSchedulerTest::TestJob",
        args: ["one"],
        kwargs: {mandatory: "Yeah"}
      },
      second_minutely: {
        cron: "*/1 * * * *",
        class: "GoudaSchedulerTest::TestJob",
        args: [6],
        kwargs: {mandatory: "Yeah", optional: "something"}
      },
      third_hourly_with_args_and_kwargs: {
        cron: "@hourly",
        class: "GoudaSchedulerTest::TestJob",
        args: [1],
        kwargs: {mandatory: "alright"}
      },
      interval: {
        interval_seconds: 250,
        class: "GoudaSchedulerTest::TestJob",
        args: [4],
        kwargs: {mandatory: "tasty"}
      }
    }
    assert_nothing_raised do
      Gouda::Scheduler.update_schedule_from_config!(tab)
    end

    travel_to Time.utc(2023, 6, 23, 20, 0)
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 4) do
      3.times do
        Gouda::Scheduler.update_scheduled_workloads!
      end
    end

    tab[:fifth] = {
      cron: "@hourly",
      class: "GoudaSchedulerTest::TestJob",
      kwargs: {mandatory: "good"}
    }

    Gouda::Scheduler.update_schedule_from_config!(tab)
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      Gouda::Scheduler.update_scheduled_workloads!
    end

    assert tab.delete(:fifth)
    Gouda::Scheduler.update_schedule_from_config!(tab)
    assert_changes_by(-> { Gouda::Workload.count }, exactly: -1) do
      Gouda::Scheduler.update_scheduled_workloads!
    end

    Gouda::Workload.all.each(&:perform_and_update_state!)
    assert_equal 0, Gouda::Workload.errored.count
    assert_equal [6, {
      "optional" => "something",
      "mandatory" => "Yeah",
      "_aj_ruby2_keywords" => ["mandatory", "optional"]
    }], Gouda::Workload.find_by(scheduler_key: "second_minutely_*/1 * * * *_GoudaSchedulerTest::TestJob").serialized_params["arguments"]
  end
end
