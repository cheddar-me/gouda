# frozen_string_literal: true

require "gouda/test_helper"

class GoudaWorkloadTest < ActiveSupport::TestCase
  include AssertHelper

  class TestJob < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new

    def perform
    end
  end

  test "#schedule_now!" do
    freeze_time
    create_enqueued_workload
    create_enqueued_workload
    workload = create_enqueued_workload
    workload.schedule_now!
    assert_equal 3, Gouda::Workload.enqueued.size
    assert_equal Time.now.utc, workload.scheduled_at
  end

  test "#mark_finished!" do
    freeze_time
    create_enqueued_workload
    create_enqueued_workload
    workload = create_enqueued_workload
    workload.mark_finished!
    assert_equal 2, Gouda::Workload.enqueued.size
    assert_equal 1, Gouda::Workload.finished.size
    assert_equal 1, Gouda::Workload.errored.size
    assert_equal Time.now.utc, workload.execution_finished_at
  end

  test "execution context detection methods" do
    # Test thread execution detection
    thread_workload = Gouda::Workload.create!(
      id: SecureRandom.uuid,
      active_job_id: SecureRandom.uuid,
      active_job_class_name: "TestJob",
      queue_name: "default",
      scheduled_at: Time.now.utc,
      serialized_params: {},
      executing_on: "hostname-1234-uuid-thread-abc123"
    )

    assert thread_workload.executed_on_thread?
    refute thread_workload.uses_async_execution?
    assert_equal :thread, thread_workload.execution_context

    # Test fiber execution detection
    fiber_workload = Gouda::Workload.create!(
      id: SecureRandom.uuid,
      active_job_id: SecureRandom.uuid,
      active_job_class_name: "TestJob",
      queue_name: "default",
      scheduled_at: Time.now.utc,
      serialized_params: {},
      executing_on: "hostname-1234-uuid-fiber-def456"
    )

    assert fiber_workload.uses_async_execution?
    refute fiber_workload.executed_on_thread?
    assert_equal :fiber, fiber_workload.execution_context

    # Test unknown execution context
    unknown_workload = Gouda::Workload.create!(
      id: SecureRandom.uuid,
      active_job_id: SecureRandom.uuid,
      active_job_class_name: "TestJob",
      queue_name: "default",
      scheduled_at: Time.now.utc,
      serialized_params: {},
      executing_on: "legacy-format-without-context"
    )

    refute unknown_workload.executed_on_thread?
    refute unknown_workload.uses_async_execution?
    assert_equal :unknown, unknown_workload.execution_context

    # Test nil executing_on
    nil_workload = Gouda::Workload.create!(
      id: SecureRandom.uuid,
      active_job_id: SecureRandom.uuid,
      active_job_class_name: "TestJob",
      queue_name: "default",
      scheduled_at: Time.now.utc,
      serialized_params: {}
    )

    refute nil_workload.executed_on_thread?
    refute nil_workload.uses_async_execution?
    assert_equal :unknown, nil_workload.execution_context
  end

  def create_enqueued_workload
    now = Time.now.utc
    test_job = TestJob.new

    Gouda::Workload.create!(
      scheduled_at: now + 1.hour,
      active_job_id: test_job.job_id,
      execution_started_at: nil,
      last_execution_heartbeat_at: nil,
      queue_name: "default",
      active_job_class_name: "GoudaWorkloadTest::TestJob",
      serialized_params: {
        job_id: test_job.job_id,
        locale: "en",
        priority: nil,
        timezone: "UTC",
        arguments: [],
        job_class: test_job.class.to_s,
        executions: 0,
        queue_name: "default",
        enqueued_at: now - 1.hour,
        exception_executions: {}
      },
      state: "enqueued",
      execution_concurrency_key: nil,
      enqueue_concurrency_key: nil,
      executing_on: "unit test",
      position_in_bulk: 0
    )
  end
end
