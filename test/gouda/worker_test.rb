# frozen_string_literal: true

require "gouda/test_helper"

class GoudaWorkerTest < ActiveSupport::TestCase
  include AssertHelper

  # self.use_transactional_tests = false

  # This is a bit obtuse but we need to be able to compute this value from inside the ActiveJob
  # and the job won't have access to the test case object. To avoid mistakes, we can put it
  # in a constant - which is lexically-scoped. We also evaluate it lazily since the Rails
  # constant is possibly not initialized yet when this code loads.
  # We need to include the PID in the path, because we might be running
  # multiple test processes on the same box - and they might start touching
  # files from each other.
  PATH_TO_TEST_FILE = -> { Tempfile.new("#{Process.pid}-gouda-worker-test-output.bin") }
  # PATH_TO_TEST_FILE = -> { File.expand_path(File.join(ENV["TEMPDR"] || "tmp", "#{Process.pid}-gouda-worker-test-output.bin")) }

  class JobWithEnqueueKey < ActiveJob::Base
    self.queue_adapter = :gouda

    def enqueue_concurrency_key
      "zombie"
    end

    def perform
    end
  end

  class SimpleJob < ActiveJob::Base
    self.queue_adapter = :gouda

    def perform
      File.open(PATH_TO_TEST_FILE.call, "a") do |f|
        f.write("A")
      end
    end
  end

  test "runs workloads from all queues without a queue constraint" do
    Gouda.in_bulk do
      6.times { SimpleJob.perform_later }
      6.times { SimpleJob.set(queue: "urgent").perform_later }
    end
    assert_equal 12, Gouda::Workload.where(state: "enqueued").count

    Gouda.worker_loop(n_threads: 1, check_shutdown: Gouda::EmptyQueueShutdownCheck.new)

    # Check that the side effects of the job have been performed - in this case, that
    # 12 chars have been written into the file. Every job writes a char.
    assert_equal 12, File.size(PATH_TO_TEST_FILE.call)

    assert_equal 0, Gouda::Workload.where(state: "enqueued").count
    assert_equal 12, Gouda::Workload.where(state: "finished").count
  end

  test "does not run workloads destined for a different queue" do
    only_from_bravo = Gouda.parse_queue_constraint("queue-bravo")
    bravo_queue_has_no_jobs = Gouda::EmptyQueueShutdownCheck.new(only_from_bravo)

    Gouda.in_bulk do
      12.times { SimpleJob.set(queue: "queue-alpha").perform_later }
    end
    assert_equal 12, Gouda::Workload.where(state: "enqueued").count

    Gouda.worker_loop(n_threads: 1, queue_constraint: only_from_bravo, check_shutdown: bravo_queue_has_no_jobs)

    assert_equal 12, Gouda::Workload.where(state: "enqueued").count
    assert_equal 0, Gouda::Workload.where(state: "finished").count
  end

  test "reaps zombie workloads and then executes replacements" do
    past = 10.minutes.ago
    2.times do
      zombie_job = JobWithEnqueueKey.new
      Gouda::Workload.create!(
        scheduled_at: past,
        active_job_id: zombie_job.job_id,
        execution_started_at: past,
        last_execution_heartbeat_at: past,
        queue_name: "default",
        active_job_class_name: "GoudaWorkerTest::JobWithEnqueueKey",
        serialized_params: {
          job_id: zombie_job.job_id,
          locale: "en",
          priority: nil,
          timezone: "UTC",
          arguments: [],
          job_class: zombie_job.class.to_s,
          executions: 0,
          queue_name: "default",
          enqueued_at: past,
          exception_executions: {}
        },
        state: "executing",
        execution_concurrency_key: nil,
        enqueue_concurrency_key: nil,
        executing_on: "unit test",
        position_in_bulk: 0
      )
    end

    assert_equal 2, Gouda::Workload.where(state: "executing").count

    Gouda.worker_loop(n_threads: 2, check_shutdown: Gouda::TimerShutdownCheck.new(2.0))

    assert_equal 0, Gouda::Workload.where(state: "enqueued").count
    assert_equal 0, Gouda::Workload.where(state: "executing").count

    # The original 2 workloads got zombie-reaped and marked "finished".
    # There should have been only 1 workload enqueued as a retry, because due to
    # the enqueue concurrency key there would only be 1 allowed into the queue. It
    # then should have executed normally and marked "finished".
    assert_equal 3, Gouda::Workload.where(state: "finished").count
  end
end
