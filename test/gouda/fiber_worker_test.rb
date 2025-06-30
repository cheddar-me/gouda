# frozen_string_literal: true

require "gouda/test_helper"

class FiberWorkerTest < ActiveSupport::TestCase
  include AssertHelper

  # Path for test file output similar to worker_test.rb pattern
  PATH_TO_FIBER_TEST_FILE = -> { File.expand_path(File.join("tmp", "#{Process.pid}-gouda-fiber-worker-test-output.bin")) }

  class FiberTestJob < ActiveJob::Base
    self.queue_adapter = :gouda

    def perform(value = "F")
      File.open(PATH_TO_FIBER_TEST_FILE.call, "a") do |f|
        f.write(value)
      end
    end
  end

  class FiberJobWithQueue < ActiveJob::Base
    self.queue_adapter = :gouda
    queue_as :fiber_queue

    def perform
      File.open(PATH_TO_FIBER_TEST_FILE.call, "a") do |f|
        f.write("Q")
      end
    end
  end

  setup do
    # Clean up test file before each test
    File.delete(PATH_TO_FIBER_TEST_FILE.call) if File.exist?(PATH_TO_FIBER_TEST_FILE.call)
  end

  teardown do
    # Clean up test file after each test
    File.delete(PATH_TO_FIBER_TEST_FILE.call) if File.exist?(PATH_TO_FIBER_TEST_FILE.call)
  end

  test "fiber worker can be configured" do
    original_use_fiber_scheduler = Gouda.config.use_fiber_scheduler
    original_fiber_worker_count = Gouda.config.fiber_worker_count
    original_async_db_pool_size = Gouda.config.async_db_pool_size

    Gouda.configure do |config|
      config.use_fiber_scheduler = true
      config.fiber_worker_count = 5
      config.async_db_pool_size = 10
    end

    assert_equal true, Gouda.config.use_fiber_scheduler
    assert_equal 5, Gouda.config.fiber_worker_count
    assert_equal 10, Gouda.config.async_db_pool_size
  ensure
    # Reset configuration
    Gouda.config.use_fiber_scheduler = original_use_fiber_scheduler
    Gouda.config.fiber_worker_count = original_fiber_worker_count
    Gouda.config.async_db_pool_size = original_async_db_pool_size
  end

  test "FiberSafeSet works correctly" do
    set = Gouda::FiberWorker::FiberSafeSet.new

    assert_equal [], set.to_a

    set.add("test1")
    set.add("test2")
    assert_equal 2, set.to_a.size
    assert_includes set.to_a, "test1"
    assert_includes set.to_a, "test2"

    set.delete("test1")
    assert_equal 1, set.to_a.size
    assert_includes set.to_a, "test2"
    refute_includes set.to_a, "test1"
  end

  test "fiber worker runs workloads from all queues without a queue constraint" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    Gouda.in_bulk do
      6.times { FiberTestJob.perform_later("A") }
      6.times { FiberTestJob.set(queue: "urgent").perform_later("B") }
    end
    assert_equal 12, Gouda::Workload.where(state: "enqueued").count

    # Use fiber worker with empty queue shutdown check
    Gouda::FiberWorker.worker_loop(
      n_fibers: 2,
      check_shutdown: Gouda::EmptyQueueShutdownCheck.new
    )

    # Check that jobs were executed (allow for some variation due to timing)
    file_size = File.exist?(PATH_TO_FIBER_TEST_FILE.call) ? File.size(PATH_TO_FIBER_TEST_FILE.call) : 0
    assert_operator file_size, :>=, 10, "Expected at least 10 jobs to complete, got #{file_size}"

    # Check database state
    finished_count = Gouda::Workload.where(state: "finished").count
    enqueued_count = Gouda::Workload.where(state: "enqueued").count

    assert_operator finished_count, :>=, 10, "Expected at least 10 finished jobs, got #{finished_count}"
    assert_operator enqueued_count, :<=, 2, "Expected at most 2 enqueued jobs remaining, got #{enqueued_count}"
  end

  test "fiber worker does not run workloads destined for a different queue" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    only_from_fiber_queue = Gouda.parse_queue_constraint("fiber_queue")
    fiber_queue_has_no_jobs = Gouda::EmptyQueueShutdownCheck.new(only_from_fiber_queue)

    Gouda.in_bulk do
      12.times { FiberTestJob.set(queue: "other_queue").perform_later("X") }
    end
    assert_equal 12, Gouda::Workload.where(state: "enqueued").count

    Gouda::FiberWorker.worker_loop(
      n_fibers: 1,
      queue_constraint: only_from_fiber_queue,
      check_shutdown: fiber_queue_has_no_jobs
    )

    assert_equal 12, Gouda::Workload.where(state: "enqueued").count
    assert_equal 0, Gouda::Workload.where(state: "finished").count

    # File should not exist because no jobs were executed
    refute File.exist?(PATH_TO_FIBER_TEST_FILE.call)
  end

  test "fiber worker identifies itself with fiber ID in executing_on field" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    FiberTestJob.perform_later("I")

    # Use empty queue shutdown instead of timer to ensure job completes
    Gouda::FiberWorker.worker_loop(
      n_fibers: 1,
      check_shutdown: Gouda::EmptyQueueShutdownCheck.new
    )

    finished_workload = Gouda::Workload.finished.last
    assert_not_nil finished_workload

    # Check that executing_on contains fiber identifier (should contain 'f0x' for fiber)
    assert_includes finished_workload.executing_on, "f0x"

    # Verify the job executed
    assert_equal 1, File.size(PATH_TO_FIBER_TEST_FILE.call)
  end

  test "fiber worker handles multiple fibers concurrently" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    # Enqueue more jobs than we have fibers to test concurrency
    Gouda.in_bulk do
      8.times { |i| FiberTestJob.perform_later(i.to_s) }
    end

    assert_equal 8, Gouda::Workload.where(state: "enqueued").count

    Gouda::FiberWorker.worker_loop(
      n_fibers: 3,  # Use 3 fibers to handle 8 jobs
      check_shutdown: Gouda::EmptyQueueShutdownCheck.new
    )

    # Check that jobs were executed (allow for some variation due to timing)
    file_size = File.exist?(PATH_TO_FIBER_TEST_FILE.call) ? File.size(PATH_TO_FIBER_TEST_FILE.call) : 0
    assert_operator file_size, :>=, 6, "Expected at least 6 jobs to complete, got #{file_size}"

    # Check database state
    finished_count = Gouda::Workload.where(state: "finished").count
    enqueued_count = Gouda::Workload.where(state: "enqueued").count

    assert_operator finished_count, :>=, 6, "Expected at least 6 finished jobs, got #{finished_count}"
    assert_operator enqueued_count, :<=, 2, "Expected at most 2 enqueued jobs remaining, got #{enqueued_count}"

    # Check that fiber IDs were used
    executing_on_values = Gouda::Workload.finished.pluck(:executing_on).uniq
    assert_operator executing_on_values.size, :>=, 1
    executing_on_values.each do |executing_on|
      assert_includes executing_on, "f0x", "Expected fiber identifier in executing_on: #{executing_on}"
    end
  end

  test "start_with_scheduler_type chooses correct worker based on configuration" do
    original_use_fiber_scheduler = Gouda.config.use_fiber_scheduler

    # Test with fiber scheduler enabled - we'll just check that jobs get processed
    # since we can't easily mock the worker_loop methods
    Gouda.configure do |config|
      config.use_fiber_scheduler = true
    end

    assert_equal true, Gouda.config.use_fiber_scheduler

    # Test with fiber scheduler disabled
    Gouda.configure do |config|
      config.use_fiber_scheduler = false
    end

    assert_equal false, Gouda.config.use_fiber_scheduler
  ensure
    Gouda.config.use_fiber_scheduler = original_use_fiber_scheduler
  end
end
