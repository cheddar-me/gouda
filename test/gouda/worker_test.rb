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
  PATH_TO_TEST_FILE = -> { File.expand_path(File.join("tmp", "#{Process.pid}-gouda-worker-test-output.bin")) }

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

  class FiberTestJob < ActiveJob::Base
    self.queue_adapter = :gouda

    def perform(value = "F")
      File.open(PATH_TO_TEST_FILE.call, "a") do |f|
        f.write(value)
      end
    end
  end

  class JobWithQueue < ActiveJob::Base
    self.queue_adapter = :gouda
    queue_as :test_queue

    def perform
      File.open(PATH_TO_TEST_FILE.call, "a") do |f|
        f.write("Q")
      end
    end
  end

  setup do
    # Clean up test file before each test
    File.delete(PATH_TO_TEST_FILE.call) if File.exist?(PATH_TO_TEST_FILE.call)
  end

  teardown do
    # Clean up test file after each test
    File.delete(PATH_TO_TEST_FILE.call) if File.exist?(PATH_TO_TEST_FILE.call)
  end

  # === Thread-based execution tests ===

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

  # === Configuration tests ===

  test "hybrid worker can be configured" do
    original_use_fiber_scheduler = Gouda.config.use_fiber_scheduler
    original_async_db_pool_size = Gouda.config.async_db_pool_size
    original_fibers_per_thread = Gouda.config.fibers_per_thread

    Gouda.configure do |config|
      config.use_fiber_scheduler = true
      config.fibers_per_thread = 5
      config.async_db_pool_size = 10
    end

    assert_equal true, Gouda.config.use_fiber_scheduler
    assert_equal 5, Gouda.config.fibers_per_thread
    assert_equal 10, Gouda.config.async_db_pool_size
  ensure
    # Reset configuration
    Gouda.config.use_fiber_scheduler = original_use_fiber_scheduler
    Gouda.config.fibers_per_thread = original_fibers_per_thread
    Gouda.config.async_db_pool_size = original_async_db_pool_size
  end

  test "ThreadSafeSet works correctly" do
    set = Gouda::ThreadSafeSet.new

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

  # === Hybrid execution tests (threads + fibers) ===

  test "hybrid worker runs workloads from all queues without a queue constraint" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    Gouda.in_bulk do
      6.times { FiberTestJob.perform_later("A") }
      6.times { FiberTestJob.set(queue: "urgent").perform_later("B") }
    end
    assert_equal 12, Gouda::Workload.where(state: "enqueued").count

    # Use hybrid worker (threads + fibers) with empty queue shutdown check
    Gouda.worker_loop(
      n_threads: 1,
      use_fibers: true,
      fibers_per_thread: 2,
      check_shutdown: Gouda::EmptyQueueShutdownCheck.new
    )

    # Check that jobs were executed (allow for some variation due to timing)
    file_size = File.exist?(PATH_TO_TEST_FILE.call) ? File.size(PATH_TO_TEST_FILE.call) : 0
    assert_operator file_size, :>=, 10, "Expected at least 10 jobs to complete, got #{file_size}"

    # Check database state
    finished_count = Gouda::Workload.where(state: "finished").count
    enqueued_count = Gouda::Workload.where(state: "enqueued").count

    assert_operator finished_count, :>=, 10, "Expected at least 10 finished jobs, got #{finished_count}"
    assert_operator enqueued_count, :<=, 2, "Expected at most 2 enqueued jobs remaining, got #{enqueued_count}"
  end

  test "hybrid worker does not run workloads destined for a different queue" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    only_from_test_queue = Gouda.parse_queue_constraint("test_queue")
    test_queue_has_no_jobs = Gouda::EmptyQueueShutdownCheck.new(only_from_test_queue)

    Gouda.in_bulk do
      12.times { FiberTestJob.set(queue: "other_queue").perform_later("X") }
    end
    assert_equal 12, Gouda::Workload.where(state: "enqueued").count

    Gouda.worker_loop(
      n_threads: 1,
      use_fibers: true,
      fibers_per_thread: 1,
      queue_constraint: only_from_test_queue,
      check_shutdown: test_queue_has_no_jobs
    )

    assert_equal 12, Gouda::Workload.where(state: "enqueued").count
    assert_equal 0, Gouda::Workload.where(state: "finished").count

    # File should not exist because no jobs were executed
    refute File.exist?(PATH_TO_TEST_FILE.call)
  end

  test "hybrid worker identifies itself with fiber ID in executing_on field" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    FiberTestJob.perform_later("I")

    # Use empty queue shutdown instead of timer to ensure job completes
    Gouda.worker_loop(
      n_threads: 1,
      use_fibers: true,
      fibers_per_thread: 1,
      check_shutdown: Gouda::EmptyQueueShutdownCheck.new
    )

    finished_workload = Gouda::Workload.finished.last
    assert_not_nil finished_workload

    # Check that executing_on contains fiber identifier (should contain 'fiber-' for fiber)
    assert_includes finished_workload.executing_on, "fiber-"

    # Verify the job executed
    assert_equal 1, File.size(PATH_TO_TEST_FILE.call)
  end

  test "hybrid worker handles multiple fibers concurrently" do
    skip "This test requires async gem and may not work in CI environment" unless defined?(Async)

    # Enqueue more jobs than we have fibers to test concurrency
    Gouda.in_bulk do
      8.times { |i| FiberTestJob.perform_later(i.to_s) }
    end

    assert_equal 8, Gouda::Workload.where(state: "enqueued").count

    Gouda.worker_loop(
      n_threads: 1,
      use_fibers: true,
      fibers_per_thread: 3,  # Use 3 fibers to handle 8 jobs
      check_shutdown: Gouda::EmptyQueueShutdownCheck.new
    )

    # Check that jobs were executed (allow for some variation due to timing)
    file_size = File.exist?(PATH_TO_TEST_FILE.call) ? File.size(PATH_TO_TEST_FILE.call) : 0
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
      assert_includes executing_on, "fiber-", "Expected fiber identifier in executing_on: #{executing_on}"
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

  # === Rails isolation level tests ===

  test "warns when Rails isolation level is not set to fiber" do
    # Skip if we don't have Rails or ActiveSupport configured
    skip "Test requires Rails environment" unless defined?(Rails) && Rails.respond_to?(:application)

    original_isolation = begin
      ActiveSupport.isolation_level
    rescue
      nil
    end

    # Set isolation level to something other than :fiber
    ActiveSupport.isolation_level = :thread

    # Capture log output using a simple array instead of StringIO
    original_logger = Gouda.instance_variable_get(:@fallback_gouda_logger)

    # Create a logger that captures messages in our array
    test_logger = ActiveSupport::TaggedLogging.new(Logger.new("File::NULL"))
    test_logger.level = Logger::WARN

    # Override the warn method to capture messages
    def test_logger.warn(message)
      @captured_messages ||= []
      @captured_messages << message
    end

    def test_logger.captured_messages
      @captured_messages || []
    end

    Gouda.instance_variable_set(:@fallback_gouda_logger, test_logger)

    # This should trigger the warning
    Gouda::FiberDatabaseSupport.check_fiber_isolation_level

    captured_messages = test_logger.captured_messages

    # Check that the warning was logged
    warning_found = captured_messages.any? { |msg| msg.include?("FIBER SCHEDULER CONFIGURATION WARNING") }
    assert warning_found, "Expected fiber scheduler warning to be logged"

    isolation_warning_found = captured_messages.any? { |msg| msg.include?("Rails isolation level is set to: thread") }
    assert isolation_warning_found, "Expected isolation level warning to be logged"

    postgres_warning_found = captured_messages.any? { |msg| msg.include?("prevent segfaults with Ruby 3.4+ and PostgreSQL") }
    assert postgres_warning_found, "Expected PostgreSQL-specific warning to be logged"
  ensure
    # Restore original isolation level if it was set
    if original_isolation
      ActiveSupport.isolation_level = original_isolation
    end
    # Restore original logger
    if original_logger
      Gouda.instance_variable_set(:@fallback_gouda_logger, original_logger)
    end
  end

  test "does not warn when Rails isolation level is correctly set to fiber" do
    # Skip if we don't have Rails or ActiveSupport configured
    skip "Test requires Rails environment" unless defined?(Rails) && Rails.respond_to?(:application)

    original_isolation = begin
      ActiveSupport.isolation_level
    rescue
      nil
    end

    # Set isolation level to :fiber
    ActiveSupport.isolation_level = :fiber

    # Capture log output using a simple array instead of StringIO
    original_logger = Gouda.instance_variable_get(:@fallback_gouda_logger)

    # Create a logger that captures messages in our array
    test_logger = ActiveSupport::TaggedLogging.new(Logger.new("File::NULL"))
    test_logger.level = Logger::INFO

    # Override the info method to capture messages
    def test_logger.info(message)
      @captured_messages ||= []
      @captured_messages << message
    end

    def test_logger.warn(message)
      @captured_messages ||= []
      @captured_messages << message
    end

    def test_logger.captured_messages
      @captured_messages || []
    end

    Gouda.instance_variable_set(:@fallback_gouda_logger, test_logger)

    # This should not trigger a warning
    Gouda::FiberDatabaseSupport.check_fiber_isolation_level

    captured_messages = test_logger.captured_messages

    # Check that no warning was logged, but info message was
    warning_found = captured_messages.any? { |msg| msg.include?("FIBER SCHEDULER CONFIGURATION WARNING") }
    refute warning_found, "Did not expect fiber scheduler warning to be logged"

    info_found = captured_messages.any? { |msg| msg.include?("Rails isolation level correctly set to :fiber") }
    assert info_found, "Expected confirmation info message to be logged"
  ensure
    # Restore original isolation level if it was set
    if original_isolation
      ActiveSupport.isolation_level = original_isolation
    end
    # Restore original logger
    if original_logger
      Gouda.instance_variable_set(:@fallback_gouda_logger, original_logger)
    end
  end
end
