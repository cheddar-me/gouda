# frozen_string_literal: true

require "gouda/test_helper"
require "csv"

class GoudaTest < ActiveSupport::TestCase
  include AssertHelper

  setup do
    Thread.current[:gouda_test_side_effects] = []
    @adapter ||= Gouda::Adapter.new
    Gouda::Railtie.initializers.each(&:run)
  end

  class GoudaTestJob < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
  end

  class IncrementJob < GoudaTestJob
    def perform
      Thread.current[:gouda_test_side_effects] << "did-run"
    end
  end

  class StandardJob < GoudaTestJob
    def perform
      "perform result of #{self.class}"
    end
  end

  class JobThatAlwaysFails < GoudaTestJob
    def perform
      raise "Some drama"
    end
  end

  class JobWithEnqueueConcurrency < GoudaTestJob
    def perform
      "perform result of #{self.class}"
    end

    def enqueue_concurrency_key
      "conc"
    end
  end

  class JobWithEnqueueConcurrencyAndCursor < GoudaTestJob
    attr_accessor :cursor_position

    def perform
      "perform result of #{self.class}"
    end

    def enqueue_concurrency_key
      "conc1"
    end
  end

  class JobWithEnqueueConcurrencyViaGoudaAndEnqueueLimit < GoudaTestJob
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with(enqueue_limit: 1, key: -> { self.class.to_s })
    def perform
      "perform result of #{self.class}"
    end
  end

  class JobWithEnqueueConcurrencyViaGoudaAndTotalLimit < GoudaTestJob
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with(total_limit: 1, key: -> { self.class.to_s })
    def perform
      "perform result of #{self.class}"
    end
  end

  class JobWithExecutionConcurrency < GoudaTestJob
    def perform
      "perform result of #{self.class}"
    end

    def execution_concurrency_key
      "there-can-be-just-one"
    end
  end

  class JobWithExecutionConcurrencyViaGoudaAndTotalLimit < GoudaTestJob
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with(total_limit: 1, key: -> { self.class.to_s })
    def perform
      "perform result of #{self.class}"
    end
  end

  class JobWithExecutionConcurrencyViaGoudaAndPerformLimit < GoudaTestJob
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with(perform_limit: 1, key: -> { self.class.to_s })
    def perform
      "perform result of #{self.class}"
    end
  end

  class HeavyJob < GoudaTestJob
    queue_as :heavy

    def perform
      "Heavy work"
    end
  end

  class JobThatReenqueuesItself < GoudaTestJob
    def perform
      enqueue
      enqueue
      enqueue
      enqueue
    end
  end

  class JobThatRetriesItself < GoudaTestJob
    class Unfortunateness < StandardError
    end

    retry_on Unfortunateness, attempts: 5, wait: 0

    def perform
      raise Unfortunateness, "Tranquilo!"
    end
  end

  class JobWithEnqueueCallback < GoudaTestJob
    after_enqueue do |job|
      Thread.current[:gouda_test_side_effects] << "after-enq"
    end

    def perform
      "whatever"
    end
  end

  test "is able to enqueue jobs" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 12) do
      some_jobs = 10.times.map { StandardJob.new }
      @adapter.enqueue_all(some_jobs)
      @adapter.enqueue(StandardJob.new)
      @adapter.enqueue_at(StandardJob.new, _at_epoch = 1)
    end
  end

  test "reports jobs waiting to start" do
    assert_equal 0, Gouda::Workload.waiting_to_start.count
    StandardJob.perform_later
    assert_equal 1, Gouda::Workload.waiting_to_start.count

    StandardJob.set(wait: -200).perform_later
    assert_equal 2, Gouda::Workload.waiting_to_start.count

    StandardJob.set(wait: 200).perform_later
    assert_equal 2, Gouda::Workload.waiting_to_start.count

    StandardJob.set(queue: "another").perform_later
    assert_equal 2, Gouda::Workload.waiting_to_start(queue_constraint: Gouda::ExceptQueueConstraint.new(["another"])).count
  end

  test "skips jobs with the same enqueue concurrency key, both within the same bulk and separately" do
    some_jobs = 12.times.map { JobWithEnqueueConcurrency.new }
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      @adapter.enqueue_all(some_jobs)
    end
    enqueued, not_enqueued = some_jobs.partition(&:successfully_enqueued?)
    assert_equal 1, enqueued.length
    assert_nil enqueued.first.enqueue_error
    assert_kind_of ActiveJob::EnqueueError, not_enqueued.first.enqueue_error

    # Attempt to enqueue one more - now it won't be filtered out at insert_all but
    # instead by the DB
    one_more = JobWithEnqueueConcurrency.new
    @adapter.enqueue_all([one_more])
    refute_predicate one_more, :successfully_enqueued?
    assert_kind_of ActiveJob::EnqueueError, one_more.enqueue_error

    # Mark the first job as done and make sure the next one enqueues correctly
    # (the presence of a job which has executed should not make the unique constraint fire)
    Gouda::Workload.update_all(state: "finished")
    one_more_still = JobWithEnqueueConcurrency.new
    @adapter.enqueue_all([one_more_still])
    assert_predicate one_more_still, :successfully_enqueued?
  end

  test "allows 2 jobs with the same enqueue concurrency key if one of them comes from the scheduler" do
    scheduled_job = JobWithEnqueueConcurrency.new
    scheduled_job.scheduler_key = "sometime"

    immediate_job = JobWithEnqueueConcurrency.new
    another_immediate_job = JobWithEnqueueConcurrency.new

    another_scheduled_job = JobWithEnqueueConcurrency.new
    another_scheduled_job.scheduler_key = "sometime"

    yet_another_scheduled_job = JobWithEnqueueConcurrency.new
    yet_another_scheduled_job.scheduler_key = "some other time" # Different cron task, but still should not enqueue

    some_jobs = [immediate_job, another_immediate_job, scheduled_job, another_scheduled_job, yet_another_scheduled_job]
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 2) do
      @adapter.enqueue_all(some_jobs)
    end

    _enqueued, not_enqueued = some_jobs.partition(&:successfully_enqueued?)
    assert_equal 3, not_enqueued.length
    assert_same_set [another_immediate_job, another_scheduled_job, yet_another_scheduled_job], not_enqueued
  end

  test "allows multiple jobs with the same enqueue concurrency but different job-iteration cursor position" do
    first_job = JobWithEnqueueConcurrencyAndCursor.new
    second_job = JobWithEnqueueConcurrencyAndCursor.new
    second_job.cursor_position = "123"

    third_job_dupe = JobWithEnqueueConcurrencyAndCursor.new
    third_job_dupe.cursor_position = "123"

    fourth_job = JobWithEnqueueConcurrencyAndCursor.new
    fourth_job.cursor_position = "456"

    some_jobs = [first_job, second_job, third_job_dupe, fourth_job]
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 3) do
      @adapter.enqueue_all(some_jobs)
    end

    _enqueued, not_enqueued = some_jobs.partition(&:successfully_enqueued?)
    assert_same_set [third_job_dupe], not_enqueued
  end

  test "skips jobs with the same enqueue concurrency key enqueued over multiple bulks" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 2) do
      some_jobs = 12.times.map { JobWithEnqueueConcurrency.new } + [StandardJob.new]
      @adapter.enqueue_all(some_jobs)
    end

    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      some_jobs = 12.times.map { JobWithEnqueueConcurrency.new } + [StandardJob.new]
      @adapter.enqueue_all(some_jobs)
    end
  end

  test "skips jobs with the same enqueue concurrency key picked from Gouda settings" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 2) do
      some_jobs = 12.times.map { JobWithEnqueueConcurrencyViaGoudaAndTotalLimit.new } + [StandardJob.new]
      @adapter.enqueue_all(some_jobs)
    end

    assert_changes_by(-> { Gouda::Workload.count }, exactly: 2) do
      some_jobs = 12.times.map { JobWithEnqueueConcurrencyViaGoudaAndEnqueueLimit.new } + [StandardJob.new]
      @adapter.enqueue_all(some_jobs)
    end
  end

  test "executes a job" do
    job = StandardJob.new
    @adapter.enqueue_all([job])

    assert_equal 1, Gouda::Workload.where(state: "enqueued").count

    execution_result = Gouda::Workload.checkout_and_perform_one(executing_on: "Unit test")
    assert_equal "perform result of GoudaTest::StandardJob", execution_result

    assert_equal 1, Gouda::Workload.where(state: "finished").count

    execution_result = Gouda::Workload.checkout_and_perform_one(executing_on: "Unit test") # No more jobs to perform
    assert_nil execution_result
  end

  test "marks a job as failed if it raises an exception" do
    job = JobThatAlwaysFails.new
    @adapter.enqueue_all([job])

    assert_equal 1, Gouda::Workload.where(state: "enqueued").count
    result = Gouda::Workload.checkout_and_perform_one(executing_on: "Unit test")
    assert_kind_of StandardError, result

    assert_equal 1, Gouda::Workload.where(state: "finished").count
    execution_result = Gouda::Workload.checkout_and_perform_one(executing_on: "Unit test") # No more jobs to perform
    assert_nil execution_result
  end

  test "executes enqueue callbacks" do
    Gouda.in_bulk do
      3.times { JobWithEnqueueCallback.perform_later }
    end
    assert_equal ["after-enq", "after-enq", "after-enq"], Thread.current[:gouda_test_side_effects]
  end

  test "does not execute multiple jobs with the same execution concurrency key" do
    @adapter.enqueue_all([JobWithExecutionConcurrency.new, JobWithExecutionConcurrency.new])

    assert_equal 2, Gouda::Workload.where(state: "enqueued").count

    first_worker = Fiber.new do
      loop do
        gouda_or_nil = Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
        Fiber.yield(gouda_or_nil)
      end
    end

    second_worker = Fiber.new do
      loop do
        gouda_or_nil = Gouda::Workload.checkout_and_lock_one(executing_on: "worker2")
        Fiber.yield(gouda_or_nil)
      end
    end

    first_job = first_worker.resume
    second_job = second_worker.resume
    assert_kind_of Gouda::Workload, first_job
    assert_nil second_job

    first_job.perform_and_update_state!
    second_job = second_worker.resume
    assert_kind_of Gouda::Workload, second_job

    assert_nil first_worker.resume
    assert_nil second_worker.resume
  end

  test "extracts execution concurrency key using Gouda configuration" do
    @adapter.enqueue_all([JobWithExecutionConcurrencyViaGoudaAndPerformLimit.new, JobWithExecutionConcurrencyViaGoudaAndTotalLimit.new])

    expected_keys = [
      "GoudaTest::JobWithExecutionConcurrencyViaGoudaAndPerformLimit",
      "GoudaTest::JobWithExecutionConcurrencyViaGoudaAndTotalLimit"
    ]
    assert_same_set expected_keys, Gouda::Workload.pluck(:execution_concurrency_key)
  end

  test "enqueues a job with a set scheduled_at" do
    job = StandardJob.new
    job.scheduled_at = Time.now.utc + 2
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      @adapter.enqueue_all([job])
    end

    # That job is delayed and will not SELECT now
    assert_nil Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
    sleep 10
    assert_kind_of Gouda::Workload, Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
  end

  test "enqueues a job with a `wait` via enqueue_at" do
    job = StandardJob.new
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      @adapter.enqueue_at(job, _epoch = (Time.now.to_i + 2))
    end

    # That job is delayed and will not SELECT now
    assert_nil Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
    sleep 10
    assert_kind_of Gouda::Workload, Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
  end

  test "is able to perform_later without a wait:" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      StandardJob.perform_later
    end
    maybe_job = Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
    assert_kind_of Gouda::Workload, maybe_job
  end

  test "is able to perform_later with wait:" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      StandardJob.set(wait: 4).perform_later
    end
    # The job will not be selected as it has to wait
    assert_nil Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
  end

  test "is able to bulk-enqueue" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 11) do
      Gouda.in_bulk do
        4.times { StandardJob.perform_later }
        Gouda.in_bulk do
          4.times { StandardJob.perform_later }
          Gouda.in_bulk do
            3.times { StandardJob.perform_later }
          end
        end
      end
    end
  end

  test "sets the correct queue with perform_later" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 1) do
      HeavyJob.perform_later
    end
    gouda = Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
    assert_equal "heavy", gouda.queue_name
  end

  test "selects jobs in priority order" do
    priorities_ascending = [0, 4, 10, 25]
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 30) do
      jobs_with_priorities = 30.times.map do
        StandardJob.new.tap do |j|
          j.priority = priorities_ascending.sample(random: case_random)
        end
      end
      @adapter.enqueue_all(jobs_with_priorities)
    end

    checked_out_priorities = []
    30.times do
      gouda = Gouda::Workload.checkout_and_lock_one(executing_on: "worker1")
      checked_out_priorities << gouda.priority
    end
    assert_equal priorities_ascending, checked_out_priorities.uniq
  end

  test "is able to perform a job which re-enqueues itself" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 5) do
      JobThatReenqueuesItself.perform_later
      Gouda::Workload.checkout_and_perform_one(executing_on: "Unit test")
    end

    assert_equal 4, Gouda::Workload.where(state: "enqueued").count
    assert_equal 1, Gouda::Workload.where(state: "finished").count
    assert_equal 1, Gouda::Workload.select("DISTINCT active_job_id").count
  end

  test "is able to perform a job which retries itself" do
    assert_changes_by(-> { Gouda::Workload.count }, exactly: 5) do
      JobThatRetriesItself.perform_later
      5.times do
        Gouda::Workload.checkout_and_perform_one(executing_on: "Unit test")
      end
    end

    assert_equal 5, Gouda::Workload.where(state: "finished").count
  end

  test "saves the time of interruption upon enqueue when the interruption timestamp ivar is set" do
    freeze_time
    t = Time.now.utc
    job = StandardJob.new
    job.interrupted_at = t
    job.enqueue

    last_inserted_workload = Gouda::Workload.last
    assert_equal last_inserted_workload.interrupted_at, t
  end

  test "raises in interrupt error when the the interruption timestamp ivar is set" do
    freeze_time
    t = Time.now.utc
    job = StandardJob.new
    job.interrupted_at = t

    assert_raises Gouda::InterruptError do
      job.perform_now
    end
  end

  test "selects jobs according to the queue constraint" do
    queues = %w[foo bar baz]
    20.times do
      StandardJob.set(queue: queues.sample(random: case_random)).perform_later
    end

    only_bad = Gouda::OnlyQueuesConstraint.new(["bad"])
    assert_nil Gouda::Workload.checkout_and_lock_one(executing_on: "worker1", queue_constraint: only_bad)

    only_foo = Gouda::OnlyQueuesConstraint.new(["foo"])
    selected_job = Gouda::Workload.checkout_and_lock_one(executing_on: "worker1", queue_constraint: only_foo)
    assert_equal "foo", selected_job.queue_name

    only_foo_and_baz = Gouda::OnlyQueuesConstraint.new(["foo", "baz"])
    selected_job = Gouda::Workload.checkout_and_lock_one(executing_on: "worker1", queue_constraint: only_foo_and_baz)
    assert ["foo", "baz"].include?(selected_job.queue_name)

    except_foo_and_baz = Gouda::ExceptQueueConstraint.new(["foo", "baz"])
    selected_job = Gouda::Workload.checkout_and_lock_one(executing_on: "worker1", queue_constraint: except_foo_and_baz)
    assert_equal "bar", selected_job.queue_name
  end

  test "has reasonable performance with a pre-seeded queue" do
    # slow_test!

    Gouda.in_bulk do
      with_sample_job_delays(n_jobs: 100_000) do |delay|
        StandardJob.set(wait: delay).perform_later
      end
    end
  end

  test "prunes the finished workloads from the table" do
    Gouda.in_bulk do
      250.times do
        StandardJob.perform_later
      end
    end
    Gouda.config.preserve_job_records = false
    Gouda::Workload.update_all(execution_finished_at: 3.days.ago, state: "finished")

    Gouda.in_bulk do
      2.times do
        StandardJob.perform_later
      end
    end

    Gouda::Workload.prune
    assert_equal 2, Gouda::Workload.count
  end

  test "sets the error column on a job that fails" do
    JobThatRetriesItself.perform_later
    Gouda::Workload.checkout_and_perform_one(executing_on: "Unit test")

    errored = Gouda::Workload.where.not(error: {}).first
    assert_equal errored.error["message"], "Tranquilo!"
    assert_equal errored.error["class_name"], "GoudaTest::JobThatRetriesItself::Unfortunateness"
    refute_empty errored.error["backtrace"]
  end

  test "has reasonable performance" do
    skip "This test is very intense (and slow!)"

    Dir.mktmpdir do |tmpdir_path|
      n_bulks = 100
      n_workers = 15
      bulk_size = 1000

      # Fill up the queue with data
      rng = Random.new
      n_bulks.times do |n|
        active_jobs = bulk_size.times.map do
          StandardJob.new.tap do |job|
            job.scheduled_at = Time.now.utc + rng.rand(3.0)
          end
        end
        @adapter.enqueue_all(active_jobs)
      end

      # Running the processing in threads is not very conductive to performance
      # measurement as there is the GIL to contend with, and this test gets real busy.
      # let's use processes instead.
      consumer_pids = n_workers.times.map do |n|
        fork do
          File.open(File.join(tmpdir_path, "bj_test_stat_#{n}.bin"), "w") do |f|
            ActiveRecord::Base.connection.execute("SET statement_timeout TO '30s'")
            loop do
              t = Process.clock_gettime(Process::CLOCK_MONOTONIC)

              did_perform = Gouda::Workload.checkout_and_perform_one(executing_on: "perf")
              break unless did_perform

              dt = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t
              f.puts(dt.to_s)
            end
          end
        end
      end

      # Let the cosumers run for 5 seconds
      sleep 5

      # Then terminate them
      consumer_pids.each do |pid|
        Process.kill("TERM", pid)
        Process.wait(pid)
      end
      # and collect stats
      times_to_consume = Dir.glob(File.join(tmpdir_path, "bj_test_stat_*.*")).flat_map do |path|
        values = File.read(path).split("\n").map { |sample| sample.to_f }
        values.tap { File.unlink(path) }
      end

      warn "Take+perform with #{n_workers} workers and #{n_bulks * bulk_size} jobs in queue: #{sample_description(times_to_consume)}"
      assert_equal n_bulks * bulk_size, Gouda::Workload.count
    end
  end

  test "actually executes workloads" do
    4.times { StandardJob.perform_later }
    2.times { JobThatAlwaysFails.perform_later }
    4.times { JobWithExecutionConcurrency.perform_later }
    6.times { JobWithEnqueueConcurrency.perform_later }

    Gouda.worker_loop(n_threads: 3, check_shutdown: Gouda::EmptyQueueShutdownCheck.new)
    assert_equal 11, Gouda::Workload.where(state: "finished").count
  end

  test "parses queue constraints" do
    constraint = Gouda.parse_queue_constraint("*")
    assert_equal Gouda::AnyQueue, constraint

    constraint = Gouda.parse_queue_constraint("-heavy")
    assert_kind_of Gouda::ExceptQueueConstraint, constraint
    assert_equal "queue_name NOT IN ('heavy')", constraint.to_sql.strip

    constraint = Gouda.parse_queue_constraint("foo,bar")
    assert_kind_of Gouda::OnlyQueuesConstraint, constraint
    assert_equal "queue_name IN ('foo','bar')", constraint.to_sql.strip
  end

  test "checkout and performs from the right queue only" do
    heavy_constraint = Gouda.parse_queue_constraint("heavy")
    except_heavy_constraint = Gouda.parse_queue_constraint("-heavy")
    heavy = HeavyJob.perform_later
    light = StandardJob.perform_later

    assert_same_set Gouda::Workload.enqueued.pluck(:active_job_id), [heavy.job_id, light.job_id]
    assert_changes -> { Gouda::Workload.finished.count }, to: 1 do
      Gouda::Workload.checkout_and_perform_one(executing_on: "test", queue_constraint: heavy_constraint)
    end
    assert_no_changes -> { Gouda::Workload.finished.count } do
      Gouda::Workload.checkout_and_perform_one(executing_on: "test", queue_constraint: heavy_constraint)
    end

    assert_equal 1, Gouda::Workload.finished.count
    assert_equal heavy.job_id, Gouda::Workload.finished.first.active_job_id

    heavy2 = HeavyJob.perform_later
    assert_same_set Gouda::Workload.enqueued.pluck(:active_job_id), [heavy2.job_id, light.job_id]

    assert_changes -> { Gouda::Workload.finished.count }, to: 2 do
      Gouda::Workload.checkout_and_perform_one(executing_on: "test", queue_constraint: except_heavy_constraint)
    end
    assert_no_changes -> { Gouda::Workload.finished.count } do
      Gouda::Workload.checkout_and_perform_one(executing_on: "test", queue_constraint: except_heavy_constraint)
    end

    assert_equal 2, Gouda::Workload.finished.count
    assert_same_set [heavy.job_id, light.job_id], Gouda::Workload.finished.pluck(:active_job_id)
  end

  test "picks up fused jobs, but marks them as finished right away without execuing the job code" do
    IncrementJob.perform_later

    assert_changes -> { Gouda::Workload.finished.count }, to: 1 do
      Gouda::Workload.checkout_and_perform_one(executing_on: "test")
    end

    Gouda::JobFuse.create!(active_job_class_name: "GoudaTest::IncrementJob")
    IncrementJob.perform_later

    assert_changes -> { Gouda::Workload.finished.count }, to: 2 do
      Gouda::Workload.checkout_and_perform_one(executing_on: "test")
    end

    assert_equal ["did-run"], Thread.current[:gouda_test_side_effects]
  end

  def sample_description(sample)
    values = sample.map(&:to_f).sort

    max = values.last #=> 9.0
    n = values.size # => 9
    values.map!(&:to_f) # => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    mean = values.reduce(&:+) / n # => 5.0
    sum_sqr = values.map { |x| x * x }.reduce(&:+) # => 285.0
    std_dev = Math.sqrt((sum_sqr - n * mean * mean) / (n - 1)) # => 2.7386127875258306

    percentile = ->(fraction) {
      if values.length > 1
        k = (fraction * (values.length - 1) + 1).floor - 1
        f = (fraction * (values.length - 1) + 1).modulo(1)
        values[k] + (f * (values[k + 1] - values[k]))
      else
        values.first
      end
    }
    p90 = percentile.call(0.9)
    p95 = percentile.call(0.95)
    p99 = percentile.call(0.99)

    "sample_size: %d, mean: %0.3f, max: %0.3f, p90: %0.3f, p95: %0.3f, p99: %0.3f, stddev: %0.3f" % [sample.length, mean, max, p90, p95, p99, std_dev]
  end

  def with_sample_job_delays(n_jobs:)
    rows = CSV.read(File.dirname(__FILE__) + "/seconds_to_start_distribution.csv")
    rows.shift
    count_to_delay_seconds = rows.map { |(a, b)| [a.to_i, b.to_i] }
    total_records = count_to_delay_seconds.map(&:first).sum
    scaling_factor = n_jobs / total_records.to_f
    count_to_delay_seconds.each do |(initial_count, delay_seconds)|
      (scaling_factor * initial_count).round.times do
        yield(delay_seconds)
      end
    end
  end
end
