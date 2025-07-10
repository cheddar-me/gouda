# frozen_string_literal: true

require "securerandom"
require "gouda/version"

module Gouda
  POLL_INTERVAL_DURATION_SECONDS = 1

  # Is used for keeping the IDs of currently executing jobs on this worker in a thread-safe way.
  # These IDs are used to update the heartbeat timestamps during execution. We need just three
  # methods here - add to a set, remove from a set, and convert the set into an array for a SQL query
  # with `WHERE id IN`.
  class ThreadSafeSet
    def initialize
      @set = Set.new
      @mutex = Mutex.new
    end

    def add(value)
      @mutex.synchronize { @set.add(value) }
      value
    end

    def delete(value)
      @mutex.synchronize { @set.delete(value) }
      value
    end

    def to_a
      @mutex.synchronize { @set.to_a }
    end
  end

  # Returns `true` once a given timer has elapsed.
  # This is useful to terminate a worker after a certain amount of time
  class TimerShutdownCheck
    def initialize(seconds_float)
      @dt = seconds_float
      @st = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def call
      (Process.clock_gettime(Process::CLOCK_MONOTONIC) - @st) > @dt
    end
  end

  # Captures UNIX signals (TERM and INT) and then returns true. Once you initialize the
  # this check you install signal handlers, meaning that the worker will not raise `Interrupt`
  # from any theads but will get the space it needs to terminate cleanly. At least for SIGINT
  # and SIGTERM this is very desirable. This is the default shutdown check.
  class TrapShutdownCheck
    def initialize
      @did_trap = false
      @did_log = false
      Signal.trap(:TERM) do
        @did_trap = :TERM
      end
      Signal.trap(:INT) do
        @did_trap = :INT
      end
    end

    def call
      if @did_trap
        @did_log ||= begin
          warn("Gouda worker signaled to terminate via SIG#{@did_trap}")
          true
        end
        true
      else
        false
      end
    end
  end

  # This shutdown check will return `true` once there
  # are no enqueued jobs to process for this worker. This
  # can be used to run a worker just as long as there are jobs to handle
  # and then to let it quit by itself (handy for spot instances and the like)
  class EmptyQueueShutdownCheck
    def initialize(queue_constraint = Gouda::AnyQueue)
      @queue_constraint = queue_constraint
    end

    def call
      Gouda.config.app_executor.wrap do
        Gouda.suppressing_sql_logs { Gouda::Workload.waiting_to_start(queue_constraint: @queue_constraint).none? }
      end
    rescue
      # It is possible that in this scenario we do not have a database set up yet, for example,
      # or we are unable to connect to the DB for whatever reason. In that case we should
      # return `false` so that the worker can poll again later.
      false
    end
  end

  # A wrapping callable which returns `true` if any of the
  # given callables return true. This can be used to combine a timed shutdown ("in 30 seconds")
  # with a signal handler shutdown ("shutdown on SIGTERM/SIGINT")
  class CombinedShutdownCheck
    # @param callables_for_condition[#call] other shutdown checks
    def initialize(*callables_for_condition)
      @conditions = callables_for_condition
    end

    def call
      # Once one shutdown check told us to shut down there is no point to query all the others
      @memo ||= @conditions.any?(&:call)
    end
  end

  # Worker class that supports both threaded and hybrid (threads + fibers) execution modes
  class Worker
    # Start looping, taking work from the queue and performing it, over multiple worker threads.
    # Once the `check_shutdown` callable returns `true` the threads will cleanly terminate and the method will return (so it is blocking).
    #
    # @param n_threads[Integer] how many _worker_ threads to start. Another thread will be started for housekeeping, so ideally this should be the size of your connection pool minus 1
    # @param check_shutdown[#call] A callable object (can be a Proc etc.). Once starts returning `true` the worker threads and the housekeeping thread will cleanly exit
    # @param use_fibers[Boolean] whether to use fibers within each thread for higher concurrency
    # @param fibers_per_thread[Integer] how many fibers to run per thread (only used if use_fibers is true)
    def self.worker_loop(n_threads:, check_shutdown: TrapShutdownCheck.new, queue_constraint: Gouda::AnyQueue, use_fibers: false, fibers_per_thread: 1)
      check_shutdown = CombinedShutdownCheck.new(*check_shutdown) if !check_shutdown.respond_to?(:call) && check_shutdown.is_a?(Array)

      log_worker_configuration(n_threads, use_fibers, fibers_per_thread)
      setup_fiber_environment if use_fibers

      worker_id = generate_worker_id
      executing_workload_ids = ThreadSafeSet.new

      raise ArgumentError, "You need at least 1 worker thread, but you requested #{n_threads}" if n_threads < 1

      worker_threads = if use_fibers
        create_hybrid_worker_threads(n_threads, fibers_per_thread, worker_id, queue_constraint, executing_workload_ids, check_shutdown)
      else
        create_threaded_worker_threads(n_threads, worker_id, queue_constraint, executing_workload_ids, check_shutdown)
      end

      run_housekeeping_loop(worker_id, executing_workload_ids, check_shutdown)
    ensure
      worker_threads&.map(&:join)
    end

    class << self
      private

      def log_worker_configuration(n_threads, use_fibers, fibers_per_thread)
        if use_fibers
          Gouda.logger.info("Using hybrid scheduler (threads + fibers)")
          Gouda.logger.info("Worker threads: #{n_threads}")
          Gouda.logger.info("Fibers per thread: #{fibers_per_thread}")
          Gouda.logger.info("Total concurrency: #{n_threads * fibers_per_thread}")
        else
          Gouda.logger.info("Using thread-based scheduler")
          Gouda.logger.info("Worker threads: #{n_threads}")
        end
      end

      def setup_fiber_environment
        # Check Rails isolation level configuration
        Gouda::FiberDatabaseSupport.check_fiber_isolation_level
      end

      def generate_worker_id
        [Socket.gethostname, Process.pid, SecureRandom.uuid].join("-")
      end

      def create_hybrid_worker_threads(n_threads, fibers_per_thread, worker_id, queue_constraint, executing_workload_ids, check_shutdown)
        n_threads.times.map do |thread_index|
          Thread.new do
            # Load the Async gem here to avoid dependency issues when not using fibers
            require "async"
            require "async/scheduler"

            # Each thread runs its own fiber scheduler
            Async do |task|
              # Create multiple fibers within this thread
              fiber_tasks = fibers_per_thread.times.map do |fiber_index|
                task.async do |worker_task|
                  worker_id_and_fiber_id = generate_execution_id(worker_id, thread: true, fiber: true)

                  run_worker_loop(worker_id_and_fiber_id, queue_constraint, executing_workload_ids, check_shutdown, worker_task)
                end
              end

              # Wait for all fibers in this thread to complete
              fiber_tasks.each(&:wait)
            end
          end
        end
      end

      def create_threaded_worker_threads(n_threads, worker_id, queue_constraint, executing_workload_ids, check_shutdown)
        n_threads.times.map do
          Thread.new do
            worker_id_and_thread_id = generate_execution_id(worker_id, thread: true, fiber: false)
            run_worker_loop(worker_id_and_thread_id, queue_constraint, executing_workload_ids, check_shutdown)
          end
        end
      end

      def generate_execution_id(worker_id, thread: false, fiber: false)
        parts = [worker_id]
        parts << "thread-#{Thread.current.object_id.to_s(16)}" if thread
        parts << "fiber-#{Fiber.current.object_id.to_s(16)}" if fiber
        parts.join("-")
      end

      def run_worker_loop(execution_id, queue_constraint, executing_workload_ids, check_shutdown, fiber_task = nil)
        loop do
          break if check_shutdown.call

          begin
            did_process = Gouda.config.app_executor.wrap do
              Gouda::Workload.checkout_and_perform_one(
                executing_on: execution_id,
                queue_constraint: queue_constraint,
                in_progress: executing_workload_ids
              )
            end

            # If no job was retrieved, sleep with appropriate scheduler
            unless did_process
              jitter_sleep_interval = POLL_INTERVAL_DURATION_SECONDS + (POLL_INTERVAL_DURATION_SECONDS * 0.25)
              sleep_with_interruptions(jitter_sleep_interval, check_shutdown, fiber_task)
            end
          rescue => e
            Gouda.logger.warn "Uncaught exception during perform (#{e.class} - #{e})"
          end
        end
      end

      def run_housekeeping_loop(worker_id, executing_workload_ids, check_shutdown)
        loop do
          break if check_shutdown.call

          begin
            Gouda.config.app_executor.wrap do
              # Mark known executing jobs as such. If a worker process is killed or the machine it is running on dies,
              # a stale timestamp can indicate to us that the job was orphaned and is marked as "executing"
              # even though the worker it was running on has failed for whatever reason.
              # Later on we can figure out what to do with those jobs (re-enqueue them or toss them)
              Gouda.suppressing_sql_logs do # these updates will also be very frequent with long-running jobs
                Gouda::Workload.where(id: executing_workload_ids.to_a, state: "executing").update_all(executing_on: worker_id, last_execution_heartbeat_at: Time.now.utc)
              end

              # Find jobs which just hung and clean them up (mark them as "finished" and enqueue replacement workloads if possible)
              Gouda::Workload.reap_zombie_workloads
            end
          rescue => e
            Gouda.instrument(:exception, {exception: e})
            Gouda.logger.warn "Uncaught exception during housekeeping (#{e.class} - #{e})"
          end

          # Jitter the sleep so that the workers booted at the same time do not all dogpile
          randomized_sleep_duration_s = POLL_INTERVAL_DURATION_SECONDS + (POLL_INTERVAL_DURATION_SECONDS.to_f * rand)
          sleep_with_interruptions(randomized_sleep_duration_s, check_shutdown)
        end
      end

      # Unified sleep method that works with both threads and fibers
      def sleep_with_interruptions(n_seconds, must_abort_proc, fiber_task = nil)
        start_time_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        check_interval_seconds = Gouda.config.polling_sleep_interval_seconds

        loop do
          return if must_abort_proc.call
          return if Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time_seconds >= n_seconds

          if fiber_task
            # Use fiber-based sleep that yields control to the scheduler
            fiber_task.sleep(check_interval_seconds)
          else
            # Use regular thread sleep
            sleep(check_interval_seconds)
          end
        end
      end
    end
  end

  # Module-level convenience method that delegates to Worker.worker_loop
  def self.worker_loop(n_threads:, check_shutdown: TrapShutdownCheck.new, queue_constraint: Gouda::AnyQueue, use_fibers: false, fibers_per_thread: 1)
    Worker.worker_loop(
      n_threads: n_threads,
      check_shutdown: check_shutdown,
      queue_constraint: queue_constraint,
      use_fibers: use_fibers,
      fibers_per_thread: fibers_per_thread
    )
  end
end
