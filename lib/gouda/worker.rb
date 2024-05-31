# frozen_string_literal: true

require "securerandom"

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
      # return false unless Rails.application # Rails is still booting and there is no application defined

      Gouda.app_executor.wrap do
        Gouda::Workload.waiting_to_start(queue_constraint: @queue_constraint).none?
      end
    rescue # If the DB connection cannot be checked out etc
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

  # Start looping, taking work from the queue and performing it, over multiple worker threads.
  # Once the `check_shutdown` callable returns `true` the threads will cleanly terminate and the method will return (so it is blocking).
  #
  # @param n_threads[Integer] how many _worker_ threads to start. Another thread will be started for housekeeping, so ideally this should be the size of your connection pool minus 1
  # @param check_shutdown[#call] A callable object (can be a Proc etc.). Once starts returning `true` the worker threads and the housekeeping thread will cleanly exit
  def self.worker_loop(n_threads:, check_shutdown: TrapShutdownCheck.new, queue_constraint: Gouda::AnyQueue)
    # We need quite a few things when starting the loop - we have to be far enough into the Rails bootup sequence
    # that both the application and the executor are available
    #
    # raise "Rails is not loaded yet" unless defined?(Rails) && Rails.respond_to?(:application)
    # raise "Rails application is not loaded yet" unless Rails.application
    # raise "Rails executor not available yet" unless Rails.application.executor

    check_shutdown = CombinedShutdownCheck.new(*check_shutdown) if !check_shutdown.respond_to?(:call) && check_shutdown.is_a?(Array)

    worker_id = [Socket.gethostname, Process.pid, SecureRandom.uuid].join("-")
    executing_workload_ids = ThreadSafeSet.new

    raise ArgumentError, "You need at least 1 worker thread, but you requested #{n_threads}" if n_threads < 1
    worker_threads = n_threads.times.map do
      Thread.new do
        worker_id_and_thread_id = [worker_id, "t0x#{Thread.current.object_id.to_s(16)}"].join("-")
        loop do
          break if check_shutdown.call

          did_process = Gouda.app_executor.wrap do
            Gouda::Workload.checkout_and_perform_one(executing_on: worker_id_and_thread_id, queue_constraint:, in_progress: executing_workload_ids)
          end

          # If no job was retrieved the queue is likely empty. Relax the polling then and ease off.
          # If a job was retrieved it is likely that a burst has just been enqueued, and we do not
          # sleep but proceed to attempt to retrieve the next job right after.
          jitter_sleep_interval = POLL_INTERVAL_DURATION_SECONDS + (POLL_INTERVAL_DURATION_SECONDS * 0.25)
          sleep_with_interruptions(jitter_sleep_interval, check_shutdown) unless did_process
        rescue => e
          warn "Uncaught exception during perform (#{e.class} - #{e}"
        end
      end
    end

    # Do the housekeeping tasks on main
    loop do
      break if check_shutdown.call

      Gouda.app_executor.wrap do
        # Mark known executing jobs as such. If a worker process is killed or the machine it is running on dies,
        # a stale timestamp can indicate to us that the job was orphaned and is marked as "executing"
        # even though the worker it was running on has failed for whatever reason.
        # Later on we can figure out what to do with those jobs (re-enqueue them or toss them)
        Gouda::Workload.where(id: executing_workload_ids.to_a, state: "executing").update_all(executing_on: worker_id, last_execution_heartbeat_at: Time.now.utc)

        # Find jobs which just hung and clean them up (mark them as "finished" and enqueue replacement workloads if possible)
        Gouda::Workload.reap_zombie_workloads
      rescue => e
        # Appsignal.add_exception(e)
        warn "Uncaught exception during housekeeping (#{e.class} - #{e}"
      end

      # Jitter the sleep so that the workers booted at the same time do not all dogpile
      randomized_sleep_duration_s = POLL_INTERVAL_DURATION_SECONDS + (POLL_INTERVAL_DURATION_SECONDS.to_f * rand)
      sleep_with_interruptions(randomized_sleep_duration_s, check_shutdown)
    end
  ensure
    worker_threads&.map(&:join)
  end

  def self.sleep_with_interruptions(n_seconds, must_abort_proc)
    start_time_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    # remaining_seconds = n_seconds
    check_interval_seconds = Gouda.polling_sleep_interval_seconds
    loop do
      return if must_abort_proc.call
      return if Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time_seconds >= n_seconds
      sleep(check_interval_seconds)
    end
  end
end
