# frozen_string_literal: true

require "securerandom"
require "async"
require "async/scheduler"
require "gouda/version"

module Gouda
  # Fiber-based worker implementation for non-blocking IO
  # This replaces the thread-based worker with a fiber scheduler approach
  class FiberWorker
    POLL_INTERVAL_DURATION_SECONDS = 1

    # Fiber-safe set for tracking executing workload IDs
    class FiberSafeSet
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

    # Start the fiber-based worker loop using Ruby's fiber scheduler
    # This replaces the traditional thread-based approach
    def self.worker_loop(n_fibers:, check_shutdown: Gouda::TrapShutdownCheck.new, queue_constraint: Gouda::AnyQueue)
      check_shutdown = Gouda::CombinedShutdownCheck.new(*check_shutdown) if !check_shutdown.respond_to?(:call) && check_shutdown.is_a?(Array)

      worker_id = [Socket.gethostname, Process.pid, SecureRandom.uuid].join("-")
      executing_workload_ids = FiberSafeSet.new

      raise ArgumentError, "You need at least 1 worker fiber, but you requested #{n_fibers}" if n_fibers < 1

      # Use Ruby's async gem with fiber scheduler for non-blocking IO
      Async do |task|
        # Create worker fibers instead of threads
        worker_tasks = n_fibers.times.map do |i|
          task.async do |worker_task|
            worker_id_and_fiber_id = [worker_id, "f0x#{Fiber.current.object_id.to_s(16)}"].join("-")

            loop do
              break if check_shutdown.call

              begin
                did_process = Gouda.config.app_executor.wrap do
                  Gouda::Workload.checkout_and_perform_one(
                    executing_on: worker_id_and_fiber_id,
                    queue_constraint: queue_constraint,
                    in_progress: executing_workload_ids
                  )
                end

                # If no job was retrieved, sleep with fiber scheduler
                unless did_process
                  jitter_sleep_interval = POLL_INTERVAL_DURATION_SECONDS + (POLL_INTERVAL_DURATION_SECONDS * 0.25)
                  fiber_sleep_with_interruptions(jitter_sleep_interval, check_shutdown, worker_task)
                end
              rescue => e
                Gouda.logger.warn "Uncaught exception during perform (#{e.class} - #{e})"
              end
            end
          end
        end

        # Housekeeping task (replaces housekeeping on main thread)
        housekeeping_task = task.async do |housekeeping_task_ctx|
          loop do
            break if check_shutdown.call

            begin
              Gouda.config.app_executor.wrap do
                # Update heartbeats for executing jobs
                Gouda.suppressing_sql_logs do
                  Gouda::Workload.where(id: executing_workload_ids.to_a, state: "executing")
                    .update_all(executing_on: worker_id, last_execution_heartbeat_at: Time.now.utc)
                end

                # Clean up zombie workloads
                Gouda::Workload.reap_zombie_workloads
              end
            rescue => e
              Gouda.instrument(:exception, {exception: e})
              Gouda.logger.warn "Uncaught exception during housekeeping (#{e.class} - #{e})"
            end

            # Fiber-based sleep with jitter
            randomized_sleep_duration_s = POLL_INTERVAL_DURATION_SECONDS + (POLL_INTERVAL_DURATION_SECONDS.to_f * rand)
            fiber_sleep_with_interruptions(randomized_sleep_duration_s, check_shutdown, housekeeping_task_ctx)
          end
        end

        # Wait for all tasks to complete
        [*worker_tasks, housekeeping_task].each(&:wait)
      end
    end

    # Fiber-based sleep that yields control to the scheduler
    def self.fiber_sleep_with_interruptions(n_seconds, must_abort_proc, task = nil)
      start_time_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      check_interval_seconds = Gouda.config.polling_sleep_interval_seconds

      loop do
        return if must_abort_proc.call
        return if Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time_seconds >= n_seconds

        # Use task.sleep for non-blocking sleep that yields to scheduler
        if task
          task.sleep(check_interval_seconds)
        else
          # Fallback to regular sleep if no task context available
          sleep(check_interval_seconds)
        end
      end
    end
  end
end
