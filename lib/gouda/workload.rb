# # frozen_string_literal: true

# This model is called "workload" for a reason. The ActiveJob can be enqueued multiple times with
# the same job ID which gets generated by Rails. These multiple enqueues of the same job are not
# exactly copies of one another. When you use job-iteration for example, your job will be retried with a different
# cursor position value. When you use ActiveJob `rescue_from` as well - the job will be retried and keep the same
# active job ID, but it then gets returned into the queue "in some way". What we want is that the records in our
# table represent a unit of work that the worker has to execute "at some point". If the same job gets enqueued multiple
# times due to retries or pause/resume we want the enqueues to be separate workloads, which can fail or succeed
# independently. This also allows the queue records to be "append-only" which allows the records to be pruned
# on a regular basis. This is why they are called "workloads" and not "jobs". "Executions" is a great term used
# by good_job but it seems that it is not clear what has the "identity". With the Workload the ID of the workload
# is the "provider ID" for ActiveJob. It is therefore possible (and likely) that multiple Workloads will exist
# sharing the same ActiveJob ID.
class Gouda::Workload < ActiveRecord::Base
  ZOMBIE_MAX_THRESHOLD = "5 minutes"

  self.table_name = "gouda_workloads"

  # GoodJob calls these "enqueued" but they are more like
  # "waiting to start" - jobs which have been scheduled past now,
  # or haven't been scheduled to a particular time, are in the "enqueued"
  # state and match the queue constraint. Jobs which have fuses set are
  # excluded from this scope, as well as jobs which have been scheduled
  # but are no longer present in the schedule.
  # The scope can be used to call `count()` to obtain the queue depth, which can
  # be employed as a driving metric for autoscaling.
  scope :waiting_to_start, ->(queue_constraint: Gouda::AnyQueue) {
    known_scheduler_keys = [nil] + Gouda::Scheduler.known_scheduler_keys.to_a
    known_scheduler_keys_condition = known_scheduler_keys.map {|key| connection.quote(key) }.join(", ")
    condition_for_ready_to_execute_jobs = <<~SQL
      #{queue_constraint.to_sql}
      AND execution_concurrency_key NOT IN (
        SELECT execution_concurrency_key FROM #{quoted_table_name} WHERE state = 'executing' AND execution_concurrency_key IS NOT NULL
      )
      AND active_job_class_name NOT IN (
        SELECT active_job_class_name FROM gouda_job_fuses
      )
      AND (scheduler_key IS NULL OR scheduler_key IN (#{known_scheduler_keys_condition}))
      AND state = 'enqueued'
      AND (scheduled_at <= clock_timestamp())
    SQL
    where(Arel.sql(condition_for_ready_to_execute_jobs))
  }

  scope :errored, -> { where("error != '{}'") }
  scope :retried, -> { where("(serialized_params -> 'exception_executions') != '{}' AND state != 'finished'") }
  scope :finished, -> { where(state: "finished") }
  scope :enqueued, -> { where(state: "enqueued") }
  scope :executing, -> { where(state: "executing") }

  def self.queue_names
    connection.select_values("SELECT DISTINCT(queue_name) FROM #{quoted_table_name} ORDER BY queue_name ASC")
  end

  def self.prune
    if Gouda.config.preserve_job_records
      where(state: "finished").where("execution_finished_at < ?", Gouda.config.cleanup_preserved_jobs_before.ago).delete_all
    else
      where(state: "finished").delete_all
    end
  end

  # Re-enqueue zombie workloads which have been left to rot due to machine kills, worker OOM kills and the like
  # With a lock so no single zombie job gets enqueued more than once
  # And wrapped in transactions with the possibility to roll back a single workload without it rollbacking the entire batch
  def self.reap_zombie_workloads
    uncached do # again needed due to the use of clock_timestamp() in the SQL
      transaction do
        zombie_workloads_scope = Gouda::Workload.lock("FOR UPDATE SKIP LOCKED").where("state = 'executing' AND last_execution_heartbeat_at < (clock_timestamp() - interval '#{ZOMBIE_MAX_THRESHOLD}')")
        zombie_workloads_scope.find_each(batch_size: 1000) do |workload|
          # with_lock will start its own transaction
          workload.with_lock("FOR UPDATE SKIP LOCKED") do
            Gouda.logger.info { "Reviving (re-enqueueing) Gouda workload #{workload.id} after interruption" }

            Gouda.instrument(:workloads_revived_counter, {size: 1, job_class: workload.active_job_class_name})

            interrupted_at = workload.last_execution_heartbeat_at
            workload.update!(state: "finished", interrupted_at: interrupted_at, last_execution_heartbeat_at: Time.now.utc, execution_finished_at: Time.now.utc)
            revived_job = ActiveJob::Base.deserialize(workload.active_job_data)
            # Save the interrupted_at timestamp so that upon execution the new job will raise a Gouda::Interrpupted exception.
            # The exception can then be handled like any other ActiveJob exception (using rescue_from or similar).
            revived_job.interrupted_at = interrupted_at
            revived_job.enqueue
          end
        rescue ActiveRecord::RecordNotFound
          # This will happen if we have selected the zombie workload in the outer block, but
          # by the point we reload it and take a FOR UPDATE SKIP LOCKED lock another worker is
          # already reaping it - a call to `reload` will cause a RecordNotFound, since Postgres
          # will hide the row from us. This is what we want in fact - we want to progress to
          # the next row. So we allow the code to proceed, as we expect that the other worker
          # (which stole the workload from us) will have set it to "state=finished" by the time we reattempt
          # our SELECT with conditions
          Gouda.logger.debug { "Gouda workload #{workload.id} cannot be reaped as it was hijacked by another worker" }
        end
      end
    end
  end

  # Lock the next workload and mark it as executing
  def self.checkout_and_lock_one(executing_on:, queue_constraint: Gouda::AnyQueue)
    where_query = <<~SQL
      #{queue_constraint.to_sql}
      AND NOT EXISTS (
        SELECT NULL
        FROM #{quoted_table_name} AS concurrent
        WHERE concurrent.state = 'executing'
          AND concurrent.execution_concurrency_key = #{quoted_table_name}.execution_concurrency_key
      )
    SQL
    # Enter a txn just to mark this job as being executed "by us". This allows us to avoid any
    # locks during execution itself, including advisory locks
    workloads_rel = Gouda::Workload
      .waiting_to_start
      .where(where_query)
      .order("#{quoted_table_name}.priority ASC NULLS LAST")
      .lock("FOR UPDATE SKIP LOCKED")
      .limit(1)

    _first_available_workload = ActiveSupport::Notifications.instrument(:checkout_and_lock_one, {queue_constraint: queue_constraint.to_sql}) do |payload|
      payload[:condition_sql] = workloads_rel.to_sql
      payload[:retried_checkouts_due_to_concurrent_exec] = 0
      uncached do # Necessary because we SELECT with a clock_timestamp() which otherwise gets cached by ActiveRecord query cache
        transaction do
          workload = Gouda.suppressing_sql_logs { workloads_rel.first } # Silence SQL output as this gets called very frequently
          return nil unless workload

          if workload.scheduler_key && !Gouda::Scheduler.known_scheduler_keys.include?(workload.scheduler_key)
            # Check whether this workload was enqueued with a scheduler key, but no longer is in the cron table.
            # If that is the case (we are trying to execute a workload which has a scheduler key, but the scheduler
            # does not know about that key) it means that the workload has been removed from the cron table and must not run.
            # Moreover: running it can be dangerous because it was likely removed from the table for a reason.
            # Should that be the case, mark the job "finished" and return `nil` to get to the next poll. If the deployed worker still has
            # the workload in its scheduler table, but a new deploy removed it - this is a race condition, but we are willing to accept it.
            # Note that we are already "just not enqueueing" that job when the cron table gets loaded - this already happens.
            #
            # Removing jobs from the queue forcibly when we load the cron table is nice, but not enough, because our system can be in a state
            # of partial deployment:
            #
            #   [  release 1 does have some_job_hourly crontab entry ]
            #                  [  release 2 no longer does                           ]
            #                  ^ --- race conditions possible here --^
            #
            # So even if we remove the crontabled workloads during app boot, it does not give us a guarantee that release 1 won't reinsert them.
            # This is why this safeguard is needed.
            error = {class_name: "WorkloadSkippedError", message: "Skipped as scheduler_key was no longer in the cron table"}
            workload.update!(state: "finished", error:)
            # And return nil. This will cause a brief "sleep" in the polling routine since the caller may think there are no more workloads
            # in the queue, but only for a brief moment.
            nil
          else
            # Once we have verified this job is OK to execute
            workload.update!(state: "executing", executing_on: executing_on, last_execution_heartbeat_at: Time.now.utc, execution_started_at: Time.now.utc)
            workload
          end
        rescue ActiveRecord::RecordNotUnique
          # It can happen that due to a race the `execution_concurrency_key NOT IN` does not capture
          # a job which _just_ entered the "executing" state, apparently after we do our SELECT. This will happen regardless
          # whether we are using a CTE or a sub-SELECT
          payload[:retried_checkouts_due_to_concurrent_exec] += 1
          nil
        end
      end
    end
  end

  # Get a new workload and call perform
  # @param in_progress[#add,#delete] Used for tracking work in progress for heartbeats
  def self.checkout_and_perform_one(executing_on:, queue_constraint: Gouda::AnyQueue, in_progress: Set.new)
    # Select a job and mark it as "executing" which will make it unavailable to any other
    workload = checkout_and_lock_one(executing_on: executing_on, queue_constraint: queue_constraint)
    if workload
      in_progress.add(workload.id)
      workload.perform_and_update_state!
    end
  ensure
    in_progress.delete(workload.id) if workload
  end

  def enqueued_at
    Time.parse(serialized_params["enqueued_at"]) if serialized_params["enqueued_at"]
  end

  def perform_and_update_state!
    Gouda.instrument(:perform_job, {workload: self}) do |instrument_payload|
      extras = {}
      if Gouda::JobFuse.exists?(active_job_class_name: active_job_class_name)
        extras[:error] = {class_name: "WorkloadSkippedError", message: "Skipped because of a fuse at #{Time.now.utc}"}
      else
        job_result = ActiveJob::Base.execute(active_job_data)

        if job_result.is_a?(Exception)
          # When an exception is handled, let's say we have a retry_on <exception> in our job, we end up here
          # and it won't be rescueed
          handled_error = job_result
          update!(error: error_hash(handled_error))
        end

        instrument_payload[:value] = job_result
        instrument_payload[:handled_error] = handled_error

        job_result
      end
    rescue => exception_not_retried_by_active_job
      # When a job fails and is not retryable it will end up here.
      update!(error: error_hash(exception_not_retried_by_active_job))
      instrument_payload[:unhandled_error] = exception_not_retried_by_active_job
      Gouda.logger.error { exception_not_retried_by_active_job }
      exception_not_retried_by_active_job # Return the exception instead of re-raising it
    ensure
      update!(state: "finished", last_execution_heartbeat_at: Time.now.utc, execution_finished_at: Time.now.utc, **extras)
      # If the workload that just finished was a scheduled workload (via timer/cron) enqueue the next execution.
      # Otherwise the next job will only get enqueued once the config is reloaded
      Gouda::Scheduler.enqueue_next_scheduled_workload_for(self)
    end
  end

  def schedule_now!
    with_lock do
      return if state != "enqueued"

      update!(scheduled_at: Time.now.utc)
    end
  end

  def mark_finished!
    with_lock do
      now = Time.now.utc
      execution_started_at ||= now

      return if state == "finished"

      update!(
        state: "finished", last_execution_heartbeat_at: now,
        execution_finished_at: now, execution_started_at: execution_started_at,
        error: {class_name: "RemovedError", message: "Manually removed at #{now}"}
      )
      Gouda::Scheduler.enqueue_next_scheduled_workload_for(self)
    end
  end

  def error_hash(error)
    {class_name: error.class.to_s, backtrace: error.backtrace.to_a, message: error.message}
  end

  def active_job_data
    serialized_params.deep_dup.merge("provider_job_id" => id, "interrupted_at" => interrupted_at, "scheduler_key" => scheduler_key) # TODO: is this memory-economical?
  end
end
