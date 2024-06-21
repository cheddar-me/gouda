# frozen_string_literal: true

# Acts as an ActiveJob adapter

class Gouda::Adapter
  prepend Gouda::BulkAdapterExtension

  ENQUEUE_ERROR_MESSAGE = <<~ERR
    The job has been rejected due to a matching enqueue concurrency key
  ERR

  # Enqueues the ActiveJob job to be performed.
  # For use by Rails; you should generally not call this directly.
  # @param active_job [ActiveJob::Base] the job to be enqueued from +#perform_later+
  # @return [String, nil] the ID of the inserted workload or nil if the insert did not go through (due to concurrency)
  def enqueue(active_job)
    # This is the method that gets called by ActiveJob internally (from inside the ActiveJob::Base instance
    # method). This is also when ActiveJob runs the enqueue callbacks. After this method returns
    # ActiveJob will set @successfully_enqueued inside the job to `true` as long as no
    # EnqueueError has been raised. This is, of course, incompatible with bulk-enqueueing (which we want)
    # to use by default. What we can do is verify the value of the property set by our `enqueue_all` method,
    # and raise the exception based on that.
    enqueue_all([active_job])
    if active_job.enqueue_error
      Gouda.logger.warn { "Error #{active_job.enqueue_error.inspect} for Gouda workload (#{active_job.job_id})" }
      raise active_job.enqueue_error
    end
    active_job.provider_job_id
  end

  # Enqueues an ActiveJob job to be run at a specific time.
  # For use by Rails; you should generally not call this directly.
  # @param active_job [ActiveJob::Base] the job to be enqueued from +#perform_later+
  # @param timestamp [Integer, nil] the epoch time to perform the job
  # @return [String, nil] the ID of the inserted Gouda or nil if the insert did not go through (due to concurrency)
  def enqueue_at(active_job, timestamp_int)
    active_job.scheduled_at = Time.at(timestamp_int).utc
    enqueue_all([active_job])
    if active_job.enqueue_error
      Gouda.logger.warn { "Error #{active_job.enqueue_error.inspect} for Gouda workload (#{active_job.job_id})" }
      raise active_job.enqueue_error
    end
    active_job.provider_job_id
  end

  # Enqueues multiple ActiveJobs.
  # For use by Rails; you should generally not call this directly.
  # @param active_job [ActiveJob::Base] the job to be enqueued from +#perform_later+
  # @param timestamp [Integer, nil] the epoch time to perform the job
  # @return [Integer] the number of jobs which were successfully sent to the queue
  def enqueue_all(active_jobs)
    t_now = Time.now.utc
    bulk_insert_attributes = active_jobs.map.with_index do |active_job, i|
      # We can't pregenerate an ID because we want to INSERT .. ON CONFLICT DO NOTHING
      # and we want Postgres to use _all_ unique indexes for it, which would include a conflict of IDs -
      # so some jobs could get silently rejected because of a duplicate ID. However unlikely this can better be prevented.
      # We can't tell Postgres to ignore conflicts on _both_ the scheduler key and the enqueue concurrency key but not on
      # the ID - it is either "all indexes" or "just one", but never "this index and that index". MERGE https://www.postgresql.org/docs/current/sql-merge.html
      # is in theory capable of solving this but let's not complicate things all to hastily, the hour is getting late
      scheduler_key = active_job.try(:executions) == 0 ? active_job.scheduler_key : nil # only enforce scheduler key on first workload
      {
        active_job_id: active_job.job_id, # Multiple jobs can have the same ID due to retries, job-iteration etc.
        scheduled_at: active_job.scheduled_at || t_now,
        scheduler_key: scheduler_key,
        priority: active_job.priority,
        execution_concurrency_key: extract_execution_concurrency_key(active_job),
        enqueue_concurrency_key: extract_enqueue_concurrency_key(active_job),
        queue_name: active_job.queue_name || "default",
        active_job_class_name: active_job.class.to_s,
        serialized_params: active_job.serialize.except("provider_job_id"), # For when a job which gets retried
        interrupted_at: active_job.interrupted_at, # So that an exception can be raised when this job gets executed
        position_in_bulk: i,
        state: "enqueued"
      }
    end

    # Filter out all the jobs with the same (and present) concurrency key and scheduler key
    bulk_insert_attributes = filter_by_unique_not_nil_hash_key(bulk_insert_attributes, :enqueue_concurrency_key)
    bulk_insert_attributes = filter_by_unique_not_nil_hash_key(bulk_insert_attributes, :scheduler_key)

    # Do a bulk insert. For jobs with an enqueue concurrency key there will be no enqueue
    # as the default for insert_all is to DO NOTHING. An exception would be nice but we are after performance here.
    # Use batches of 500 so that we do not exceed the maximum statement size or do not create a transaction for the
    # insert which times out
    inserted_ids_and_positions = bulk_insert_attributes.each_slice(500).flat_map do |chunk|
      Gouda.instrument(:insert_all, n_rows: chunk.size) do |payload|
        rows = Gouda::Workload.insert_all(chunk, returning: [:id, :position_in_bulk])
        payload[:inserted_jobs] = rows.length
        payload[:rejected_jobs] = chunk.size - rows.length
        rows
      end
    end

    # Mark all the jobs we ended up not enqueuing as such. If these jobs are getting enqueued "one by one"
    # then their callbacks have already run, and they are already set to `successfully_enqueued = true`. If
    # they are enqueued using `enqueue_all` directly there are no guarantees, as `enqueue_all` is a fairly new
    # Rails feature. Now is the moment we need to "fish out" our bulk enqueue position and use it to detect
    # which jobs did get enqueued and which didn't. Yes, this is a bit roundabout - but otherwise we could
    # have a unique index and DO NOTHING just on the enqueue concurrency key
    inserted_ids_and_positions.each do |row|
      i = row.fetch("position_in_bulk")
      active_jobs[i].provider_job_id = row.fetch("id")
      active_jobs[i].successfully_enqueued = true
    end
    _, failed_enqueue = active_jobs.partition(&:successfully_enqueued?)
    failed_enqueue.each do |active_job|
      active_job.successfully_enqueued = false
      active_job.enqueue_error = ActiveJob::EnqueueError.new(ENQUEUE_ERROR_MESSAGE)
    end

    # And return how many jobs we _did_ enqueue
    inserted_ids_and_positions.length
  end

  # The whole point of Gouda is actually co-committing jobs with the business objects they use. The
  # changes in Rails are directed towards shifting the job enqueues into an after_commit hook, so
  # that the jobs - when they start executing - will always find the committed business-objects in
  # the database. It is their attempt at ensuring read-after-write consistency in the face of two
  # separate data stores. However, with a DB-based job queue which is using the same database
  # as the rest of the application, we actually want the opposite - if a transaction commits,
  # we want it to commit both the jobs to be done on the business objects and the business objects
  # themselves. Folding the job enqueues into the same transaction can also be a great improvement
  # to performance. Some of our jobs also imply that a job was generated as a result of a business
  # model change. With after_commit, there is a subtle race condition where your application may
  # crash between you doing the COMMIT on your transaction and the after_commit hooks executing.
  # We want to avoid this in Gouda and always have a guarantee that if our main models committed,
  # so did the jobs that use them.
  # So: tell ActiveJob that we prefer the jobs to be co-committed.
  #
  # See https://github.com/rails/rails/pull/51426
  def enqueue_after_transaction_commit?
    false
  end

  private

  def combine_enqueue_concurrency_key(enqueue_concurrency_key, scheduler_key, cursor_position)
    # We also include the scheduler key into the enqueue key. This is done for the following reasons:
    # Our scheduler always schedules "next subsequent" job once a job completes or fails. If we already have
    # a job scheduled for execution way in the future (say - next month), and the enqueue concurrency key is set,
    # we will need to manually remove it from the queue if we want to run its instance sooner. We could define a
    # unique index on (enqueue_concurrency_key, scheduler_key) - but that would make our enqueue concurrency keys
    # because NULLs in the scheduler_key are not considered equal to each other. We could mofidy our index statement
    # with NULLS NOT DISTINCT - see https://www.postgresql.org/docs/current/indexes-unique.html - but that would
    # create another problem. We want NULLs to _be_ distinct for the enqueue_concurrency_key column, but we want them
    # to _not_ be distinct for the scheduler_key column (one off-scheduler job eneuqued at most for the same
    # scheduler_key value). Postgres does not give us this ability, sadly. So the way to go about it is to
    # mix the scheduler key (name of the scheduled task + cron pattern and whatnot) into the enqueue_concurrency_key
    # value itself - this provides us with all the necessary properties.
    # For job-iteration we need to do the same so that we can have multiple jobs enqueued with the same key but
    # different cursor positions
    [enqueue_concurrency_key, scheduler_key, cursor_position].compact.join(":")
  end

  def extract_enqueue_concurrency_key(active_job)
    ck_value = active_job.try(:enqueue_concurrency_key)
    return unless ck_value.present?

    enqueueing_as = active_job.try(:scheduler_key).present? ? "scheduled" : "immediate"
    combine_enqueue_concurrency_key(ck_value, enqueueing_as, active_job.try(:cursor_position))
  end

  def extract_execution_concurrency_key(active_job)
    active_job.try(:execution_concurrency_key)
  end

  # Finds all hashes in the given attributes which have the same value of the given attribute and preserves just one
  # in the returned array. We need to do that for both the scheduler key and the enqueue concurrency key.
  def filter_by_unique_not_nil_hash_key(bulk_insert_attributes, key_name)
    # This is not as nice as a combo of partition/unique_by and whatnot but it is linear time, so there.
    seen = Set.new
    bulk_insert_attributes.filter do |item|
      maybe_key = item.fetch(key_name)
      if maybe_key && seen.include?(maybe_key)
        false
      elsif maybe_key
        seen << maybe_key
        true
      else
        true
      end
    end
  end
end
