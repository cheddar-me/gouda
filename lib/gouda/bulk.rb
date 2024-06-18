# frozen_string_literal: true

module Gouda
  # Inside this call, all `perform_later` calls on ActiveJob subclasses
  # (including mailers) will be buffered. The call is reentrant, so you
  # can have multiple `in_bulk` calls with arbitrary nesting. At the end
  # of the block, the buffered jobs will be enqueued using their respective
  # adapters. If an adapter supports `enqueue_all` (Sidekiq does in recent
  # releases of Rails, for example), this functionality will be used. This
  # method is especially useful when doing things such as mass-emails, or
  # maintenance tasks where a large swath of jobs gets enqueued at once.
  #
  # @example
  #    Gouda.in_bulk do
  #      User.recently_joined.find_each do |recently_joined_user|
  #        GreetingJob.perform_later(recently_joined_user)
  #      end
  #    end
  # @return [Object] the return value of the block
  def self.in_bulk(&blk)
    if Thread.current[:gouda_bulk_buffer].nil?
      Thread.current[:gouda_bulk_buffer] = []
      retval = yield
      buf, Thread.current[:gouda_bulk_buffer] = Thread.current[:gouda_bulk_buffer], nil
      enqueue_jobs_via_their_adapters(buf)
      retval
    else # There already is an open bulk
      yield
    end
  end

  # This method exists in edge Rails so probably can be replaced later:
  # https://github.com/rails/rails/commit/9b62f88a2fde0d2bf8c4f6e3bcd06ecba7ca9d8d
  def self.enqueue_jobs_via_their_adapters(active_jobs)
    jobs_per_adapter = active_jobs.compact.group_by { |aj| aj.class.queue_adapter }
    jobs_per_adapter.each_pair do |adapter, active_jobs|
      if adapter.respond_to?(:enqueue_all)
        adapter.enqueue_all(active_jobs)
      else
        active_jobs.each { |aj| adapter.enqueue(aj) }
      end
    end
  end

  module BulkAdapterExtension
    def enqueue_all(active_jobs)
      if Thread.current[:gouda_bulk_buffer]
        Thread.current[:gouda_bulk_buffer].append(*active_jobs)
        active_jobs
      else
        super
      end
    end
  end
end
