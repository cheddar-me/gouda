# frozen_string_literal: true

module Gouda
  module ActiveJobExtensions
    module Interrupts
      extend ActiveSupport::Concern

      included do
        around_perform do |job, block|
          # The @gouda_workload_interrupted_at ivar gets set on the job when the Workload
          # gets reaped as a zombie. It contains the last know heartbeat of the job, assuming
          # that it got interrupted around that particular time. The ivar gets persisted not
          # into the original Workload (which gets marked "finished") but into the new Workload
          # which the reap_zombie_workloads method enqueues.
          if job.interrupted_at
            Gouda.logger.warn { "Job: #{job.class.name} #{job.job_id} was previously interrupted" }
            # The job is going to be re-enqueued it InterruptError is marked as retriable. We need
            # to remove `interrupted_at` otherwise it will get raised again once that new job
            # starts executing - which is not what we want
            interrupted_error_time = job.interrupted_at
            job.interrupted_at = nil

            raise Gouda::InterruptError, "Job was interrupted around #{interrupted_error_time}"
          end
          block.call
        end

        # This overrides ActiveJob::Base to also set the "interrupted_at" value, which Gouda
        # supplies in the active_job_data hash. The value is needed so that the job can correctly
        # raise an InterruptError after an interruption, and we have to do it here so that we can
        # still use ActiveJob::Base.execute, which Appsignal overloads.
        # We also need to retain the scheduler_key value so that retries which ActiveJob does for us
        # preserve that value when remarshaling the job
        def self.deserialize(active_job_data)
          super.tap do |job|
            job.interrupted_at = active_job_data["interrupted_at"]
            job.scheduler_key = active_job_data["scheduler_key"]
          end
        end

        attr_accessor :interrupted_at
        attr_accessor :scheduler_key
      end
    end
  end
end
