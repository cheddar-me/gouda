# frozen_string_literal: true

# Timers handles jobs which run either on a Cron schedule or using arbitrary time intervals

require "fugit"
module Gouda::Scheduler
  # A timer entry is either a Cron pattern or an interval duration, and configures which job needs to be scheduled
  # and when
  class Entry < Struct.new(:name, :cron, :interval_seconds, :job_class, :kwargs, :args, :set, keyword_init: true)
    def scheduler_key
      [name, interval_seconds, cron, job_class].compact.join("_")
    end

    def next_at
      if interval_seconds
        first_existing = Gouda::Workload.where(scheduler_key: scheduler_key).where("scheduled_at > NOW()").order("scheduled_at DESC").pluck(:scheduled_at).first
        (first_existing || Time.now.utc) + interval_seconds
      elsif cron
        fugit = Fugit::Cron.parse(cron)
        raise ArgumentError, "Unable to parse cron pattern #{cron.inspect}" unless fugit
        Time.at(fugit.next_time.to_i).utc
      end
    end

    def build_active_job
      next_at = self.next_at
      return unless next_at

      job_class = self.job_class.constantize

      active_job = kwargs_value.present? ? job_class.new(*args_value, **kwargs_value) : job_class.new(*args_value) # This method supports ruby2_keywords
      active_job.scheduled_at = next_at
      active_job.scheduler_key = scheduler_key

      set_value.present? ? active_job.set(set_value) : active_job
    end

    private

    def set_value
      value = set || {}
      value.respond_to?(:call) ? value.call : value
    end

    def args_value
      value = args || []
      value.respond_to?(:call) ? value.call : value
    end

    def kwargs_value
      value = kwargs || nil
      value.respond_to?(:call) ? value.call : value
    end
  end

  def self.update_schedule_from_config!(cron_table_hash = nil)
    Gouda.logger.info "Updating scheduled workload entries..."
    if cron_table_hash.blank?
      config_from_rails = Rails.application.config.try(:gouda)

      cron_table_hash = if config_from_rails.present?
        config_from_rails.dig(:cron).to_h if config_from_rails.dig(:enable_cron)
      elsif Gouda.config.enable_cron
        Gouda.config.cron
      end

      return unless cron_table_hash
    end

    defaults = {cron: nil, interval_seconds: nil, kwargs: nil, args: nil}
    @cron_table = cron_table_hash.map do |(name, cron_entry_params)|
      # `class` is a reserved keyword and a method that exists on every Ruby object so...
      cron_entry_params[:job_class] ||= cron_entry_params.delete(:class)
      params_with_defaults = defaults.merge(cron_entry_params)
      Entry.new(name: name, **params_with_defaults)
    end
  end

  def self.enqueue_next_scheduled_workload_for(finished_workload)
    return unless finished_workload.scheduler_key

    timer_table = @cron_table.to_a.index_by(&:scheduler_key)
    timer_entry = timer_table[finished_workload.scheduler_key]
    return unless timer_entry

    Gouda.enqueue_jobs_via_their_adapters([timer_entry.build_active_job])
  end

  def self.entries
    @cron_table || []
  end

  def self.update_scheduled_workloads!
    table_entries = @cron_table || []

    # Remove any cron keyed workloads which no longer match config-wise
    known_keys = table_entries.map(&:scheduler_key).uniq
    Gouda::Workload.transaction do
      Gouda::Workload.where.not(scheduler_key: known_keys).delete_all

      # Insert the next iteration for every "next" entry in the crontab.
      active_jobs_to_enqueue = table_entries.filter_map(&:build_active_job)
      Gouda.logger.info "#{active_jobs_to_enqueue.size} job(s) to enqueue from the scheduler."
      enqjobs = Gouda.enqueue_jobs_via_their_adapters(active_jobs_to_enqueue)
      Gouda.logger.info "#{enqjobs.size} scheduled job(s) enqueued."
    end
  end
end
