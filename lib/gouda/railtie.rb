# frozen_string_literal: true

module Gouda
  UNINITIALISED_DATABASE_EXCEPTIONS = [ActiveRecord::NoDatabaseError, ActiveRecord::StatementInvalid, ActiveRecord::ConnectionNotEstablished]

  class Railtie < Rails::Railtie
    rake_tasks do
      task preload: :setup do
        if defined?(Rails) && Rails.respond_to?(:application)
          if Rails.application.config.eager_load
            ActiveSupport.run_load_hooks(:before_eager_load, Rails.application)
            Rails.application.config.eager_load_namespaces.each(&:eager_load!)
          end
        end
      end
    end

    initializer "gouda.configure_rails_initialization" do
      Gouda.config.app_executor = if defined?(Rails) && Rails.respond_to?(:application)
        Rails.application.executor
      else
        ActiveSupport::Executor
      end
    end

    initializer "gouda.active_job.extensions" do
      ActiveSupport.on_load :active_job do
        include Gouda::ActiveJobExtensions::Interrupts
      end
    end

    generators do
      require "generators/gouda/install_generator"
    end

    # The `to_prepare` block which is executed once in production
    # and before each request in development.
    config.to_prepare do
      if defined?(Rails) && Rails.respond_to?(:application)
        config_from_rails = Rails.application.config.try(:gouda)
        if config_from_rails
          Gouda.config.cleanup_preserved_jobs_before = config_from_rails[:cleanup_preserved_jobs_before]
          Gouda.config.preserve_job_records = config_from_rails[:preserve_job_records]
          Gouda.config.polling_sleep_interval_seconds = config_from_rails[:polling_sleep_interval_seconds]
          Gouda.config.worker_thread_count = config_from_rails[:worker_thread_count]
          Gouda.config.use_fiber_scheduler = config_from_rails[:use_fiber_scheduler]
          Gouda.config.fibers_per_thread = config_from_rails[:fibers_per_thread]
        end
      else
        Gouda.config.preserve_job_records = false
        Gouda.config.polling_sleep_interval_seconds = 0.2
      end

      Gouda::Scheduler.build_scheduler_entries_list!
      begin
        Gouda::Scheduler.upsert_workloads_from_entries_list!
      rescue *Gouda::UNINITIALISED_DATABASE_EXCEPTIONS
        # Do nothing. On a freshly checked-out Rails app, running even unrelated Rails tasks
        # (such as asset compilation) - or, more importantly, initial db:create -
        # will cause a NoDatabaseError, as this is a chicken-and-egg problem. That error
        # is safe to ignore in this instance - we should let the outer task proceed,
        # because if there is no database we should allow it to get created.
      end
    end
  end
end
