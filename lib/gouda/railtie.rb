# frozen_string_literal: true

module Gouda
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
      Gouda.app_executor = if defined?(Rails) && Rails.respond_to?(:application)
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

    # The `to_prepare` block which is executed once in production
    # and before each request in development.
    config.to_prepare do
      config_from_rails = Rails.application.config.try(:gouda)
      if config_from_rails
        Gouda::Scheduler.update_schedule_from_config!
        Gouda.cleanup_preserved_jobs_before = config_from_rails[:cleanup_preserved_jobs_before]
        Gouda.preserve_job_records = config_from_rails[:preserve_job_records]
        Gouda.polling_sleep_interval_seconds = config_from_rails[:polling_sleep_interval_seconds]
        Gouda.worker_thread_count = config_from_rails[:worker_thread_count]
      end
    end
  end
end
