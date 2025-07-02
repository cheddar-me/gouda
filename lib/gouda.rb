# frozen_string_literal: true

require "active_support"
require "active_support/core_ext/numeric/time"
require "active_support/configurable"
require "rails/railtie"
require_relative "gouda/bulk"
require_relative "gouda/adapter"
require_relative "gouda/scheduler"
require_relative "gouda/railtie" if defined?(Rails::Railtie)
require_relative "gouda/workload"
require_relative "gouda/worker"
require_relative "gouda/fiber_worker"
require_relative "gouda/job_fuse"
require_relative "gouda/queue_constraints"
require_relative "gouda/active_job_extensions/interrupts"
require_relative "gouda/active_job_extensions/concurrency"
require_relative "active_job/queue_adapters/gouda_adapter"

module Gouda
  class Gouda::Configuration
    include ActiveSupport::Configurable

    config_accessor(:preserve_job_records, default: false)
    config_accessor(:cleanup_preserved_jobs_before, default: 3.hours)
    config_accessor(:polling_sleep_interval_seconds, default: 0.2)
    config_accessor(:worker_thread_count, default: 1)
    config_accessor(:app_executor)
    config_accessor(:cron, default: {})
    config_accessor(:enable_cron, default: true)
    # Deprecated logger configuration. This needs to be available in the
    # Configuration object because it might be used from the Rails app
    # that is using Gouda. The config values will be ignored though.
    config_accessor(:logger, default: nil)
    config_accessor(:log_level, default: nil)

    # Fiber-specific configuration options
    config_accessor(:fiber_worker_count, default: 1)
    config_accessor(:use_fiber_scheduler, default: false)
    config_accessor(:async_db_pool_size, default: 25)
  end

  class InterruptError < StandardError
  end

  class ConcurrencyExceededError < StandardError
  end

  def self.start
    start_with_scheduler_type
  end

  # Enhanced start method that chooses between thread and fiber execution
  def self.start_with_scheduler_type
    queue_constraint = if ENV["GOUDA_QUEUES"]
      Gouda.parse_queue_constraint(ENV["GOUDA_QUEUES"])
    else
      Gouda::AnyQueue
    end

    logger.info("Gouda version: #{Gouda::VERSION}")

    if Gouda.config.use_fiber_scheduler
      logger.info("Using fiber-based scheduler")
      logger.info("Worker fibers: #{Gouda.config.fiber_worker_count}")

      # Check database pool configuration (non-destructive)
      FiberDatabaseSupport.check_pool_configuration
      FiberDatabaseSupport.check_fiber_isolation_level
      
      # Use fiber-based worker
      FiberWorker.worker_loop(
        n_fibers: Gouda.config.fiber_worker_count,
        queue_constraint: queue_constraint
      )
    else
      logger.info("Using thread-based scheduler")
      logger.info("Worker threads: #{Gouda.config.worker_thread_count}")

      # Use original thread-based worker
      worker_loop(
        n_threads: Gouda.config.worker_thread_count,
        queue_constraint: queue_constraint
      )
    end
  end

  def self.config
    @config ||= Configuration.new
  end

  def self.configure
    yield config
  end

  def self.logger
    # By default, return a logger that sends data nowhere. The `Rails.logger` method
    # only becomes available later in the Rails lifecycle.
    @fallback_gouda_logger ||= ActiveSupport::TaggedLogging.new(Logger.new($stdout)).tap do |logger|
      logger.level = Logger::WARN
    end

    # We want the Rails-configured loggers to take precedence over ours, since Gouda
    # is just an ActiveJob adapter and the Workload is just an ActiveRecord, in the end.
    # So it should be up to the developer of the app, not to us, to set the logger up
    # and configure out. There are also gems such as "stackdriver" from Google which
    # rather unceremoniously overwrite the Rails logger with their own. If that happens,
    # it is the choice of the user to do so - and we should honor that choice. Same for
    # the logging level - the Rails logger level must take precendence. Same for logger
    # broadcasts which get set up, for example, by the Rails console when you start it.
    logger_to_use = Rails.try(:logger) || ActiveJob::Base.try(:logger) || @fallback_gouda_logger
    logger_to_use.tagged("Gouda") # So that we don't have to manually prefix/tag on every call
  end

  def self.suppressing_sql_logs(&)
    # This is used for frequently-called methods that poll the DB. If logging is done at a low level (DEBUG)
    # those methods print a lot of SQL into the logs, on every poll. While that is useful if
    # you collect SQL queries from the logs, in most cases - especially if this is used
    # in a side-thread inside Puma - the output might be quite annoying. So silence the
    # logger when we poll, but just to INFO. Omitting DEBUG-level messages gets rid of the SQL.
    if Gouda::Workload.logger
      Gouda::Workload.logger.silence(Logger::INFO, &)
    else
      # In tests (and at earlier stages of the Rails boot cycle) the global ActiveRecord logger may be nil
      yield
    end
  end

  def self.instrument(channel, options, &)
    ActiveSupport::Notifications.instrument("#{channel}.gouda", options, &)
  end

  def self.create_tables(active_record_schema)
    active_record_schema.create_enum :gouda_workload_state, %w[enqueued executing finished]
    active_record_schema.create_table :gouda_workloads, id: :uuid do |t|
      t.uuid :active_job_id, null: false
      t.timestamp :scheduled_at, null: false
      t.timestamp :execution_started_at
      t.timestamp :execution_finished_at
      t.timestamp :last_execution_heartbeat_at
      t.timestamp :interrupted_at, null: true

      t.string :scheduler_key, null: true
      t.string :queue_name, null: false, default: "default"
      t.integer :priority
      t.string :active_job_class_name, null: false
      t.jsonb :serialized_params
      t.jsonb :error, default: {}, null: false
      t.enum :state, enum_type: :gouda_workload_state, default: "enqueued", null: false
      t.string :execution_concurrency_key
      t.string :enqueue_concurrency_key
      t.string :executing_on
      t.integer :position_in_bulk

      t.timestamps
    end

    active_record_schema.add_index :gouda_workloads, [:priority, :id, :scheduled_at], where: "state = 'enqueued'", name: :gouda_checkout_all_index
    active_record_schema.add_index :gouda_workloads, [:id, :last_execution_heartbeat_at], where: "state = 'executing'", name: :gouda_last_heartbeat_index
    active_record_schema.add_index :gouda_workloads, [:enqueue_concurrency_key], where: "state = 'enqueued' AND enqueue_concurrency_key IS NOT NULL", unique: true, name: :guard_double_enqueue
    active_record_schema.add_index :gouda_workloads, [:scheduler_key], where: "state = 'enqueued' AND scheduler_key IS NOT NULL", unique: true, name: :guard_double_schedule
    active_record_schema.add_index :gouda_workloads, [:execution_concurrency_key], where: "state = 'executing' AND execution_concurrency_key IS NOT NULL", unique: true, name: :guard_double_exec
    active_record_schema.add_index :gouda_workloads, [:active_job_id], name: :same_job_display_idx
    active_record_schema.add_index :gouda_workloads, [:priority], order: {priority: "ASC NULLS LAST"}, name: :ordered_priority_idx
    active_record_schema.add_index :gouda_workloads, [:last_execution_heartbeat_at], name: :index_gouda_workloads_on_last_execution_heartbeat_at
    active_record_schema.add_index :gouda_workloads, [:scheduler_key], name: :index_gouda_workloads_on_scheduler_key

    active_record_schema.create_table :gouda_job_fuses, id: false do |t|
      t.string :active_job_class_name, null: false

      t.timestamps
    end
  end

  # Database configuration helpers for fiber mode
  module FiberDatabaseSupport
    def self.check_pool_configuration
      return unless defined?(ActiveRecord::Base)
      
      current_pool_size = ActiveRecord::Base.connection_pool.size
      desired_pool_size = Gouda.config.async_db_pool_size
      
      if current_pool_size < desired_pool_size
        logger.warn("Database pool size (#{current_pool_size}) may be too small for fiber concurrency")
        logger.warn("Consider increasing pool size to #{desired_pool_size} in database.yml")
        logger.warn("Current pool size should work but may cause fiber blocking on DB connections")
      else
        logger.info("Database pool size (#{current_pool_size}) is sufficient for fiber concurrency")
      end
    rescue => e
      logger.warn("Could not check database pool configuration: #{e.message}")
    end

    def self.check_fiber_isolation_level
      return unless defined?(Rails) && Rails.respond_to?(:application) && Rails.application

      begin
        current_isolation = ActiveSupport.isolation_level
        
        # Check if we're using PostgreSQL
        using_postgresql = false
        begin
          if defined?(ActiveRecord::Base) && ActiveRecord::Base.connection.adapter_name == 'PostgreSQL'
            using_postgresql = true
          end
        rescue => e
          # If we can't determine the adapter, assume we might be using PostgreSQL to be safe
          using_postgresql = true
        end
        
        if current_isolation != :fiber && using_postgresql
          logger.warn("=" * 80)
          logger.warn("FIBER SCHEDULER CONFIGURATION WARNING")
          logger.warn("=" * 80)
          logger.warn("Gouda fiber mode is enabled with PostgreSQL but Rails isolation level is set to: #{current_isolation}")
          logger.warn("For optimal fiber-based performance and to avoid potential issues with PostgreSQL,")
          logger.warn("you should set the Rails isolation level to :fiber")
          logger.warn("")
          logger.warn("Add this to your config/application.rb:")
          logger.warn("  config.active_support.isolation_level = :fiber")
          logger.warn("")
          logger.warn("This ensures ActiveRecord connection pools work correctly with fibers")
          logger.warn("and PostgreSQL connections, and can prevent segfaults with Ruby 3.4+.")
          logger.warn("=" * 80)
        elsif current_isolation == :fiber
          logger.info("Rails isolation level correctly set to :fiber for fiber-based execution")
        elsif !using_postgresql
          logger.info("Non-PostgreSQL database detected - isolation level configuration may not be required")
        end
      rescue => e
        logger.warn("Could not check Rails isolation level: #{e.message}")
      end
    end
    
    private
    
    def self.logger
      Gouda.logger
    end
  end
end
