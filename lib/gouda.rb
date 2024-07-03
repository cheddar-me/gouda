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
  end

  class InterruptError < StandardError
  end

  class ConcurrencyExceededError < StandardError
  end

  def self.start
    queue_constraint = if ENV["GOUDA_QUEUES"]
      Gouda.parse_queue_constraint(ENV["GOUDA_QUEUES"])
    else
      Gouda::AnyQueue
    end

    logger.info("Gouda version: #{Gouda::VERSION}")
    logger.info("Worker threads: #{Gouda.config.worker_thread_count}")

    worker_loop(n_threads: Gouda.config.worker_thread_count, queue_constraint: queue_constraint)
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
    @fallback_gouda_logger ||= begin
      ActiveSupport::Logger.new($stdout).tap do |logger|
        logger.level = Logger::WARN
      end
    end

    # We want the Rails-configured loggers to take precedence over ours, since Gouda
    # is just an ActiveJob adapter and the Workload is just an ActiveRecord, in the end.
    # So it should be up to the developer of the app, not to us, to set the logger up
    # and configure out. There are also gems such as "stackdriver" from Google which
    # rather unceremonously overwrite the Rails logger with their own. If that happens,
    # it is the choice of the user to do so - and we should honor that choice. Same for
    # the logging level - the Rails logger level must take precendence. Same for logger
    # broadcasts which get set up, for example, by the Rails console when you start it.
    Rails.try(:logger) || ActiveJob::Base.try(:logger) || @fallback_gouda_logger
  end

  def self.instrument(channel, options, &block)
    ActiveSupport::Notifications.instrument("#{channel}.gouda", options, &block)
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
end
