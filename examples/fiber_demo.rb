#!/usr/bin/env ruby

# Demonstration of Gouda's fiber-based non-blocking IO worker
# Run this script to see fiber workers in action

require_relative "../lib/gouda"
require "active_record"
require "active_job"

# Setup database connection (assumes PostgreSQL is available)
ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  database: "gouda_demo",
  username: ENV["USER"]
)

# Create tables if they don't exist
begin
  ActiveRecord::Base.connection.execute("SELECT 1 FROM gouda_workloads LIMIT 1")
rescue ActiveRecord::StatementInvalid
  ActiveRecord::Schema.define(version: 1) do |schema|
    Gouda.create_tables(schema)
  end
end

# Configure Gouda for fiber-based execution
Gouda.configure do |config|
  config.use_fiber_scheduler = true
  config.fiber_worker_count = 5
  config.async_db_pool_size = 10
  config.polling_sleep_interval_seconds = 0.1
  config.preserve_job_records = true
end

# Demo job that simulates IO-bound work
class DemoAsyncJob < ActiveJob::Base
  queue_as :default

  def perform(task_name, duration = 0.1)
    puts "🚀 Starting #{task_name} (Fiber: #{Fiber.current.object_id})"

    # Simulate async IO work (this will be non-blocking with fiber scheduler)
    sleep(duration)

    puts "✅ Completed #{task_name} after #{duration}s"

    # Return some result
    "Task #{task_name} completed at #{Time.now}"
  end
end

# Demo job that makes HTTP requests (would be non-blocking with proper async HTTP client)
class HttpFetchJob < ActiveJob::Base
  queue_as :http

  def perform(url)
    puts "🌐 Fetching #{url} (Fiber: #{Fiber.current.object_id})"

    # In a real scenario, you'd use an async HTTP client like:
    # require 'async/http'
    # Async do
    #   internet = Async::HTTP::Internet.new
    #   response = internet.get(url)
    #   puts "📄 Fetched #{response.read.size} bytes from #{url}"
    # end

    # For demo purposes, simulate the request
    sleep(0.2)
    puts "📄 Simulated fetch from #{url}"
  end
end

puts "🎯 Gouda Fiber Worker Demo"
puts "========================="
puts "Configuration:"
puts "  - Fiber scheduler: #{Gouda.config.use_fiber_scheduler}"
puts "  - Fiber workers: #{Gouda.config.fiber_worker_count}"
puts "  - DB pool size: #{Gouda.config.async_db_pool_size}"
puts ""

# Clean up old jobs
Gouda::Workload.delete_all

# Enqueue demo jobs
puts "📝 Enqueueing jobs..."
Gouda.in_bulk do
  # Enqueue several IO-bound jobs
  5.times do |i|
    DemoAsyncJob.perform_later("Task-#{i}", 0.1 + (i * 0.05))
  end

  # Enqueue some HTTP jobs
  3.times do |i|
    HttpFetchJob.perform_later("https://example.com/page#{i}")
  end
end

queued_count = Gouda::Workload.where(state: "enqueued").count
puts "📊 Enqueued #{queued_count} jobs"
puts ""

puts "🔄 Starting fiber workers..."
puts "Press Ctrl+C to stop"
puts ""

# Run the fiber worker
begin
  Gouda.start
rescue Interrupt
  puts "\n🛑 Stopping workers..."
end

# Show results
puts "\n📊 Final Results:"
puts "  - Finished: #{Gouda::Workload.where(state: "finished").count}"
puts "  - Enqueued: #{Gouda::Workload.where(state: "enqueued").count}"
puts "  - Executing: #{Gouda::Workload.where(state: "executing").count}"

puts "\n💡 Notice how multiple jobs ran concurrently using fibers!"
puts "   Each job shows its Fiber object ID to demonstrate concurrency."
