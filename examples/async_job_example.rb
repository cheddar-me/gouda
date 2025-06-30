# Example of an async-compatible job for use with Gouda's fiber scheduler

require 'async'
require 'async/http'

class AsyncHttpJob < ApplicationJob
  queue_as :default

  def perform(url)
    # This job will work with both thread-based and fiber-based workers
    # When using fiber scheduler, HTTP requests will be non-blocking
    
    # Use async-compatible HTTP client
    response = fetch_url_async(url)
    
    # Process the response
    process_response(response)
  end

  private

  def fetch_url_async(url)
    # When running under fiber scheduler, this will be non-blocking
    # When running under thread scheduler, this will block the thread
    Async do |task|
      internet = Async::HTTP::Internet.new
      response = internet.get(url)
      response.read
    ensure
      internet&.close
    end
  end

  def process_response(data)
    # Process the fetched data
    Rails.logger.info "Fetched #{data.size} bytes"
    
    # Example: Save to database (this will also be async with fiber scheduler)
    MyModel.create!(data: data, processed_at: Time.current)
  end
end

# Example of a job that benefits from fiber concurrency
class BulkDataProcessorJob < ApplicationJob
  queue_as :bulk_processing

  def perform(record_ids)
    # Process multiple records concurrently using fibers
    results = []
    
    # When using fiber scheduler, these database operations can be concurrent
    record_ids.each do |id|
      # This will be non-blocking with fiber scheduler
      record = MyModel.find(id)
      result = process_record(record)
      results << result
    end
    
    # Aggregate results
    aggregate_results(results)
  end

  private

  def process_record(record)
    # Simulate some async work (API call, file processing, etc.)
    # With fiber scheduler, multiple records can be processed concurrently
    {
      id: record.id,
      processed_at: Time.current,
      result: expensive_operation(record)
    }
  end

  def expensive_operation(record)
    # This could be an HTTP API call, file processing, etc.
    # that benefits from non-blocking IO
    sleep(0.1) # Simulated IO wait
    "processed_#{record.id}"
  end

  def aggregate_results(results)
    # Save aggregated results
    Rails.logger.info "Processed #{results.size} records"
  end
end

# Example of configuring different queue constraints for fiber workers
class FiberWorkerInitializer
  def self.start_workers
    if Rails.env.production?
      # Start multiple workers with different queue constraints
      
      # High-throughput fiber worker for IO-bound jobs
      Thread.new do
        Gouda.configure do |config|
          config.use_fiber_scheduler = true
          config.fiber_worker_count = 20
          config.async_db_pool_size = 25
        end
        
        Gouda.start
      end
      
      # Traditional thread worker for CPU-bound jobs
      Thread.new do
        Gouda.configure do |config|
          config.use_fiber_scheduler = false
          config.worker_thread_count = 4
        end
        
        ENV['GOUDA_QUEUES'] = 'cpu_intensive'
        Gouda.start
      end
    end
  end
end 