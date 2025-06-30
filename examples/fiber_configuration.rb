# Example configuration for using Gouda with fiber-based non-blocking IO

# In your Rails configuration or initializer:
Gouda.configure do |config|
  # Enable fiber-based scheduler instead of thread-based
  config.use_fiber_scheduler = true

  # Number of worker fibers (can be higher than CPU cores since they're non-blocking)
  config.fiber_worker_count = 10

  # Database connection pool size for fiber concurrency
  # Should be >= fiber_worker_count + 1 (for housekeeping)
  config.async_db_pool_size = 25

  # Polling interval (how often to check for new jobs when queue is empty)
  config.polling_sleep_interval_seconds = 0.1

  # Other existing configuration options still work
  config.preserve_job_records = false
  config.cleanup_preserved_jobs_before = 3.hours
end

# Example of starting the worker with fibers:
# Gouda.start

# Benefits of fiber-based approach:
# 1. Non-blocking IO operations (database queries, HTTP requests, etc.)
# 2. Higher concurrency with less memory overhead than threads
# 3. Better performance for IO-bound jobs
# 4. Cooperative scheduling reduces context switching overhead

# Important considerations:
# 1. Your jobs must use async-compatible gems for full benefit
# 2. Database connection pool size should be configured appropriately
# 3. CPU-intensive jobs won't benefit much from fiber approach
# 4. Requires Ruby 3.1+ with Fiber.scheduler support
