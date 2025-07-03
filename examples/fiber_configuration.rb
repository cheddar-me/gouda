# Example configuration for Gouda fiber-based job processing
# This shows how to properly configure Rails and Gouda for optimal fiber performance

# CRITICAL: Set Rails isolation level to :fiber (config/application.rb)
# This MUST be set when using PostgreSQL to prevent segfaults and ensure proper fiber concurrency
#
# # config/application.rb
# class Application < Rails::Application
#   # Required for fiber mode with PostgreSQL - prevents segfaults and connection issues
#   config.active_support.isolation_level = :fiber
# end

# Gouda fiber configuration
Gouda.configure do |config|
  # Enable fiber-based worker (instead of threads)
  config.use_fiber_scheduler = true

  # Number of concurrent worker fibers
  # Can be higher than CPU cores since fibers are lightweight for IO-bound work
  config.fiber_worker_count = 10

  # Database connection pool size
  # Should be >= fiber_worker_count + buffer for other Rails processes
  config.async_db_pool_size = 25

  # Other standard Gouda configuration options work the same
  config.preserve_job_records = true
  config.cleanup_preserved_jobs_before = 3.hours
end

# Database configuration should also be updated (config/database.yml)
#
# development:
#   adapter: postgresql
#   database: myapp_development
#   pool: 25              # Should match or exceed async_db_pool_size
#   checkout_timeout: 10  # Timeout for getting connections from pool

# Example of starting the fiber-based worker
# Gouda.start

# The worker will automatically:
# 1. Check that Rails isolation level is set to :fiber (warn if not)
# 2. Check that database pool size is sufficient (warn if too small)
# 3. Use fiber-based concurrency for non-blocking IO

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
# 5. When using PostgreSQL: Rails isolation level MUST be set to :fiber
