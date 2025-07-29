# frozen_string_literal: true

module Gouda
  # ConnectionManagedExecutor wraps the Rails executor to implement Rails 7.2+ 
  # connection management patterns. This prevents database connections from being
  # held for the entire duration of job execution.
  #
  # The key insight is that job code often calls ActiveRecord::Base.connection
  # directly, which checks out a permanent connection. This executor temporarily
  # overrides that method during job execution to use per-query connections instead.
  class ConnectionManagedExecutor
    def initialize(base_executor)
      @base_executor = base_executor
    end

    def wrap(&block)
      @base_executor.wrap do
        # Temporarily override ActiveRecord::Base.connection to prevent permanent checkout
        if defined?(ActiveRecord::Base)
          # Store the original connection method
          original_connection_method = ActiveRecord::Base.method(:connection)
          
          # Override the connection method to use per-query connections
          ActiveRecord::Base.define_singleton_method(:connection) do
            # Use with_connection for per-query access instead of permanent checkout
            connection_pool.with_connection do |conn|
              conn
            end
          end
          
          begin
            # Execute the job with connection override in place
            block.call
          ensure
            # Always restore the original connection method
            ActiveRecord::Base.define_singleton_method(:connection, original_connection_method)
          end
        else
          # Fallback if ActiveRecord is not available
          block.call
        end
      end
    end

    # Delegate any other methods to the base executor
    def method_missing(method_name, *args, **kwargs, &block)
      @base_executor.public_send(method_name, *args, **kwargs, &block)
    end

    def respond_to_missing?(method_name, include_private = false)
      @base_executor.respond_to?(method_name, include_private) || super
    end
  end
end 