# frozen_string_literal: true

require_relative "test_helper"

class ConnectionTestJob < ActiveJob::Base
  def perform(sleep_duration = 2)
    # Access database at start
    ActiveRecord::Base.connection.execute("SELECT 'job started'")
    
    # Simulate long-running work
    sleep(sleep_duration)
    
    # Access database at end
    ActiveRecord::Base.connection.execute("SELECT 'job finished'")
  end
end

class ConnectionManagementTest < ActiveSupport::TestCase
  def setup
    super
    ActiveRecord::Base.connection_handler.clear_active_connections!
    @adapter = Gouda::Adapter.new
  end

  def teardown
    ActiveRecord::Base.connection_handler.clear_active_connections!
    super
  end

  test "connections are not held during job execution sleep periods" do
    # Enable connection management for this test
    original_setting = Gouda.config.prevent_connection_hoarding
    Gouda.config.prevent_connection_hoarding = true
    Gouda::Railtie.initializers.find { |i| i.name == "gouda.configure_rails_initialization" }.run
    
    begin
      # Create and enqueue a job that sleeps
      job = ConnectionTestJob.new(1)
      @adapter.enqueue(job)
      workload = Gouda::Workload.last
      
      pool = ActiveRecord::Base.connection_pool
      connection_held_during_sleep = false
      
      # Monitor connection pool in a separate thread
      monitor_thread = Thread.new do
        sleep(0.3) # Wait for job to start and access DB
        connection_held_during_sleep = pool.stat[:busy] > 0
      end
      
      # Execute the job
      execution_thread = Thread.new do
        Gouda.config.app_executor.wrap do
          workload.perform_and_update_state!
        end
      end
      
      execution_thread.join
      monitor_thread.join
      
      # Verify that connections were not held during the sleep period
      refute connection_held_during_sleep, 
             "Expected connections to be released during job sleep period, but they were held"
    ensure
      # Restore the original setting
      Gouda.config.prevent_connection_hoarding = original_setting
      Gouda::Railtie.initializers.find { |i| i.name == "gouda.configure_rails_initialization" }.run
    end
  end
  
  test "prevent_connection_hoarding configuration is disabled by default" do
    # Verify that connection management is disabled by default for backwards compatibility
    refute Gouda.config.prevent_connection_hoarding,
           "Expected prevent_connection_hoarding to be disabled by default for backwards compatibility"
           
    # Verify that the plain executor is used when the setting is disabled (default)
    refute_includes Gouda.config.app_executor.class.name, "ConnectionManagedExecutor",
                   "Expected plain executor when prevent_connection_hoarding is false (default)"
  end
  
  test "prevent_connection_hoarding can be enabled" do
    # Temporarily enable the setting
    original_setting = Gouda.config.prevent_connection_hoarding
    Gouda.config.prevent_connection_hoarding = true
    
    # Re-run the railtie initializer to apply the change
    Gouda::Railtie.initializers.find { |i| i.name == "gouda.configure_rails_initialization" }.run
    
    begin
      # Verify that ConnectionManagedExecutor is used when enabled
      assert_includes Gouda.config.app_executor.class.name, "ConnectionManagedExecutor",
                     "Expected ConnectionManagedExecutor when prevent_connection_hoarding is true"
    ensure
      # Restore the original setting and re-initialize
      Gouda.config.prevent_connection_hoarding = original_setting
      Gouda::Railtie.initializers.find { |i| i.name == "gouda.configure_rails_initialization" }.run
    end
  end
  
  test "prevent_connection_hoarding can be disabled" do
    # Temporarily disable the setting
    original_setting = Gouda.config.prevent_connection_hoarding
    Gouda.config.prevent_connection_hoarding = false
    
    # Re-run the railtie initializer to apply the change
    Gouda::Railtie.initializers.find { |i| i.name == "gouda.configure_rails_initialization" }.run
    
    begin
      # Verify that the plain executor is used when disabled
      refute_includes Gouda.config.app_executor.class.name, "ConnectionManagedExecutor",
                     "Expected plain executor when prevent_connection_hoarding is false"
      
      # In test environment, it should be ActiveSupport::Executor (the class)
      # or Rails application executor if available
      executor_is_plain = Gouda.config.app_executor == ActiveSupport::Executor ||
                         Gouda.config.app_executor.class.name.include?("Executor")
      
      assert executor_is_plain,
             "Expected plain Rails executor when prevent_connection_hoarding is false, got: #{Gouda.config.app_executor.class.name}"
    ensure
      # Restore the original setting and re-initialize
      Gouda.config.prevent_connection_hoarding = original_setting
      Gouda::Railtie.initializers.find { |i| i.name == "gouda.configure_rails_initialization" }.run
    end
  end
end 