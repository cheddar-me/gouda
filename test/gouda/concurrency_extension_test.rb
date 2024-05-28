# frozen_string_literal: true

require "gouda/test_helper"

class GoudaConcurrencyExtensionTest < ActiveSupport::TestCase
  include AssertHelper
  class TestJobWithoutConcurrency < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
  end

  class TestJobWithPerformConcurrency < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with(perform_limit: 1)

    def perform(*args)
    end
  end

  setup do
    @adapter ||= Gouda::Adapter.new
    Gouda::Railtie.initializers.each(&:run)
  end

  test "gouda_control_concurrency_with with just perform_limit sets a perform concurrency key and no enqueue concurrency key" do
    job = TestJobWithPerformConcurrency.new
    assert_nil job.enqueue_concurrency_key
    assert job.execution_concurrency_key
  end

  test "gouda_control_concurrency_with with just perform_limit makes the perform concurrency key dependent on job params" do
    job1 = TestJobWithPerformConcurrency.new(1, 2, :something)
    assert job1.execution_concurrency_key

    job2 = TestJobWithPerformConcurrency.new(1, 2, :something)
    assert_equal job2.execution_concurrency_key, job1.execution_concurrency_key

    job3 = TestJobWithPerformConcurrency.new(1, 2, :something_else)
    refute_equal job3.execution_concurrency_key, job1.execution_concurrency_key
  end

  class TestJobWithCommonConcurrency < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with(total_limit: 1)

    def perform(*args)
    end
  end

  test "gouda_control_concurrency_with with total_limit sets a perform concurrency key and an enqueue concurrency key" do
    job = TestJobWithCommonConcurrency.new
    assert job.enqueue_concurrency_key
    assert job.execution_concurrency_key
  end

  test "gouda_control_concurrency_with with total_limit makes the perform concurrency key dependent on job params" do
    job1 = TestJobWithCommonConcurrency.new(1, 2, :something)
    assert job1.execution_concurrency_key

    job2 = TestJobWithCommonConcurrency.new(1, 2, :something)
    assert_equal job2.execution_concurrency_key, job1.execution_concurrency_key

    job3 = TestJobWithCommonConcurrency.new(1, 2, :something_else)
    refute_equal job3.execution_concurrency_key, job1.execution_concurrency_key
  end

  test "gouda_control_concurrency_with with total_limit makes the enqueue concurrency key dependent on job params" do
    job1 = TestJobWithCommonConcurrency.new(1, 2, :something)
    assert job1.enqueue_concurrency_key

    job2 = TestJobWithCommonConcurrency.new(1, 2, :something)
    assert_equal job2.enqueue_concurrency_key, job1.enqueue_concurrency_key

    job3 = TestJobWithCommonConcurrency.new(1, 2, :something_else)
    refute_equal job3.enqueue_concurrency_key, job1.enqueue_concurrency_key
  end

  class TestJobWithEnqueueConcurrency < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with(enqueue_limit: 1)

    def perform(*args)
    end
  end

  test "gouda_control_concurrency_with with enqueue_limit sets a perform concurrency key and an enqueue concurrency key" do
    job = TestJobWithEnqueueConcurrency.new
    assert job.enqueue_concurrency_key
    assert_nil job.execution_concurrency_key
  end

  test "gouda_control_concurrency_with with enqueue_limit makes the enqueue concurrency key dependent on job params" do
    job1 = TestJobWithEnqueueConcurrency.new(1, 2, :something)
    assert job1.enqueue_concurrency_key

    job2 = TestJobWithEnqueueConcurrency.new(1, 2, :something)
    assert_equal job2.enqueue_concurrency_key, job1.enqueue_concurrency_key

    job3 = TestJobWithEnqueueConcurrency.new(1, 2, :something_else)
    refute_equal job3.enqueue_concurrency_key, job1.enqueue_concurrency_key
  end

  class TestJobWithCustomKey < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with total_limit: 1, key: "42"
  end

  test "can use an arbitrary string as the custom key" do
    job = TestJobWithCustomKey.new
    assert_equal "42", job.enqueue_concurrency_key
    assert_equal "42", job.execution_concurrency_key
  end

  class TestJobWithCustomKeyProc < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
    include Gouda::ActiveJobExtensions::Concurrency
    gouda_control_concurrency_with total_limit: 1, key: -> { @ivar }

    def initialize(...)
      super
      @ivar = "123"
    end
  end

  test "can use a proc that gets instance_exec'd as the custom key" do
    job = TestJobWithCustomKeyProc.new
    assert_equal "123", job.enqueue_concurrency_key
    assert_equal "123", job.execution_concurrency_key
  end

  class TestJobWithWithUnconfiguredConcurrency < ActiveJob::Base
    self.queue_adapter = Gouda::Adapter.new
    include Gouda::ActiveJobExtensions::Concurrency
  end

  test "validates arguments" do
    assert_raises ArgumentError do
      TestJobWithWithUnconfiguredConcurrency.gouda_control_concurrency_with
    end

    assert_raises ArgumentError do
      TestJobWithWithUnconfiguredConcurrency.gouda_control_concurrency_with total_limit: 2
    end

    assert_raises ArgumentError do
      TestJobWithWithUnconfiguredConcurrency.gouda_control_concurrency_with perform_limit: 2
    end

    assert_raises ArgumentError do
      TestJobWithWithUnconfiguredConcurrency.gouda_control_concurrency_with enqueue_limit: 2
    end

    assert_raises ArgumentError do
      TestJobWithWithUnconfiguredConcurrency.gouda_control_concurrency_with total_limit: 2, bollocks: 4
    end
  end
end
