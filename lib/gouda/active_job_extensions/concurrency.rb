# frozen_string_literal: true

module Gouda
  module ActiveJobExtensions
    module Concurrency
      extend ActiveSupport::Concern

      VALID_TYPES = [String, Symbol, Numeric, Date, Time, TrueClass, FalseClass, NilClass].freeze

      included do
        class_attribute :gouda_concurrency_config, instance_accessor: false, default: {}
      end

      class_methods do
        def gouda_control_concurrency_with(total_limit: nil, perform_limit: nil, enqueue_limit: nil, key: nil)
          raise ArgumentError, "Need one of total_limit, perform_limit, enqueue_limit" if [total_limit, perform_limit, enqueue_limit].all?(&:blank?)
          raise ArgumentError, "The only available limit is 1" if [total_limit, perform_limit, enqueue_limit].any? { |v| v.is_a?(Integer) && v != 1 }

          if total_limit
            perform_limit = total_limit
            enqueue_limit = total_limit
          end

          self.gouda_concurrency_config = {perform_limit:, enqueue_limit:, key:}
        end
      end

      # This method will be tried by the Gouda adapter
      def enqueue_concurrency_key
        job_config = self.class.try(:gouda_concurrency_config)
        return unless job_config
        return unless job_config[:enqueue_limit]

        _gouda_concurrency_extension_key_via_config || _gouda_concurrency_extension_automatic_key_from_class_and_args
      end

      # This method will be tried by the Gouda adapter
      def execution_concurrency_key
        job_config = self.class.try(:gouda_concurrency_config)
        return unless job_config
        return unless job_config[:perform_limit]

        _gouda_concurrency_extension_key_via_config || _gouda_concurrency_extension_automatic_key_from_class_and_args
      end

      # Generates automatic serialized sha1 key
      def _gouda_concurrency_extension_automatic_key_from_class_and_args
        # To have a stable serialization of an ActiveJob we can re-use the method defined by
        # ActiveJob itself. We need to have the job class name and all the arguments, and for arguments
        # which are ActiveRecords or derivatives - we want them converted into global IDs. This also avoids
        # having attributes of the argument ActiveModels contribute to the concurrency key.
        # Add "cursor_position" from job-iteration so that different offsets of the same job can run
        # concurrently.
        pertinent_job_attributes = serialize.slice("job_class", "arguments", "priority", "cursor_position")
        Digest::SHA1.hexdigest(JSON.dump(pertinent_job_attributes))
      end

      # Generates the concurrency key from the configuration
      def _gouda_concurrency_extension_key_via_config
        key = self.class.gouda_concurrency_config[:key]
        return if key.blank?

        key = key.respond_to?(:call) ? instance_exec(&key) : key
        raise TypeError, "Concurrency key must be a String; was a #{key.class}" unless VALID_TYPES.any? { |type| key.is_a?(type) }

        key
      end
    end
  end
end
