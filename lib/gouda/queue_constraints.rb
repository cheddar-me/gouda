# frozen_string_literal: true

module Gouda
  # A queue constraint supports just one method, `to_sql`, which returns
  # a condition based on the `queue_name` value of the `gouda_workloads`
  # table. The minimal constraint is just a no-op - it allows execution
  # of jobs from all queues in the system.
  module AnyQueue
    def self.to_sql
      "1=1"
    end
  end

  # Allows execution of jobs only from specified queues
  # For example, if you have a queue named "gpu", and you run
  # jobs requiring a GPU on this queue, on your worker script
  # running on GPU-equipped machines you could use
  # `OnlyQueuesConstraint.new([:gpu])`
  class OnlyQueuesConstraint < Struct.new(:queue_names)
    def to_sql
      placeholders = (["?"] * queue_names.length).join(",")
      Gouda::Workload.sanitize_sql_array([<<~SQL, *queue_names])
        queue_name IN (#{placeholders})
      SQL
    end
  end

  # Allows execution of jobs from queues except the given ones
  # For example, if you have a queue named "emails" which is time-critical,
  # on all other machines your worker script can specify
  # `ExceptQueueConstraint.new([:emails])`
  class ExceptQueueConstraint < Struct.new(:queue_names)
    def to_sql
      placeholders = (["?"] * queue_names.length).join(",")
      Gouda::Workload.sanitize_sql_array([<<~SQL, *queue_names])
        queue_name NOT IN (#{placeholders})
      SQL
    end
  end

  # Parse a string representing a group of queues into a queue constraint
  # Note that this works similar to good_job. For example, the
  # constraints do not necessarily compose all that well.
  #
  # @param queue_constraint_str[String] Queue string
  # @return [Hash]
  #   How to match a given queue. It can have the following keys and values:
  #   - +{ all: true }+ indicates that all queues match.
  #   - +{ exclude: Array<String> }+ indicates the listed queue names should
  #     not match.
  #   - +{ include: Array<String> }+ indicates the listed queue names should
  #     match.
  # @example
  #   Gouda::QueueConstraints.queue_parser('-queue1,queue2')
  #   => { exclude: [ 'queue1', 'queue2' ] }
  def self.parse_queue_constraint(queue_constraint_str)
    string = queue_constraint_str.presence || "*"

    case string.first
    when "-"
      exclude_queues = true
      string = string[1..]
    when "+"
      string = string[1..]
    end

    queues = string.split(",").map(&:strip)

    if queues.include?("*")
      AnyQueue
    elsif exclude_queues
      ExceptQueueConstraint.new([queues])
    else
      OnlyQueuesConstraint.new([queues])
    end
  end
end
