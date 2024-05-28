# frozen_string_literal: true

module Gouda
  module AnyQueue
    def self.to_sql
      "1=1"
    end
  end

  class OnlyQueuesConstraint < Struct.new(:queue_names)
    def to_sql
      placeholders = (["?"] * queue_names.length).join(",")
      ActiveRecord::Base.sanitize_sql_array([<<~SQL, *queue_names])
        queue_name IN (#{placeholders})
      SQL
    end
  end

  class ExceptQueueConstraint < Struct.new(:queue_names)
    def to_sql
      placeholders = (["?"] * queue_names.length).join(",")
      ActiveRecord::Base.sanitize_sql_array([<<~SQL, *queue_names])
        queue_name NOT IN (#{placeholders})
      SQL
    end
  end

  def self.parse_queue_constraint(constraint_str_from_envvar)
    parsed = queue_parser(constraint_str_from_envvar)
    if parsed[:include]
      OnlyQueuesConstraint.new(parsed[:include])
    elsif parsed[:exclude]
      ExceptQueueConstraint.new(parsed[:exclude])
    else
      AnyQueue
    end
  end

  # Parse a string representing a group of queues into a more readable data
  # structure.
  # @param string [String] Queue string
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
  def self.queue_parser(string)
    string = string.presence || "*"

    case string.first
    when "-"
      exclude_queues = true
      string = string[1..]
    when "+"
      string = string[1..]
    end

    queues = string.split(",").map(&:strip)

    if queues.include?("*")
      {all: true}
    elsif exclude_queues
      {exclude: queues}
    else
      {include: queues}
    end
  end
end
