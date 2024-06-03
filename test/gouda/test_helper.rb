# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "debug"
require "active_record"
require "active_job"
require "active_support/test_case"
require "minitest/autorun"
require "minitest"
require "support/assert_helper"
require "gouda"

begin
  ActiveRecord::Migration.maintain_test_schema!
rescue ActiveRecord::PendingMigrationError => e
  puts e.to_s.strip
  exit 1
end

class ActiveSupport::TestCase
  SEED_DB_NAME = -> { "gouda_tests_%s" % Random.new(Minitest.seed).hex(4) }

  def self.adapter
    @adapter || Gouda::Adapter.new
  end

  attr_reader :case_random

  setup do
    # create_postgres_database_if_none
    @case_random = Random.new(Minitest.seed)
  end

  teardown do
    # truncate_test_tables
  end

  def create_postgres_database_if_none
    ActiveRecord::Base.establish_connection(adapter: "postgresql", encoding: "unicode", database: SEED_DB_NAME.call)
    ActiveRecord::Base.connection.execute("SELECT 1 FROM gouda_workloads")
  rescue ActiveRecord::NoDatabaseError, ActiveRecord::ConnectionNotEstablished
    create_postgres_database
    retry
  rescue ActiveRecord::StatementInvalid
    ActiveRecord::Schema.define(version: 1) do |via_definer|
      Gouda.create_tables(via_definer)
    end
    retry
  end

  def create_postgres_database
    ActiveRecord::Migration.verbose = false
    ActiveRecord::Base.establish_connection(adapter: "postgresql", database: "postgres")
    ActiveRecord::Base.connection.create_database(SEED_DB_NAME.call, charset: :unicode)
    ActiveRecord::Base.connection.close
    ActiveRecord::Base.establish_connection(adapter: "postgresql", encoding: "unicode", database: SEED_DB_NAME.call)
  end

  def truncate_test_tables
    ActiveRecord::Base.connection.execute("TRUNCATE TABLE gouda_workloads")
    ActiveRecord::Base.connection.execute("TRUNCATE TABLE gouda_job_fuses")
  end

  def test_create_tables
    ActiveRecord::Base.transaction do
      ActiveRecord::Base.connection.execute("DROP TABLE gouda_workloads")
      ActiveRecord::Base.connection.execute("DROP TABLE gouda_job_fuses")
      # The adapter has to be in a variable as the schema definition is scoped to the migrator, not self
      ActiveRecord::Schema.define(version: 1) do |via_definer|
        Gouda.create_tables(via_definer)
      end
    end
  end
end
