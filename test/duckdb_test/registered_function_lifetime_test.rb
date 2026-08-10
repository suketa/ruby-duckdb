# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # DuckDB registers C-API functions in the database-level catalog and has no
  # unregister API, so a registration must outlive the connection that made it.
  class RegisteredFunctionLifetimeTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
    end

    def teardown
      @db&.close
    end

    def register_double(con, name)
      con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = name
        sf.add_parameter(DuckDB::LogicalType::INTEGER)
        sf.return_type = DuckDB::LogicalType::INTEGER
        sf.set_function { |v| v * 2 }
      end)
    end

    def test_function_survives_disconnect_of_registering_connection
      con1 = @db.connect
      con2 = @db.connect
      register_double(con1, 'double_after_disconnect')

      con1.disconnect
      3.times { GC.start }

      assert_equal 42, con2.query('SELECT double_after_disconnect(21)').first.first
    end

    def test_function_survives_collection_of_registering_connection
      con2 = @db.connect
      register_double(@db.connect, 'double_after_gc')
      3.times { GC.start }

      assert_equal 42, con2.query('SELECT double_after_gc(21)').first.first
    end

    def test_function_survives_compaction_after_disconnect
      skip 'GC.compact not available' unless GC.respond_to?(:compact)
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      con1 = @db.connect
      con2 = @db.connect
      register_double(con1, 'double_after_compact')

      con1.disconnect
      3.times { GC.compact }

      assert_equal 42, con2.query('SELECT double_after_compact(21)').first.first
    end

    def test_aggregate_function_survives_disconnect
      con1 = @db.connect
      con2 = @db.connect
      con1.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'sum_after_disconnect',
          return_type: :bigint,
          params: [:bigint],
          init: -> { 0 },
          update: ->(state, value) { state + value },
          combine: ->(state, other_state) { state + other_state }
        )
      )

      con1.disconnect
      3.times { GC.start }

      result = con2.query('SELECT sum_after_disconnect(i) FROM range(1, 5) t(i)')

      assert_equal 10, result.first.first
    end

    def test_database_outlives_connection
      con = @db.connect
      @db = nil
      3.times { GC.start }

      assert_equal 1, con.query('SELECT 1').first.first
    end
  end
end
