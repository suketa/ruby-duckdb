# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # UDF errors reach DuckDB through C string entry points, so a message the C
  # API cannot carry used to raise out of the reporter itself. On a DuckDB
  # worker thread that killed the executor thread, and every later UDF callback
  # waited for a completion signal that never came.
  class FunctionErrorMessageTest < Minitest::Test
    class MessageRaises < StandardError
      def message
        raise 'message itself raises'
      end
    end

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      # More than one row group, so the scan is parallel and the callbacks run
      # on DuckDB worker threads rather than inline on the calling thread.
      @con.query('CREATE TABLE t AS SELECT (i % 7)::VARCHAR AS a FROM range(300000) s(i)')
    end

    def teardown
      @con&.close
      @db&.close
    end

    def register_scalar(name, &block)
      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = name
        sf.add_parameter(DuckDB::LogicalType::VARCHAR)
        sf.return_type = DuckDB::LogicalType::VARCHAR
        sf.set_function(&block)
      end)
    end

    # A regression here strands the dispatcher, and the queries below then never
    # return. Nothing in Ruby can bound that: the stranded state holds the GVL,
    # so Thread#join timeouts never fire and even SIGTERM is not delivered. The
    # bound lives in CI instead, as timeout-minutes on the test step.
    def test_scalar_error_message_with_a_null_byte_is_reported_to_duckdb
      register_scalar('nul_boom') { |v| raise "bad token: \0#{v}" }

      error = assert_raises(DuckDB::Error) do
        @con.query('SELECT nul_boom(a) FROM t')
      end

      assert_match(/bad token:/, error.message)
    end

    def test_scalar_error_message_that_cannot_be_read_is_reported_to_duckdb
      register_scalar('unreadable_boom') { |_v| raise MessageRaises }

      assert_raises(DuckDB::Error) do
        @con.query('SELECT unreadable_boom(a) FROM t')
      end
    end

    def test_aggregate_error_message_with_a_null_byte_is_reported_to_duckdb
      @con.register_aggregate_function(
        DuckDB::AggregateFunction.create(
          name: 'nul_boom_agg',
          return_type: :varchar,
          params: [:varchar],
          init: -> { +'' },
          update: ->(_state, v) { raise "bad token: \0#{v}" },
          combine: ->(state, _other) { state },
          finalize: ->(state) { state }
        )
      )

      error = assert_raises(DuckDB::Error) do
        @con.query('SELECT nul_boom_agg(a) FROM t')
      end

      assert_match(/bad token:/, error.message)
    end

    def test_udf_dispatch_survives_an_error_message_duckdb_cannot_be_given
      register_scalar('nul_boom2') { |v| raise "bad token: \0#{v}" }
      register_scalar('echo') { |v| "echo#{v}" }

      # Deliberately loose: the reporting path is asserted above, and a regression
      # there must not fail this test before it reaches the recovery check.
      assert_raises(StandardError) do
        @con.query('SELECT nul_boom2(a) FROM t')
      end

      result = @con.query('SELECT echo(a) FROM t LIMIT 1').to_a

      assert_equal [['echo0']], result
    end
  end
end
