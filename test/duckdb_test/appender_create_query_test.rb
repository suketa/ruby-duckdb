# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # create_query passes the types array's length to DuckDB as the count of both
  # the types and the column names, so a column name list of a different length
  # made DuckDB read uninitialized stack slots as names.
  class AppenderCreateQueryTest < Minitest::Test
    QUERY = 'INSERT INTO t SELECT i, val FROM my_appended_data'

    # to_str returns a fresh String, so nothing but the C local holds it.
    class ColumnName
      def initialize(name)
        @name = name
      end

      def to_str
        @name.dup
      end
    end

    def setup
      skip 'not supported' unless DuckDB::Appender.respond_to?(:create_query)

      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query('CREATE TABLE t (i INT, val VARCHAR)')
    end

    def teardown
      GC.stress = false
      @con&.close
      @db&.close
    end

    def types
      [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]
    end

    def create_query_appender(column_names)
      DuckDB::Appender.create_query(@con, QUERY, types, 'my_appended_data', column_names)
    end

    def with_gc_stress
      GC.stress = true
      yield
    ensure
      GC.stress = false
    end

    def test_fewer_column_names_than_types_raises_argument_error
      error = assert_raises(ArgumentError) { create_query_appender(%w[i]) }

      assert_match(/column names size \(1\).+types size \(2\)/, error.message)
    end

    def test_more_column_names_than_types_raises_argument_error
      assert_raises(ArgumentError) { create_query_appender(%w[i val extra]) }
    end

    def test_empty_column_names_raises_argument_error
      assert_raises(ArgumentError) { create_query_appender([]) }
    end

    def test_nil_column_names_falls_back_to_the_duckdb_defaults
      appender = DuckDB::Appender.create_query(
        @con, 'INSERT INTO t SELECT col1, col2 FROM my_appended_data', types, 'my_appended_data', nil
      )
      appender.append_row(1, 'hello')
      appender.close

      assert_equal [[1, 'hello']], @con.query('SELECT * FROM t').to_a
    end

    def test_column_names_survive_gc_while_the_appender_is_created
      appender = with_gc_stress { create_query_appender([ColumnName.new('i'), ColumnName.new('val')]) }
      appender.append_row(1, 'hello')
      appender.close

      assert_equal [[1, 'hello']], @con.query('SELECT * FROM t').to_a
    end
  end
end
