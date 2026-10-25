# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  # Finite TIMESTAMP values near INT64_MIN make duckdb_from_timestamp throw a
  # C++ exception. Decoding them must raise instead of aborting the process.
  # The alias keeps DuckDB from stringifying the value for the column name,
  # which would fail inside DuckDB before the result reaches Ruby.
  class ResultTimestampOutOfRangeTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con.close
      @db.close
    end

    def test_int64_min_timestamp
      result = @con.query('SELECT make_timestamp((-9223372036854775807 - 1)::BIGINT) AS t')

      assert_raises(RangeError) { result.to_a }
    end

    def test_first_out_of_range_timestamp
      result = @con.query('SELECT make_timestamp(-9223372022400000001::BIGINT) AS t')

      assert_raises(RangeError) { result.to_a }
    end

    def test_min_convertible_timestamp
      result = @con.query('SELECT make_timestamp(-9223372022400000000::BIGINT) AS t')

      assert_instance_of(Time, result.first.first)
    end
  end
end
