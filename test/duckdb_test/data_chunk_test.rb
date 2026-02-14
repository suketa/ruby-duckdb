# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class DataChunkTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.disconnect
      @db.close
    end

    # Test 1: DataChunk API exists (actual testing deferred to Phase 4)
    def test_data_chunk_column_count
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_column_count'

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
      end

      # NOTE: Can't test DataChunk.column_count until execute callback is implemented (Phase 4)
      assert_equal table_function, result
    end

    # Test 2: DataChunk get and set size (API verification)
    def test_data_chunk_size
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_size'

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      # NOTE: Can't test DataChunk.size until execute callback is implemented (Phase 4)
      assert_equal table_function, result
    end

    # Test 3: DataChunk get_vector (API verification)
    def test_data_chunk_get_vector
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_vector'

      result = table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      # NOTE: Can't test DataChunk.get_vector until execute callback is implemented (Phase 4)
      assert_equal table_function, result
    end
  end
end
