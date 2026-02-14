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

    # Test 4: Vector#logical_type returns LogicalType
    def test_vector_logical_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @conn.execute('SET threads=1')

      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_vector_type'

      table_function.bind do |bind_info|
        bind_info.add_result_column('bigint_col', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('varchar_col', DuckDB::LogicalType::VARCHAR)
        bind_info.add_result_column('double_col', DuckDB::LogicalType::DOUBLE)
      end

      table_function.init { |_init_info| } # rubocop:disable Lint/EmptyBlock

      table_function.execute do |_func_info, output|
        # Get vectors
        bigint_vector = output.get_vector(0)
        varchar_vector = output.get_vector(1)
        double_vector = output.get_vector(2)

        # Check logical_type returns LogicalType object
        bigint_type = bigint_vector.logical_type
        varchar_type = varchar_vector.logical_type
        double_type = double_vector.logical_type

        assert_instance_of DuckDB::LogicalType, bigint_type
        assert_instance_of DuckDB::LogicalType, varchar_type
        assert_instance_of DuckDB::LogicalType, double_type

        # Check types match (using Symbol comparison)
        assert_equal :bigint, bigint_type.type
        assert_equal :varchar, varchar_type.type
        assert_equal :double, double_type.type

        output.size = 0
      end

      @conn.register_table_function(table_function)
      @conn.query('SELECT * FROM test_vector_type()')
    end
  end
end
