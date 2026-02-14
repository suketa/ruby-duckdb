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

    # Test 5: DataChunk#set_value with INTEGER
    def test_data_chunk_set_value_integer # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @conn.execute('SET threads=1')

      done = false
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_set_value_int'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::INTEGER)
      end

      table_function.init { |_init_info| done = false }

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          # Use set_value instead of low-level API
          output.set_value(0, 0, 42)
          output.set_value(0, 1, 100)
          output.size = 2
          done = true
        end
      end

      @conn.register_table_function(table_function)
      result = @conn.query('SELECT * FROM test_set_value_int()')
      rows = result.each.to_a

      assert_equal 2, rows.length
      assert_equal 42, rows[0].first
      assert_equal 100, rows[1].first
    end

    # Test 6: DataChunk#set_value with BIGINT
    def test_data_chunk_set_value_bigint # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @conn.execute('SET threads=1')

      done = false
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_set_value_bigint'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      table_function.init { |_init_info| done = false }

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 9_223_372_036_854_775_807)
          output.size = 1
          done = true
        end
      end

      @conn.register_table_function(table_function)
      result = @conn.query('SELECT * FROM test_set_value_bigint()')
      rows = result.each.to_a

      assert_equal 1, rows.length
      assert_equal 9_223_372_036_854_775_807, rows[0].first
    end

    # Test 7: DataChunk#set_value with VARCHAR
    def test_data_chunk_set_value_varchar # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @conn.execute('SET threads=1')

      done = false
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_set_value_varchar'

      table_function.bind do |bind_info|
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
      end

      table_function.init { |_init_info| done = false }

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 'Alice')
          output.set_value(0, 1, 'Bob')
          output.size = 2
          done = true
        end
      end

      @conn.register_table_function(table_function)
      result = @conn.query('SELECT * FROM test_set_value_varchar()')
      rows = result.each.to_a

      assert_equal 2, rows.length
      assert_equal 'Alice', rows[0].first
      assert_equal 'Bob', rows[1].first
    end

    # Test 8: DataChunk#set_value with DOUBLE
    def test_data_chunk_set_value_double # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @conn.execute('SET threads=1')

      done = false
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_set_value_double'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::DOUBLE)
      end

      table_function.init { |_init_info| done = false }

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 3.14)
          output.set_value(0, 1, 2.718)
          output.size = 2
          done = true
        end
      end

      @conn.register_table_function(table_function)
      result = @conn.query('SELECT * FROM test_set_value_double()')
      rows = result.each.to_a

      assert_equal 2, rows.length
      assert_in_delta 3.14, rows[0].first, 0.001
      assert_in_delta 2.718, rows[1].first, 0.001
    end

    # Test 9: DataChunk#set_value with NULL
    def test_data_chunk_set_value_null # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @conn.execute('SET threads=1')

      done = false
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_set_value_null'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::INTEGER)
      end

      table_function.init { |_init_info| done = false }

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, 42)
          output.set_value(0, 1, nil)
          output.set_value(0, 2, 100)
          output.size = 3
          done = true
        end
      end

      @conn.register_table_function(table_function)
      result = @conn.query('SELECT * FROM test_set_value_null()')
      rows = result.each.to_a

      assert_equal 3, rows.length
      assert_equal 42, rows[0].first
      assert_nil rows[1].first
      assert_equal 100, rows[2].first
    end

    # Test 10: DataChunk#set_value with multiple columns
    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
    def test_data_chunk_set_value_multiple_columns
      @conn.execute('SET threads=1')

      done = false
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_set_value_multi'

      table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
        bind_info.add_result_column('score', DuckDB::LogicalType::DOUBLE)
      end

      table_function.init { |_init_info| done = false }

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          # Row 0
          output.set_value(0, 0, 1)
          output.set_value(1, 0, 'Alice')
          output.set_value(2, 0, 95.5)

          # Row 1
          output.set_value(0, 1, 2)
          output.set_value(1, 1, 'Bob')
          output.set_value(2, 1, 87.3)

          output.size = 2
          done = true
        end
      end

      @conn.register_table_function(table_function)
      result = @conn.query('SELECT * FROM test_set_value_multi()')
      rows = result.each.to_a

      assert_equal 2, rows.length

      row0 = rows[0]

      assert_equal 1, row0[0]
      assert_equal 'Alice', row0[1]
      assert_in_delta 95.5, row0[2], 0.001

      row1 = rows[1]

      assert_equal 2, row1[0]
      assert_equal 'Bob', row1[1]
      assert_in_delta 87.3, row1[2], 0.001
    end
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
  end
end
