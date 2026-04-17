# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionIntegrationTest < Minitest::Test
    def setup
      @database = DuckDB::Database.open
      @connection = @database.connect
    end

    def teardown
      @connection.disconnect
      @database.close
    end

    # Test 1: Simple table function returning data
    # rubocop:disable Minitest/MultipleAssertions
    def test_simple_table_function
      table_function = create_simple_function

      @connection.register_table_function(table_function)
      result = @connection.query('SELECT * FROM simple_function()')

      rows = result.each.to_a

      assert_equal 3, rows.count
      assert_equal [1, 'Alice'], rows[0]
      assert_equal [2, 'Bob'], rows[1]
      assert_equal [3, 'Charlie'], rows[2]
    end
    # rubocop:enable Minitest/MultipleAssertions

    # Test 2: Table function with parameters
    # rubocop:disable Minitest/MultipleAssertions
    def test_table_function_with_parameters
      table_function = create_parameterized_function

      @connection.register_table_function(table_function)
      result = @connection.query("SELECT * FROM repeat_string('hello', 3)")

      rows = result.each.to_a

      assert_equal 3, rows.count
      assert_equal ['test'], rows[0]
      assert_equal ['test'], rows[1]
      assert_equal ['test'], rows[2]
    end
    # rubocop:enable Minitest/MultipleAssertions

    # Verifies that register_table_function works with threads > 1.
    # Callbacks are dispatched to the shared executor thread so correctness
    # is preserved even when DuckDB invokes them from worker threads.
    def test_register_table_function_under_multi_thread_setting
      @connection.execute('SET threads=4')
      table_function = create_simple_function

      @connection.register_table_function(table_function)
      rows = @connection.query('SELECT * FROM simple_function()').each.to_a

      assert_equal [[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']], rows
    end

    # An exception raised inside the execute callback must surface as
    # DuckDB::Error — even when DuckDB invokes the callback from a worker
    # thread. Guards against longjmp escaping through native frames.
    def test_execute_callback_error_propagation_under_multi_thread_setting
      @connection.execute('SET threads=4')

      tf = DuckDB::TableFunction.new
      tf.name = 'raising_function'
      tf.bind { |bind_info| bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT) }
      tf.init { |_init_info| } # rubocop:disable Lint/EmptyBlock
      tf.execute { |_func_info, _output| raise 'boom from execute' }

      @connection.register_table_function(tf)

      error = assert_raises(DuckDB::Error) do
        @connection.query('SELECT * FROM raising_function()').each.to_a
      end
      assert_match(/boom from execute/, error.message)
    end

    # When the total row count spans multiple chunks, execute is invoked
    # repeatedly until size=0 is returned. Exercises the executor dispatch
    # across many invocations under threads > 1.
    def test_execute_callback_multi_chunk_under_multi_thread_setting
      @connection.execute('SET threads=4')
      target_rows = 5000
      emitted = 0

      tf = DuckDB::TableFunction.new
      tf.name = 'multi_chunk_function'
      tf.bind { |bind_info| bind_info.add_result_column('n', DuckDB::LogicalType::BIGINT) }
      tf.init { |_init_info| emitted = 0 }
      tf.execute do |_func_info, output|
        remaining = target_rows - emitted
        if remaining <= 0
          output.size = 0
        else
          batch = [remaining, 1000].min
          batch.times { |i| output.set_value(0, i, emitted + i) }
          output.size = batch
          emitted += batch
        end
      end

      @connection.register_table_function(tf)
      result = @connection.query('SELECT COUNT(*) AS c, SUM(n) AS s FROM multi_chunk_function()').each.to_a

      assert_equal target_rows, result[0][0]
      assert_equal (0...target_rows).sum, result[0][1]
    end

    private

    def create_simple_function
      done = false # Track state with closure variable

      table_function = DuckDB::TableFunction.new
      table_function.name = 'simple_function'

      table_function.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
      end

      table_function.init { |_init_info| done = false } # Reset state

      table_function.execute do |_func_info, output|
        if done
          # Already returned all rows
          output.size = 0
        else
          # Get vectors for both columns
          id_vector = output.get_vector(0)
          name_vector = output.get_vector(1)

          # Write data to id column (BIGINT)
          id_data = id_vector.get_data
          DuckDB::MemoryHelper.write_bigint(id_data, 0, 1)
          DuckDB::MemoryHelper.write_bigint(id_data, 1, 2)
          DuckDB::MemoryHelper.write_bigint(id_data, 2, 3)

          # Write data to name column (VARCHAR)
          name_vector.assign_string_element(0, 'Alice')
          name_vector.assign_string_element(1, 'Bob')
          name_vector.assign_string_element(2, 'Charlie')

          # Set the number of rows
          output.size = 3
          done = true
        end
      end

      table_function
    end

    def create_parameterized_function
      done = false # Track state

      table_function = DuckDB::TableFunction.new
      table_function.name = 'repeat_string'
      table_function.add_parameter(DuckDB::LogicalType::VARCHAR)
      table_function.add_parameter(DuckDB::LogicalType::BIGINT)

      table_function.bind do |bind_info|
        # Just define output schema - we'll read parameters in execute
        bind_info.add_result_column('value', DuckDB::LogicalType::VARCHAR)
      end

      table_function.init { |_init_info| done = false } # Reset state

      table_function.execute do |_func_info, output|
        if done
          output.size = 0
        else
          # For now, just hardcode writing 3 rows
          value_vector = output.get_vector(0)

          value_vector.assign_string_element(0, 'test')
          value_vector.assign_string_element(1, 'test')
          value_vector.assign_string_element(2, 'test')

          output.size = 3
          done = true
        end
      end

      table_function
    end

    def create_minimal_function
      table_function = DuckDB::TableFunction.new
      table_function.name = 'test_func'

      table_function.bind do |bind_info|
        bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
      end

      table_function.init { |_init_info| } # Empty init
      table_function.execute { |_func_info, output| output.size = 0 }

      table_function
    end
  end
end
