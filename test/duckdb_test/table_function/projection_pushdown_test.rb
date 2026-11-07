# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class TableFunctionProjectionPushdownTest < Minitest::Test
    COLUMNS = %w[id name score].freeze
    ROW = { 'id' => 1, 'name' => 'Alice', 'score' => 90 }.freeze

    def setup
      @database = DuckDB::Database.open
      @connection = @database.connect
    end

    def teardown
      @connection.disconnect
      @database.close
    end

    def test_output_has_only_selected_columns_when_enabled
      observed = register_table_function('pushdown_on_output', pushdown: true)

      @connection.query('SELECT score FROM pushdown_on_output()').to_a

      assert_equal 1, observed[:output_column_count]
    end

    def test_init_reports_selected_columns_in_query_order_when_enabled
      observed = register_table_function('pushdown_on_init', pushdown: true)

      @connection.query('SELECT score, name FROM pushdown_on_init()').to_a

      assert_equal %w[score name], observed[:projected]
    end

    def test_query_returns_selected_columns_when_enabled
      register_table_function('pushdown_on_result', pushdown: true)

      result = @connection.query('SELECT score, name FROM pushdown_on_result()').to_a

      assert_equal [[90, 'Alice']], result
    end

    def test_output_has_all_columns_by_default
      observed = register_table_function('pushdown_default', pushdown: nil)

      result = @connection.query('SELECT score FROM pushdown_default()').to_a

      assert_equal [3, [[90]]], [observed[:output_column_count], result]
    end

    def test_output_has_all_columns_when_disabled
      observed = register_table_function('pushdown_off', pushdown: false)

      result = @connection.query('SELECT score FROM pushdown_off()').to_a

      assert_equal [3, [[90]]], [observed[:output_column_count], result]
    end

    private

    # Registers a table function with columns id, name and score that returns
    # a single row (ROW).
    #
    # The execute block fills the output chunk according to its shape: when the
    # chunk holds every column the whole row is written, otherwise only the
    # columns DuckDB requested in the init stage are written, in that order.
    #
    # Returns a Hash that is filled with what the callbacks observed.
    def register_table_function(name, pushdown:)
      observed = {}

      tf = DuckDB::TableFunction.new
      tf.name = name
      tf.supports_projection_pushdown = pushdown unless pushdown.nil?

      tf.bind do |bind_info|
        bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
        bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
        bind_info.add_result_column('score', DuckDB::LogicalType::BIGINT)
      end

      tf.init do |init_info|
        observed[:projected] = Array.new(init_info.column_count) { |i| COLUMNS[init_info.column_index(i)] }
      end

      done = false
      tf.execute do |_func_info, output|
        observed[:output_column_count] = output.column_count
        if done
          output.size = 0
        else
          cols = output.column_count == COLUMNS.size ? COLUMNS : observed[:projected]
          cols.each_with_index { |col, i| output.set_value(i, 0, ROW[col]) }
          output.size = 1
          done = true
        end
      end

      @connection.register_table_function(tf)
      observed
    end
  end
end
