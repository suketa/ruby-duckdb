# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ScalarFunctionsTest < Minitest::Test
    def test_nullary_scalar_function_returning_text
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :text) do
            'Hello from ruby-duckdb'
          end

          result = con.query('SELECT my_func()')
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [["Hello from ruby-duckdb"]])
        end
      end
    end

    def test_scalar_function_defined_in_a_block
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :text) do
            'From a block'
          end

          result = con.query('SELECT my_func()')
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [["From a block"]])
        end
      end
    end

    def test_scalar_function_defined_in_a_proc
      DuckDB::Database.open do |db|
        db.connect do |con|
          my_func_impl = Proc.new do
            "From a Proc"
          end

          con.register_scalar_function("my_func", my_func_impl, return_type: :text)

          result = con.query('SELECT my_func()')
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [["From a Proc"]])
        end
      end
    end

    def test_binary_scalar_function_returning_text
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :text, parameter_types: [:text, :text]) do |x1, x2|
            [x1, x1, x2].join("-")
          end

          result = con.query("SELECT my_func('A', 'B')")
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [["A-A-B"]])
        end
      end
    end

    def test_binary_scalar_function_returning_integer
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :integer, parameter_types: [:integer, :integer]) do |x1, x2|
            (x1 + x1) * x2
          end

          result = con.query("SELECT my_func(2, 5)")
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [[20]])
        end
      end
    end

    def test_scalar_function_raising_an_error
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :integer) do
            raise StandardError, 'BOOM'
          end

          error = assert_raises(StandardError) { con.query("SELECT my_func()") }
          assert_equal 'Invalid Input Error: Ruby error raise while executing the UDF: BOOM', error.message
        end
      end
    end

    # TODO: add tests for:
    # TODO:   - VOLATILE
  end
end
