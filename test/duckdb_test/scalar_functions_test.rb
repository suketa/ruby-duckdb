# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ScalarFunctionsTest < Minitest::Test
    # Parameters count
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

    def test_scalar_with_too_much_parameters
      DuckDB::Database.open do |db|
        db.connect do |con|
          error = assert_raises(StandardError) do
            con.register_scalar_function("my_func", return_type: :integer, parameter_types: Array.new(17, :text)) do
              nil
            end
          end
          assert_equal 'Too much parameters added to the scalar function (ruby-duckdb internal limit)', error.message
        end
      end
    end

    # NULL handling
    def test_binary_scalar_function_returning_null_text
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :text, parameter_types: [:text, :text]) do |x1, x2|
            nil
          end

          result = con.query("SELECT my_func('A', 'B')")
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [[nil]])
        end
      end
    end

    # Implementation shape
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

    # Raised error during execution
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

    # VOLATILE
    def test_scalar_function_are_not_volatile_by_default
      DuckDB::Database.open do |db|
        db.connect do |con|
          i = -3
          con.register_scalar_function("my_func", return_type: :integer) do
            i += 1
          end

          result = con.query("SELECT my_func() FROM range(2)")
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [[-2], [-2]])
        end
      end
    end

    def test_scalar_function_can_be_marked_as_volatile
      DuckDB::Database.open do |db|
        db.connect do |con|
          i = -3
          con.register_scalar_function("my_func", return_type: :integer, volatile: true) do
            i += 1
          end

          result = con.query("SELECT my_func() FROM range(2)")
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [[-2], [-1]])
        end
      end
    end

    # TEXT type
    def test_scalar_function_with_text_as_input_output
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :text, parameter_types: [:text]) do |x1|
            "#{x1}-world"
          end

          result = con.query("SELECT my_func('hello')")
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [['hello-world']])
        end
      end
    end

    def test_scalar_function_invalid_text_returned
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :text) { 6 }

          error = assert_raises(StandardError) { con.query("SELECT my_func()") }
          assert_equal 'Invalid Input Error: Returned value from UDF is not a text', error.message
        end
      end
    end
    
    # INTEGER type
    def test_scalar_function_with_integer_as_input_output
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :integer, parameter_types: [:integer]) do |x1|
            x1 + 10
          end

          result = con.query("SELECT my_func(8)")
          assert_equal(result.column_count, 1)
          assert_equal(result.to_a, [[18]])
        end
      end
    end

    def test_scalar_function_invalid_integer_returned
      DuckDB::Database.open do |db|
        db.connect do |con|
          con.register_scalar_function("my_func", return_type: :integer) { 'ABC' }

          error = assert_raises(StandardError) { con.query("SELECT my_func()") }
          assert_equal 'Invalid Input Error: Returned value from UDF is not an integer', error.message
        end
      end
    end
  end
end
