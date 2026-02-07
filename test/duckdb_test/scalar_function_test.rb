# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ScalarFunctionTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con&.close
      @db&.close
    end

    def test_initialize
      sf = DuckDB::ScalarFunction.new

      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_name_setter
      sf = DuckDB::ScalarFunction.new
      sf.name = 'test_function'

      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_return_type_setter
      sf = DuckDB::ScalarFunction.new
      logical_type = DuckDB::LogicalType.new(4) # DUCKDB_TYPE_INTEGER
      sf.return_type = logical_type

      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_return_type_setter_raises_error_for_unsupported_type
      sf = DuckDB::ScalarFunction.new
      timestamp_type = DuckDB::LogicalType.new(13) # DUCKDB_TYPE_TIMESTAMP (unsupported)

      error = assert_raises(DuckDB::Error) do
        sf.return_type = timestamp_type
      end

      assert_match(/only.*supported/i, error.message)
    end

    def test_set_function
      sf = DuckDB::ScalarFunction.new
      sf1 = sf.set_function { 1 }

      assert_instance_of DuckDB::ScalarFunction, sf1
      assert_equal sf1.__id__, sf.__id__
    end

    def test_register_scalar_function
      # Scalar functions with Ruby callbacks require single-threaded execution
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'foo'
      sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
      sf.set_function { 1 }

      @con.register_scalar_function(sf)

      result = @con.execute('SELECT foo()')

      assert_equal 1, result.first.first
    end

    def test_register_scalar_function_raises_error_without_single_thread
      sf = DuckDB::ScalarFunction.new
      sf.name = 'will_fail'
      sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
      sf.set_function { 1 }

      # Should raise error because threads is not 1
      error = assert_raises(DuckDB::Error) do
        @con.register_scalar_function(sf)
      end

      assert_match(/single-threaded execution/, error.message)
      assert_match(/SET threads=1/, error.message)
    end

    def test_add_parameter
      sf = DuckDB::ScalarFunction.new
      logical_type = DuckDB::LogicalType.new(4) # DUCKDB_TYPE_INTEGER

      result = sf.add_parameter(logical_type)

      assert_instance_of DuckDB::ScalarFunction, result
      assert_equal sf.__id__, result.__id__
    end

    def test_scalar_function_with_one_parameter # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value INTEGER)')
      @con.execute('INSERT INTO test_table VALUES (5), (10), (15)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double'
      sf.add_parameter(DuckDB::LogicalType.new(4)) # INTEGER
      sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
      sf.set_function { |col1| 2 * col1 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT double(value) FROM test_table ORDER BY value')

      assert_equal [[10], [20], [30]], result.to_a
    end

    def test_scalar_function_with_two_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (a INTEGER, b INTEGER)')
      @con.execute('INSERT INTO test_table VALUES (5, 3), (10, 2), (15, 4)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_nums'
      sf.add_parameter(DuckDB::LogicalType.new(4)) # INTEGER
      sf.add_parameter(DuckDB::LogicalType.new(4)) # INTEGER
      sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
      sf.set_function { |a, b| a + b }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_nums(a, b) FROM test_table ORDER BY a')

      assert_equal [[8], [12], [19]], result.to_a
    end

    def test_scalar_function_with_null_input # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value INTEGER)')
      @con.execute('INSERT INTO test_table VALUES (5), (NULL), (15)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double'
      sf.add_parameter(DuckDB::LogicalType.new(4)) # INTEGER
      sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
      sf.set_function { |col1| col1.nil? ? nil : 2 * col1 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT double(value) FROM test_table ORDER BY value')

      assert_equal [[10], [30], [nil]], result.to_a
    end

    def test_scalar_function_bigint_return_type # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value BIGINT)')
      @con.execute('INSERT INTO test_table VALUES (9223372036854775807)') # Max int64

      sf = DuckDB::ScalarFunction.new
      sf.name = 'subtract_one'
      sf.add_parameter(DuckDB::LogicalType.new(5)) # BIGINT
      sf.return_type = DuckDB::LogicalType.new(5) # BIGINT
      sf.set_function { |v| v - 1 } # Subtract to avoid overflow

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT subtract_one(value) FROM test_table')

      assert_equal 9_223_372_036_854_775_806, result.first.first
    end

    def test_scalar_function_double_return_type # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value DOUBLE)')
      @con.execute('INSERT INTO test_table VALUES (3.14159)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'multiply_by_two'
      sf.add_parameter(DuckDB::LogicalType.new(11)) # DOUBLE (type ID 11)
      sf.return_type = DuckDB::LogicalType.new(11) # DOUBLE
      sf.set_function { |v| v * 2 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT multiply_by_two(value) FROM test_table')

      assert_in_delta 6.28318, result.first.first, 0.00001
    end

    def test_scalar_function_boolean_return_type # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value INTEGER)')
      @con.execute('INSERT INTO test_table VALUES (5), (10), (15)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'is_greater_than_ten'
      sf.add_parameter(DuckDB::LogicalType.new(4)) # INTEGER
      sf.return_type = DuckDB::LogicalType.new(1) # BOOLEAN (type ID 1)
      sf.set_function { |v| v > 10 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT is_greater_than_ten(value) FROM test_table ORDER BY value')

      assert_equal [[false], [false], [true]], result.to_a
    end

    def test_scalar_function_float_return_type # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value FLOAT)')
      @con.execute('INSERT INTO test_table VALUES (2.5)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_half'
      sf.add_parameter(DuckDB::LogicalType.new(10)) # FLOAT (type ID 10)
      sf.return_type = DuckDB::LogicalType.new(10) # FLOAT
      sf.set_function { |v| v + 0.5 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_half(value) FROM test_table')

      assert_in_delta 3.0, result.first.first, 0.0001
    end

    def test_scalar_function_varchar_return_type # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (name VARCHAR)')
      @con.execute("INSERT INTO test_table VALUES ('Alice'), ('Bob')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_greeting'
      sf.add_parameter(DuckDB::LogicalType.new(17)) # VARCHAR (type ID 17)
      sf.return_type = DuckDB::LogicalType.new(17) # VARCHAR
      sf.set_function { |name| "Hello, #{name}!" }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_greeting(name) FROM test_table ORDER BY name')

      assert_equal [['Hello, Alice!'], ['Hello, Bob!']], result.to_a
    end

    def test_scalar_function_blob_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (data BLOB)')
      @con.execute("INSERT INTO test_table VALUES ('\\x00\\x01\\x02\\x03'::BLOB), ('\\x00\\xAA\\xBB\\xCC'::BLOB)")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_prefix'
      sf.add_parameter(DuckDB::LogicalType.new(18)) # BLOB (type ID 18)
      sf.return_type = DuckDB::LogicalType.new(18) # BLOB
      sf.set_function { |data| DuckDB::Blob.new("\xFF".b + data) }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_prefix(data) FROM test_table ORDER BY data')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal "\xFF\x00\x01\x02\x03".b, rows[0][0]
      assert_equal "\xFF\x00\xAA\xBB\xCC".b, rows[1][0]
    end
  end
end
