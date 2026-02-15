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
      logical_type = DuckDB::LogicalType::INTEGER
      sf.return_type = logical_type

      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_return_type_setter_raises_error_for_unsupported_type
      sf = DuckDB::ScalarFunction.new
      interval_type = DuckDB::LogicalType::INTERVAL # Unsupported type for testing

      error = assert_raises(DuckDB::Error) do
        sf.return_type = interval_type
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
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { 1 }

      @con.register_scalar_function(sf)

      result = @con.execute('SELECT foo()')

      assert_equal 1, result.first.first
    end

    def test_register_scalar_function_raises_error_without_single_thread
      sf = DuckDB::ScalarFunction.new
      sf.name = 'will_fail'
      sf.return_type = DuckDB::LogicalType::INTEGER
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
      logical_type = DuckDB::LogicalType::INTEGER

      result = sf.add_parameter(logical_type)

      assert_instance_of DuckDB::ScalarFunction, result
      assert_equal sf.__id__, result.__id__
    end

    def test_add_parameter_raises_error_for_unsupported_type
      sf = DuckDB::ScalarFunction.new
      interval_type = DuckDB::LogicalType::INTERVAL # Unsupported type for testing

      error = assert_raises(DuckDB::Error) do
        sf.add_parameter(interval_type)
      end

      assert_match(/only.*parameter types.*supported/i, error.message)
    end

    def test_add_parameter_raises_error_for_invalid_argument
      sf = DuckDB::ScalarFunction.new

      error = assert_raises(DuckDB::Error) do
        sf.add_parameter('not a logical type')
      end

      assert_match(/must be a DuckDB::LogicalType/i, error.message)
    end

    def test_scalar_function_with_one_parameter # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value INTEGER)')
      @con.execute('INSERT INTO test_table VALUES (5), (10), (15)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double'
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |col1| 2 * col1 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT double(value) FROM test_table ORDER BY value')

      assert_equal [[10], [20], [30]], result.to_a
    end

    def test_scalar_function_with_two_parameters # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (a INTEGER, b INTEGER)')
      @con.execute('INSERT INTO test_table VALUES (5, 3), (10, 2), (15, 4)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_nums'
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::INTEGER
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
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::INTEGER
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
      sf.add_parameter(DuckDB::LogicalType::BIGINT)
      sf.return_type = DuckDB::LogicalType::BIGINT
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
      sf.add_parameter(DuckDB::LogicalType::DOUBLE)
      sf.return_type = DuckDB::LogicalType::DOUBLE # DOUBLE
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
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::BOOLEAN # BOOLEAN (type ID 1)
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
      sf.add_parameter(DuckDB::LogicalType::FLOAT)
      sf.return_type = DuckDB::LogicalType::FLOAT # FLOAT
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
      sf.add_parameter(DuckDB::LogicalType::VARCHAR)
      sf.return_type = DuckDB::LogicalType::VARCHAR # VARCHAR
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
      sf.add_parameter(DuckDB::LogicalType::BLOB)
      sf.return_type = DuckDB::LogicalType::BLOB # BLOB
      sf.set_function { |data| DuckDB::Blob.new("\xFF".b + data) }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_prefix(data) FROM test_table ORDER BY data')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal "\xFF\x00\x01\x02\x03".b, rows[0][0]
      assert_equal "\xFF\x00\xAA\xBB\xCC".b, rows[1][0]
    end

    def test_scalar_function_timestamp_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00'), ('2024-12-25 23:59:59')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_one_hour'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP)
      sf.return_type = DuckDB::LogicalType::TIMESTAMP # TIMESTAMP
      sf.set_function { |ts| ts + 3600 } # Add 1 hour (3600 seconds)

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_one_hour(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal Time.new(2024, 1, 15, 11, 30, 0), rows[0][0]
      assert_equal Time.new(2024, 12, 26, 0, 59, 59), rows[1][0]
    end

    def test_scalar_function_date_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (d DATE)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15'), ('2024-12-25')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_one_day'
      sf.add_parameter(DuckDB::LogicalType::DATE)
      sf.return_type = DuckDB::LogicalType::DATE # DATE
      sf.set_function { |date| date + 1 } # Add 1 day

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_one_day(d) FROM test_table ORDER BY d')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal Date.new(2024, 1, 16), rows[0][0]
      assert_equal Date.new(2024, 12, 26), rows[1][0]
    end

    def test_scalar_function_time_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (t TIME)')
      @con.execute("INSERT INTO test_table VALUES ('10:30:00'), ('23:59:59')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_one_hour'
      sf.add_parameter(DuckDB::LogicalType::TIME)
      sf.return_type = DuckDB::LogicalType::TIME # TIME
      sf.set_function { |time| time + 3600 } # Add 1 hour (3600 seconds)

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_one_hour(t) FROM test_table ORDER BY t')
      rows = result.to_a

      assert_equal 2, rows.size
      # TIME values are returned as Time objects with today's date
      assert_equal 11, rows[0][0].hour
      assert_equal 30, rows[0][0].min
      assert_equal 0, rows[1][0].hour
      assert_equal 59, rows[1][0].min
      assert_equal 59, rows[1][0].min
    end

    def test_scalar_function_smallint_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value SMALLINT)')
      @con.execute('INSERT INTO test_table VALUES (32767), (-32768), (1000)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_100'
      sf.add_parameter(DuckDB::LogicalType::SMALLINT)
      sf.return_type = DuckDB::LogicalType::SMALLINT # SMALLINT
      sf.set_function { |v| v + 100 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_100(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      assert_equal(-32_668, rows[0][0]) # -32768 + 100
      assert_equal 1100, rows[1][0] # 1000 + 100
      assert_equal(-32_669, rows[2][0]) # 32767 + 100 = 32867, overflows to -32669
    end

    def test_scalar_function_tinyint_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value TINYINT)')
      @con.execute('INSERT INTO test_table VALUES (100), (-50), (0)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double_value'
      sf.add_parameter(DuckDB::LogicalType::TINYINT)
      sf.return_type = DuckDB::LogicalType::TINYINT # TINYINT
      sf.set_function { |v| v * 2 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT double_value(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      assert_equal(-100, rows[0][0]) # -50 * 2
      assert_equal 0, rows[1][0]     # 0 * 2
      assert_equal(-56, rows[2][0])  # 100 * 2 = 200, overflows to -56
    end

    def test_scalar_function_utinyint_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value UTINYINT)')
      @con.execute('INSERT INTO test_table VALUES (255), (0), (100)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_10'
      sf.add_parameter(DuckDB::LogicalType::UTINYINT)
      sf.return_type = DuckDB::LogicalType::UTINYINT # UTINYINT
      sf.set_function { |v| v + 10 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_10(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      assert_equal 10, rows[0][0]  # 0 + 10
      assert_equal 110, rows[1][0] # 100 + 10
      assert_equal 110, rows[1][0] # 100 + 10
      assert_equal 9, rows[2][0]   # 255 + 10 = 265, overflows to 9
    end

    def test_scalar_function_usmallint_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value USMALLINT)')
      @con.execute('INSERT INTO test_table VALUES (65535), (0), (1000)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_100'
      sf.add_parameter(DuckDB::LogicalType::USMALLINT)
      sf.return_type = DuckDB::LogicalType::USMALLINT # USMALLINT
      sf.set_function { |v| v + 100 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_100(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      assert_equal 100, rows[0][0]   # 0 + 100
      assert_equal 1100, rows[1][0]  # 1000 + 100
      assert_equal 99, rows[2][0]    # 65535 + 100 = 65635, overflows to 99
    end

    def test_scalar_function_uinteger_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value UINTEGER)')
      @con.execute('INSERT INTO test_table VALUES (4294967200), (0), (1000000)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_100'
      sf.add_parameter(DuckDB::LogicalType::UINTEGER)
      sf.return_type = DuckDB::LogicalType::UINTEGER # UINTEGER
      sf.set_function { |v| v + 100 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_100(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      assert_equal 100, rows[0][0] # 0 + 100
      assert_equal 1_000_100, rows[1][0] # 1000000 + 100
      assert_equal 4, rows[2][0] # 4294967200 + 100 = 4294967300, overflows to 4
    end

    def test_scalar_function_ubigint_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value UBIGINT)')
      @con.execute('INSERT INTO test_table VALUES (9223372036854775807), (0), (1000000000)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double_value'
      sf.add_parameter(DuckDB::LogicalType::UBIGINT)
      sf.return_type = DuckDB::LogicalType::UBIGINT # UBIGINT
      sf.set_function { |v| v * 2 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT double_value(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      assert_equal 0, rows[0][0] # 0 * 2
      assert_equal 2_000_000_000, rows[1][0] # 1000000000 * 2
      assert_equal 18_446_744_073_709_551_614, rows[2][0] # 9223372036854775807 * 2
    end

    def test_scalar_function_gc_safety # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('SET threads=1')

      # Register function and immediately lose reference
      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = 'test_func'
        sf.return_type = DuckDB::LogicalType::INTEGER
        sf.set_function { 42 }
      end)

      # Force aggressive GC to try to collect the ScalarFunction object
      old_stress = GC.stress
      GC.stress = true

      begin
        3.times { GC.start }

        # Should NOT crash - the connection keeps the function alive
        result = @con.execute('SELECT test_func()')
        rows = result.to_a

        assert_equal 1, rows.size
        assert_equal 42, rows[0][0]
      ensure
        GC.stress = old_stress
      end
    end

    def test_gc_compaction_safety # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      skip 'GC.compact not available' unless GC.respond_to?(:compact)
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      @con.execute('SET threads=1')

      # Register scalar function with callback that captures local variable
      multiplier = 10
      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = 'multiply_by_ten'
        sf.add_parameter(DuckDB::LogicalType::INTEGER)
        sf.return_type = DuckDB::LogicalType::INTEGER
        sf.set_function { |v| v * multiplier }
      end)

      # Force GC compaction - this may move the Proc object
      GC.compact

      # Execute query multiple times to ensure callback still works
      5.times do
        result = @con.execute('SELECT multiply_by_ten(7)')

        assert_equal 70, result.first.first, 'Callback failed after GC compaction'
      end

      # Force another compaction and test again
      GC.compact
      result = @con.execute('SELECT multiply_by_ten(3)')

      assert_equal 30, result.first.first
    end

    def test_gc_compaction_with_table_scan # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      skip 'GC.compact not available' unless GC.respond_to?(:compact)
      skip 'GC.compact hangs on Windows in parallel test execution' if Gem.win_platform?

      @con.execute('SET threads=1')
      @con.execute('CREATE TABLE test_table (value INTEGER)')
      @con.execute('INSERT INTO test_table VALUES (1), (2), (3), (4), (5)')

      # Register function
      @con.register_scalar_function(DuckDB::ScalarFunction.new.tap do |sf|
        sf.name = 'square'
        sf.add_parameter(DuckDB::LogicalType::INTEGER)
        sf.return_type = DuckDB::LogicalType::INTEGER
        sf.set_function { |v| v * v }
      end)

      # Compact and query
      GC.compact
      result = @con.execute('SELECT square(value) FROM test_table ORDER BY value')

      assert_equal [[1], [4], [9], [16], [25]], result.to_a

      # Compact again and query again
      GC.compact
      result = @con.execute('SELECT square(value) FROM test_table ORDER BY value')

      assert_equal [[1], [4], [9], [16], [25]], result.to_a
    end

    # Tests for ScalarFunction.create class method

    def test_create_with_single_parameter # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :triple,
        return_type: DuckDB::LogicalType::INTEGER,
        parameter_type: DuckDB::LogicalType::INTEGER
      ) { |v| v * 3 }

      assert_instance_of DuckDB::ScalarFunction, sf

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT triple(5)')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal 15, rows[0][0]
    end

    def test_create_with_multiple_parameters # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :add_numbers,
        return_type: DuckDB::LogicalType::INTEGER,
        parameter_types: [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::INTEGER]
      ) { |a, b| a + b }

      assert_instance_of DuckDB::ScalarFunction, sf

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_numbers(10, 20)')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal 30, rows[0][0]
    end

    def test_create_with_no_parameters # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :constant_value,
        return_type: DuckDB::LogicalType::INTEGER
      ) { 42 }

      assert_instance_of DuckDB::ScalarFunction, sf

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT constant_value()')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal 42, rows[0][0]
    end

    def test_create_requires_block
      error = assert_raises(ArgumentError) do
        DuckDB::ScalarFunction.create(
          name: :test,
          return_type: DuckDB::LogicalType::INTEGER
        )
      end

      assert_match(/block required/i, error.message)
    end

    def test_create_rejects_both_parameter_type_and_parameter_types
      error = assert_raises(ArgumentError) do
        DuckDB::ScalarFunction.create(
          name: :test,
          return_type: DuckDB::LogicalType::INTEGER,
          parameter_type: DuckDB::LogicalType::INTEGER,
          parameter_types: [DuckDB::LogicalType::INTEGER]
        ) { |v| v }
      end

      assert_match(/cannot specify both/i, error.message)
    end

    def test_create_accepts_symbol_for_name
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :symbol_name,
        return_type: DuckDB::LogicalType::INTEGER
      ) { 123 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT symbol_name()')
      rows = result.to_a

      assert_equal 123, rows[0][0]
    end

    def test_create_accepts_string_for_name
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: 'string_name',
        return_type: DuckDB::LogicalType::INTEGER
      ) { 456 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT string_name()')
      rows = result.to_a

      assert_equal 456, rows[0][0]
    end

    def test_create_with_different_types # rubocop:disable Metrics/MethodLength
      @con.execute('SET threads=1')

      sf = DuckDB::ScalarFunction.create(
        name: :concat_with_separator,
        return_type: DuckDB::LogicalType::VARCHAR,
        parameter_types: [
          DuckDB::LogicalType::VARCHAR,
          DuckDB::LogicalType::VARCHAR,
          DuckDB::LogicalType::VARCHAR
        ]
      ) { |a, sep, b| "#{a}#{sep}#{b}" }

      @con.register_scalar_function(sf)
      result = @con.execute("SELECT concat_with_separator('Hello', ' - ', 'World')")
      rows = result.to_a

      assert_equal 'Hello - World', rows[0][0]
    end
  end
end
