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
      bit_type = DuckDB::LogicalType::BIT # Unsupported type for testing

      error = assert_raises(DuckDB::Error) do
        sf.return_type = bit_type
      end

      assert_match(/not supported/i, error.message)
    end

    def test_set_function
      sf = DuckDB::ScalarFunction.new
      sf1 = sf.set_function { 1 }

      assert_instance_of DuckDB::ScalarFunction, sf1
      assert_equal sf1.__id__, sf.__id__
    end

    def test_register_scalar_function
      sf = DuckDB::ScalarFunction.new
      sf.name = 'foo'
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { 1 }

      @con.register_scalar_function(sf)

      result = @con.execute('SELECT foo()')

      assert_equal 1, result.first.first
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
      bit_type = DuckDB::LogicalType::BIT # Unsupported type for testing

      error = assert_raises(DuckDB::Error) do
        sf.add_parameter(bit_type)
      end

      assert_match(/not supported/i, error.message)
    end

    def test_add_parameter_raises_error_for_invalid_argument
      sf = DuckDB::ScalarFunction.new

      error = assert_raises(DuckDB::Error) do
        sf.add_parameter('not a logical type')
      end

      assert_match(/Unknown logical type/i, error.message)
    end

    def test_scalar_function_with_one_parameter
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

    def test_scalar_function_with_null_input
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

    def test_scalar_function_bigint_return_type
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

    def test_scalar_function_double_return_type
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

    def test_scalar_function_boolean_return_type
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

    def test_scalar_function_float_return_type
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

    def test_scalar_function_varchar_return_type
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

    def test_scalar_function_hugeint_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('CREATE TABLE test_table (value HUGEINT)')
      @con.execute('INSERT INTO test_table VALUES (85070591730234615865843651857942052863), ' \
                   '(-85070591730234615865843651857942052864), (0)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double_value'
      sf.add_parameter(DuckDB::LogicalType::HUGEINT)
      sf.return_type = DuckDB::LogicalType::HUGEINT
      sf.set_function { |v| v * 2 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT double_value(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      # -85070591730234615865843651857942052864 * 2
      assert_equal(-170_141_183_460_469_231_731_687_303_715_884_105_728, rows[0][0])
      assert_equal 0, rows[1][0] # 0 * 2
      # 85070591730234615865843651857942052863 * 2
      assert_equal 170_141_183_460_469_231_731_687_303_715_884_105_726, rows[2][0]
    end

    def test_scalar_function_uhugeint_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('CREATE TABLE test_table (value UHUGEINT)')
      @con.execute('INSERT INTO test_table VALUES (170141183460469231731687303715884105727), (0), (1000000000)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double_value'
      sf.add_parameter(DuckDB::LogicalType::UHUGEINT)
      sf.return_type = DuckDB::LogicalType::UHUGEINT
      sf.set_function { |v| v * 2 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT double_value(value) FROM test_table ORDER BY value')
      rows = result.to_a

      assert_equal 3, rows.size
      assert_equal 0, rows[0][0] # 0 * 2
      assert_equal 2_000_000_000, rows[1][0] # 1000000000 * 2
      # 170141183460469231731687303715884105727 * 2
      assert_equal 340_282_366_920_938_463_463_374_607_431_768_211_454, rows[2][0]
    end

    def test_scalar_function_gc_safety # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
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

    def test_create_with_no_parameters
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

    def test_scalar_function_with_multithread
      @con.execute('SET threads=4')
      @con.execute('CREATE TABLE large_test AS SELECT range::INTEGER AS value FROM range(10000)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'triple'
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::BIGINT
      sf.set_function { |v| v * 3 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT SUM(triple(value)) FROM large_test')

      # sum(0..9999) * 3 = 49995000 * 3 = 149985000
      assert_equal 149_985_000, result.first.first
    end

    def test_scalar_function_with_symbol_return_type_and_params
      @con.execute('CREATE TABLE large_test AS SELECT range::INTEGER AS value FROM range(10000)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'triple'
      sf.add_parameter(:integer)
      sf.return_type = :bigint
      sf.set_function { |v| v * 3 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT SUM(triple(value)) FROM large_test')

      # sum(0..9999) * 3 = 49995000 * 3 = 149985000
      assert_equal 149_985_000, result.first.first
    end

    # -------------------------------------------------------------------------
    # Tests for varargs_type= — wraps duckdb_scalar_function_set_varargs
    #
    # sf.varargs_type = logical_type  marks a scalar function to accept a
    # variable number of arguments all of the given type.  The block receives
    # the varargs values as a Ruby splat (*args) so callers write |*args|.
    #
    # Implementation checklist (see issue #1122):
    #   - Add `varargs_type=` to lib/duckdb/scalar_function.rb (type validation
    #     + delegation to C via `_set_varargs`)
    #   - Add `_set_varargs` C binding in ext/duckdb/scalar_function.c that
    #     calls duckdb_scalar_function_set_varargs
    #   - Extend the callback dispatch so vararg columns for each row are
    #     collected and forwarded as individual splat arguments to the block
    #   - Extend ScalarFunction.create to accept a `varargs_type:` keyword
    # -------------------------------------------------------------------------

    def test_varargs_type_setter
      # varargs_type= should configure the function to accept any number of
      # arguments of the given type and return self for chaining.

      sf = DuckDB::ScalarFunction.new
      sf.varargs_type = DuckDB::LogicalType::VARCHAR

      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_varargs_type_setter_with_symbol
      # varargs_type= should accept a type symbol (e.g. :varchar) for consistency
      # with add_parameter and return_type=.

      sf = DuckDB::ScalarFunction.new
      sf.varargs_type = :varchar

      assert_instance_of DuckDB::ScalarFunction, sf
    end

    def test_varargs_type_setter_raises_error_for_unsupported_type
      # varargs_type= should raise DuckDB::Error when given a type not in
      # SUPPORTED_TYPES (e.g. BIT), mirroring the behaviour of add_parameter.

      sf = DuckDB::ScalarFunction.new

      error = assert_raises(DuckDB::Error) do
        sf.varargs_type = DuckDB::LogicalType::BIT
      end

      assert_match(/not supported/i, error.message)
    end

    def test_varargs_type_setter_raises_error_for_invalid_argument
      # varargs_type= should raise DuckDB::Error when given something that is not
      # a LogicalType or a recognised type symbol, mirroring add_parameter.

      sf = DuckDB::ScalarFunction.new

      assert_raises(DuckDB::Error) do
        sf.varargs_type = 'not a logical type'
      end
    end

    def test_scalar_function_with_varargs_zero_args
      # When called with zero SQL arguments DuckDB invokes the callback with a
      # data chunk that has 0 columns. The block receives an empty splat so
      # [].sum = 0.
      # Note: the DuckDB C API test (capi_scalar_functions.cpp) returns NULL
      # for zero-arg calls, but that is the callback's own choice.  In Ruby the
      # block decides; here we choose 0 to keep the test deterministic.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_varargs'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |*args| args.sum }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT sum_varargs()')

      assert_equal 0, result.first.first
    end

    def test_scalar_function_with_varargs_single_arg
      # A varargs function called with a single INTEGER literal should receive
      # that one value via the splat and return it unchanged.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_varargs'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |*args| args.sum }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT sum_varargs(42)')

      assert_equal 42, result.first.first
    end

    def test_scalar_function_with_varargs_multiple_args
      # A varargs function called with several INTEGER literals should receive
      # all values via the splat and be able to aggregate them.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_varargs'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |*args| args.sum }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT sum_varargs(1, 2, 3, 4, 5)')

      assert_equal 15, result.first.first
    end

    def test_scalar_function_with_varargs_varchar
      # Varargs function with VARCHAR type: all string arguments are joined.
      # Verifies that the correct type conversion path is used for non-numeric
      # vararg types.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'concat_varargs'
      sf.varargs_type = DuckDB::LogicalType::VARCHAR
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.join }

      @con.register_scalar_function(sf)
      result = @con.execute("SELECT concat_varargs('Hello', ', ', 'World')")

      assert_equal 'Hello, World', result.first.first
    end

    def test_scalar_function_with_varargs_on_table
      # Varargs function applied to multiple columns per row from a table.
      # Each row's column values arrive as splat args; the block sums them.

      @con.execute('CREATE TABLE test_varargs (a INTEGER, b INTEGER, c INTEGER)')
      @con.execute('INSERT INTO test_varargs VALUES (1, 2, 3), (4, 5, 6)')

      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_varargs'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |*args| args.sum }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT sum_varargs(a, b, c) FROM test_varargs ORDER BY a')

      assert_equal [[6], [15]], result.to_a
    end

    def test_scalar_function_with_varargs_null_handling # rubocop:disable Metrics/AbcSize
      # DuckDB applies standard SQL NULL propagation by default: if ANY argument
      # is NULL the function returns NULL without invoking the block.
      # This matches the DuckDB C API test: my_addition(40, 42, NULL) → NULL.
      # To receive NULLs inside the block, set_special_handling would be needed
      # (see issue #1122 — not yet implemented).

      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_varargs'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |*args| args.sum }

      @con.register_scalar_function(sf)

      # Any NULL arg → NULL result (block not invoked)
      assert_nil @con.execute('SELECT sum_varargs(1, NULL, 3)').first.first
      assert_nil @con.execute('SELECT sum_varargs(NULL, 2)').first.first

      # No NULL args → block IS invoked, returns correct sum
      assert_equal 6, @con.execute('SELECT sum_varargs(1, 2, 3)').first.first
    end

    def test_scalar_function_with_varargs_type_mismatch_raises_error
      # Calling a BIGINT varargs function with incompatible types (e.g. VARCHAR
      # or LIST) should fail. DuckDB C API test confirms:
      #   SELECT my_addition('hello', [1])  →  error
      # The error is raised by DuckDB during query execution, not at registration.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_varargs'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |*args| args.sum }

      @con.register_scalar_function(sf)

      assert_raises(DuckDB::Error) do
        @con.execute("SELECT sum_varargs('hello', [1])")
      end
    end

    def test_scalar_function_with_varargs_any_type # rubocop:disable Metrics/AbcSize
      # varargs_type= with DuckDB::LogicalType::ANY allows the function to
      # accept arguments of any type — each arg may differ.  The DuckDB C API
      # test uses DUCKDB_TYPE_ANY for this (capi_scalar_functions.cpp,
      # "variadic number of ANY parameters").
      sf = DuckDB::ScalarFunction.new
      sf.name = 'count_args'
      sf.varargs_type = DuckDB::LogicalType::ANY
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_function { |*args| args.size }

      @con.register_scalar_function(sf)

      assert_equal 0, @con.execute('SELECT count_args()').first.first
      assert_equal 2, @con.execute('SELECT count_args(42, 99)').first.first
      assert_equal 3, @con.execute("SELECT count_args(1, 'hello', true)").first.first
    end

    def test_scalar_function_create_with_varargs_type
      # ScalarFunction.create should accept a varargs_type: keyword as an
      # alternative to parameter_type:/parameter_types:, creating a function
      # that calls varargs_type= internally.

      sf = DuckDB::ScalarFunction.create(
        name: :sum_varargs,
        return_type: :integer,
        varargs_type: :integer
      ) { |*args| args.sum }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT sum_varargs(1, 2, 3)')

      assert_equal 6, result.first.first
    end

    # -------------------------------------------------------------------------
    # Tests for mixing fixed parameters with varargs
    #
    # DuckDB stores fixed params (add_parameter) and the varargs type in
    # separate fields on the same struct, so they coexist — like C's
    # printf(const char *fmt, ...).
    # -------------------------------------------------------------------------

    def test_varargs_type_can_be_combined_with_add_parameter
      # add_parameter and varargs_type= operate on independent fields in DuckDB,
      # so both can be set on the same function. The callback receives the fixed
      # args first, then the varargs as additional splat elements.
      sf = DuckDB::ScalarFunction.new
      sf.name = 'join_with_sep'
      sf.add_parameter(DuckDB::LogicalType::VARCHAR) # fixed: separator
      sf.varargs_type = DuckDB::LogicalType::VARCHAR # trailing: values to join
      sf.return_type  = DuckDB::LogicalType::VARCHAR
      sf.set_function { |sep, *parts| parts.join(sep) }

      @con.register_scalar_function(sf)
      result = @con.execute("SELECT join_with_sep(', ', 'foo', 'bar', 'baz')")

      assert_equal 'foo, bar, baz', result.first.first
    end

    def test_varargs_type_combined_with_multiple_fixed_parameters
      # Multiple fixed parameters followed by varargs.
      # Fixed args arrive positionally; varargs fill the remaining splat.
      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_after_offset'
      sf.add_parameter(DuckDB::LogicalType::INTEGER) # fixed: offset to add
      sf.varargs_type = DuckDB::LogicalType::INTEGER # trailing: values
      sf.return_type  = DuckDB::LogicalType::INTEGER
      sf.set_function { |offset, *nums| nums.sum + offset }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT sum_after_offset(10, 1, 2, 3)')

      assert_equal 16, result.first.first
    end

    def test_scalar_function_create_with_varargs_type_and_parameter_type
      # ScalarFunction.create should allow varargs_type: together with
      # parameter_type: (fixed params + trailing varargs).
      sf = DuckDB::ScalarFunction.create(
        name: :join_with_sep2,
        return_type: :varchar,
        parameter_type: :varchar,   # fixed: separator
        varargs_type: :varchar      # trailing: values
      ) { |sep, *parts| parts.join(sep) }

      @con.register_scalar_function(sf)
      result = @con.execute("SELECT join_with_sep2('-', 'a', 'b', 'c')")

      assert_equal 'a-b-c', result.first.first
    end

    def test_scalar_function_create_null_handling_false_by_default
      # null_handling: defaults to false — DuckDB short-circuits on NULL input
      # and returns NULL without calling the block.
      sf = DuckDB::ScalarFunction.create(
        name: :default_null,
        return_type: :integer,
        parameter_type: :integer
      ) { |v| v.nil? ? 0 : v }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT default_null(NULL)').first.first
      assert_equal 42, @con.execute('SELECT default_null(42)').first.first
    end

    def test_scalar_function_create_with_null_handling_true
      # null_handling: true calls set_special_handling so the block receives
      # nil for NULL inputs and can return a non-NULL value.
      sf = DuckDB::ScalarFunction.create(
        name: :null_as_zero,
        return_type: :integer,
        parameter_type: :integer,
        null_handling: true
      ) { |v| v.nil? ? 0 : v }

      @con.register_scalar_function(sf)

      assert_equal 0,  @con.execute('SELECT null_as_zero(NULL)').first.first
      assert_equal 42, @con.execute('SELECT null_as_zero(42)').first.first
    end

    def test_scalar_function_create_null_handling_with_varargs
      # null_handling: true also works with varargs — NULLs arrive as nil
      # in the splat and the block can count or replace them.
      sf = DuckDB::ScalarFunction.create(
        name: :count_nulls,
        return_type: :integer,
        varargs_type: :integer,
        null_handling: true
      ) { |*args| args.count(&:nil?) }

      @con.register_scalar_function(sf)

      assert_equal 0, @con.execute('SELECT count_nulls(1, 2, 3)').first.first
      assert_equal 1, @con.execute('SELECT count_nulls(1, NULL, 3)').first.first
      assert_equal 3, @con.execute('SELECT count_nulls(NULL, NULL, NULL)').first.first
    end

    # Tests for set_special_handling
    #
    # `duckdb_scalar_function_set_special_handling` marks a scalar function to
    # receive NULL inputs directly, bypassing DuckDB's standard SQL NULL
    # propagation (which normally short-circuits the callback and returns NULL
    # whenever any argument is NULL).
    #
    # With special handling enabled the Ruby block IS called even when one or
    # more arguments are NULL (nil in Ruby), giving the function the chance to
    # decide its own NULL semantics — e.g. treating NULL as 0, or returning a
    # default value.
    #
    # Reference: duckdb_scalar_function_set_special_handling (duckdb.h:3719)
    # Issue: https://github.com/suketa/ruby-duckdb/issues/1122
    # =========================================================================

    def test_set_special_handling_returns_self
      # set_special_handling should return the ScalarFunction itself so it can
      # be chained fluently with other configuration calls.

      sf = DuckDB::ScalarFunction.new
      result = sf.set_special_handling

      assert_instance_of DuckDB::ScalarFunction, result
      assert_equal sf.__id__, result.__id__
    end

    def test_set_special_handling_block_receives_null_arg
      # By default DuckDB skips the callback and returns NULL when any input is
      # NULL.  After set_special_handling the block IS called with nil, so the
      # function can return something other than NULL.
      #
      # Here we implement COALESCE-like behaviour: treat a NULL INTEGER as 0.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'null_as_zero'
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_special_handling
      sf.set_function { |val| val.nil? ? 0 : val }

      @con.register_scalar_function(sf)

      # Without special handling, NULL input → NULL output.
      # With special handling, NULL input → block is called → 0.
      assert_equal 0,  @con.execute('SELECT null_as_zero(NULL)').first.first
      assert_equal 42, @con.execute('SELECT null_as_zero(42)').first.first
    end

    def test_set_special_handling_with_two_parameters_any_null # rubocop:disable Metrics/AbcSize,Metrics/MethodLength,Minitest/MultipleAssertions
      # Both parameters arrive as-is (nil or value) when special handling is
      # set.  The block should handle every combination of nil / non-nil.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_nullable'
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_special_handling
      # Treat NULL as 0 for addition
      sf.set_function { |a, b| (a || 0) + (b || 0) }

      @con.register_scalar_function(sf)

      assert_equal 5, @con.execute('SELECT add_nullable(2, 3)').first.first
      assert_equal 3, @con.execute('SELECT add_nullable(NULL, 3)').first.first
      assert_equal 2, @con.execute('SELECT add_nullable(2, NULL)').first.first
      assert_equal 0, @con.execute('SELECT add_nullable(NULL, NULL)').first.first
    end

    def test_set_special_handling_with_varargs_null_coalesce # rubocop:disable Metrics/AbcSize
      # Varargs + special handling: even if individual args are NULL the block
      # is invoked and may inspect/replace each nil in the splat.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'sum_coalesce'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_special_handling
      sf.set_function { |*args| args.map { |v| v || 0 }.sum }

      @con.register_scalar_function(sf)

      # Without special_handling this would return NULL; with it we get the sum
      # of non-NULL values (NULLs coerced to 0).
      assert_equal 4, @con.execute('SELECT sum_coalesce(1, NULL, 3)').first.first
      assert_equal 0, @con.execute('SELECT sum_coalesce(NULL, NULL)').first.first
    end

    def test_set_special_handling_without_null_returns_normally
      # Enabling special handling must not change behaviour when all inputs are
      # non-NULL — the block should still be called and return correctly.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'double_special'
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_special_handling
      sf.set_function { |val| val * 2 }

      @con.register_scalar_function(sf)

      assert_equal 10, @con.execute('SELECT double_special(5)').first.first
    end

    def test_set_special_handling_can_return_null_explicitly
      # Even with special handling the block may choose to return nil (NULL) —
      # the method must not force a non-NULL result.

      sf = DuckDB::ScalarFunction.new
      sf.name = 'passthrough_null'
      sf.add_parameter(DuckDB::LogicalType::INTEGER)
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_special_handling
      sf.set_function { |val| val } # echo input — nil stays nil

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT passthrough_null(NULL)').first.first
      assert_equal 7, @con.execute('SELECT passthrough_null(7)').first.first
    end

    def test_set_special_handling_null_count_varargs_integer # rubocop:disable Metrics/AbcSize,Metrics/MethodLength,Minitest/MultipleAssertions
      # Mirrors the canonical DuckDB C API test (capi_scalar_functions.cpp,
      # "Test Scalar Functions - variadic number of ANY parameters") but uses
      # INTEGER varargs instead of ANY, because DuckDB::LogicalType::ANY is not
      # yet exposed (issue #1122).
      #
      # The function counts how many of its INTEGER arguments are NULL.
      # With default NULL propagation the block would never be called when any
      # arg is NULL; set_special_handling lets us actually count them.
      #
      # Expected behaviour (directly from the DuckDB C API test):
      #   my_null_count(40, 1, 3)        → 0   (no NULLs)
      #   my_null_count(1, 42, NULL)     → 1   (one NULL)
      #   my_null_count(NULL, NULL, NULL)→ 3   (three NULLs)
      #   my_null_count()                → 0   (no args, no NULLs)

      sf = DuckDB::ScalarFunction.new
      sf.name = 'my_null_count'
      sf.varargs_type = DuckDB::LogicalType::INTEGER
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_special_handling
      sf.set_function { |*args| args.count(&:nil?) }

      @con.register_scalar_function(sf)

      assert_equal 0, @con.execute('SELECT my_null_count(40, 1, 3)').first.first
      assert_equal 1, @con.execute('SELECT my_null_count(1, 42, NULL)').first.first
      assert_equal 3, @con.execute('SELECT my_null_count(NULL, NULL, NULL)').first.first
      assert_equal 0, @con.execute('SELECT my_null_count()').first.first
    end

    def test_set_special_handling_null_count_any_varargs # rubocop:disable Metrics/AbcSize,Metrics/MethodLength,Minitest/MultipleAssertions
      # Same null_count function as above but using DuckDB::LogicalType::ANY,
      # which allows arguments of mixed types (matching the DuckDB C API test
      # exactly: my_null_count(40, [1], 'hello', 3) → 0).
      #
      # Skipped additionally until DuckDB::LogicalType::ANY is added
      # (DUCKDB_TYPE_ANY = 34, see issue #1122).

      sf = DuckDB::ScalarFunction.new
      sf.name = 'my_null_count_any'
      sf.varargs_type = DuckDB::LogicalType::ANY
      sf.return_type = DuckDB::LogicalType::INTEGER
      sf.set_special_handling
      sf.set_function { |*args| args.count(&:nil?) }

      @con.register_scalar_function(sf)

      assert_equal 0, @con.execute("SELECT my_null_count_any(40, 'hello', 3)").first.first
      assert_equal 1, @con.execute('SELECT my_null_count_any(1, 42, NULL)').first.first
      assert_equal 3, @con.execute('SELECT my_null_count_any(NULL, NULL, NULL)').first.first
      assert_equal 0, @con.execute('SELECT my_null_count_any()').first.first
    end

    def test_scalar_function_timestamp_s_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_S)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00'), ('2024-12-25 23:59:59')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_one_hour_ts_s'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_S)
      sf.return_type = DuckDB::LogicalType::TIMESTAMP_S
      sf.set_function { |ts| ts + 3600 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_one_hour_ts_s(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal Time.new(2024, 1, 15, 11, 30, 0), rows[0][0]
      assert_equal Time.new(2024, 12, 26, 0, 59, 59), rows[1][0]
    end

    def test_scalar_function_timestamp_s_parameter_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_S)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00'), ('2024-12-25 23:59:59')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_s_to_string'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_S)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_s_to_string(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00', rows[0][0]
      assert_equal '2024-12-25 23:59:59', rows[1][0]
    end

    def test_scalar_function_timestamp_s_varargs_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMP_S, ts2 TIMESTAMP_S)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00', '2024-06-15 12:00:00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'latest_ts_s'
      sf.varargs_type = DuckDB::LogicalType::TIMESTAMP_S
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.max.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT latest_ts_s(ts1, ts2) FROM test_table')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal '2024-06-15 12:00:00', rows[0][0]
    end

    def test_scalar_function_timestamp_s_default_null_handling
      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_s_null_test'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_S)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT ts_s_null_test(NULL::TIMESTAMP_S)').first.first
      assert_equal '2024-01-15 10:30:00',
                   @con.execute("SELECT ts_s_null_test('2024-01-15 10:30:00'::TIMESTAMP_S)").first.first
    end

    def test_scalar_function_create_with_timestamp_s_symbol # rubocop:disable Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_S)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00'), ('2024-12-25 23:59:59')")

      sf = DuckDB::ScalarFunction.create(
        name: :ts_s_symbol_test,
        return_type: :varchar,
        parameter_type: :timestamp_s
      ) { |ts| ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_s_symbol_test(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00', rows[0][0]
      assert_equal '2024-12-25 23:59:59', rows[1][0]
    end

    def test_scalar_function_timestamp_s_null_handling_true
      sf = DuckDB::ScalarFunction.create(
        name: :ts_s_null_aware,
        return_type: :varchar,
        parameter_type: :timestamp_s,
        null_handling: true
      ) { |ts| ts.nil? ? 'NULL_VALUE' : ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)

      assert_equal 'NULL_VALUE', @con.execute('SELECT ts_s_null_aware(NULL::TIMESTAMP_S)').first.first
      assert_equal '2024-01-15 10:30:00',
                   @con.execute("SELECT ts_s_null_aware('2024-01-15 10:30:00'::TIMESTAMP_S)").first.first
    end

    def test_scalar_function_timestamp_s_multiple_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMP_S, ts2 TIMESTAMP_S)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00', '2024-06-15 12:00:00')")
      @con.execute("INSERT INTO test_table VALUES ('2024-12-25 23:59:59', '2024-03-01 00:00:00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_s_diff'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_S)
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_S)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts1, ts2| ts1 > ts2 ? 'first' : 'second' }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_s_diff(ts1, ts2) FROM test_table ORDER BY ts1')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal 'second', rows[0][0]
      assert_equal 'first', rows[1][0]
    end

    def test_scalar_function_timestamp_ms_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_MS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123'), ('2024-12-25 23:59:59.999')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_one_hour_ts_ms'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_MS)
      sf.return_type = DuckDB::LogicalType::TIMESTAMP_MS
      sf.set_function { |ts| ts + 3600 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_one_hour_ts_ms(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal Time.new(2024, 1, 15, 11, 30, 0.123r), rows[0][0]
      assert_equal Time.new(2024, 12, 26, 0, 59, 59.999r), rows[1][0]
    end

    def test_scalar_function_timestamp_ms_parameter_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_MS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123'), ('2024-12-25 23:59:59.999')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_ms_to_string'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_MS)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S.%L') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_ms_to_string(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00.123', rows[0][0]
      assert_equal '2024-12-25 23:59:59.999', rows[1][0]
    end

    def test_scalar_function_timestamp_ms_varargs_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMP_MS, ts2 TIMESTAMP_MS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123', '2024-06-15 12:00:00.456')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'latest_ts_ms'
      sf.varargs_type = DuckDB::LogicalType::TIMESTAMP_MS
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.max.strftime('%Y-%m-%d %H:%M:%S.%L') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT latest_ts_ms(ts1, ts2) FROM test_table')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal '2024-06-15 12:00:00.456', rows[0][0]
    end

    def test_scalar_function_timestamp_ms_default_null_handling
      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_ms_null_test'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_MS)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S.%L') }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT ts_ms_null_test(NULL::TIMESTAMP_MS)').first.first
      assert_equal '2024-01-15 10:30:00.123',
                   @con.execute("SELECT ts_ms_null_test('2024-01-15 10:30:00.123'::TIMESTAMP_MS)").first.first
    end

    def test_scalar_function_create_with_timestamp_ms_symbol # rubocop:disable Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_MS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123'), ('2024-12-25 23:59:59.999')")

      sf = DuckDB::ScalarFunction.create(
        name: :ts_ms_symbol_test,
        return_type: :varchar,
        parameter_type: :timestamp_ms
      ) { |ts| ts.strftime('%Y-%m-%d %H:%M:%S.%L') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_ms_symbol_test(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00.123', rows[0][0]
      assert_equal '2024-12-25 23:59:59.999', rows[1][0]
    end

    def test_scalar_function_timestamp_ms_null_handling_true
      sf = DuckDB::ScalarFunction.create(
        name: :ts_ms_null_aware,
        return_type: :varchar,
        parameter_type: :timestamp_ms,
        null_handling: true
      ) { |ts| ts.nil? ? 'NULL_VALUE' : ts.strftime('%Y-%m-%d %H:%M:%S.%L') }

      @con.register_scalar_function(sf)

      assert_equal 'NULL_VALUE', @con.execute('SELECT ts_ms_null_aware(NULL::TIMESTAMP_MS)').first.first
      assert_equal '2024-01-15 10:30:00.123',
                   @con.execute("SELECT ts_ms_null_aware('2024-01-15 10:30:00.123'::TIMESTAMP_MS)").first.first
    end

    def test_scalar_function_timestamp_ms_multiple_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMP_MS, ts2 TIMESTAMP_MS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123', '2024-06-15 12:00:00.456')")
      @con.execute("INSERT INTO test_table VALUES ('2024-12-25 23:59:59.999', '2024-03-01 00:00:00.001')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_ms_diff'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_MS)
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_MS)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts1, ts2| ts1 > ts2 ? 'first' : 'second' }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_ms_diff(ts1, ts2) FROM test_table ORDER BY ts1')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal 'second', rows[0][0]
      assert_equal 'first', rows[1][0]
    end

    def test_scalar_function_timestamp_ns_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_NS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123456'), ('2024-12-25 23:59:59.999999')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_one_hour_ts_ns'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_NS)
      sf.return_type = DuckDB::LogicalType::TIMESTAMP_NS
      sf.set_function { |ts| ts + 3600 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_one_hour_ts_ns(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal Time.new(2024, 1, 15, 11, 30, Rational(123_456, 1_000_000)), rows[0][0]
      assert_equal Time.new(2024, 12, 26, 0, 59, Rational(999_999, 1_000_000) + 59), rows[1][0]
    end

    def test_scalar_function_timestamp_ns_parameter_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_NS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123456'), ('2024-12-25 23:59:59.999999')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_ns_to_string'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_NS)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S.%N') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_ns_to_string(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00.123456000', rows[0][0]
      assert_equal '2024-12-25 23:59:59.999999000', rows[1][0]
    end

    def test_scalar_function_timestamp_ns_varargs_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMP_NS, ts2 TIMESTAMP_NS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123456', '2024-06-15 12:00:00.654321')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'latest_ts_ns'
      sf.varargs_type = DuckDB::LogicalType::TIMESTAMP_NS
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.max.strftime('%Y-%m-%d %H:%M:%S.%N') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT latest_ts_ns(ts1, ts2) FROM test_table')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal '2024-06-15 12:00:00.654321000', rows[0][0]
    end

    def test_scalar_function_timestamp_ns_default_null_handling
      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_ns_null_test'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_NS)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S.%N') }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT ts_ns_null_test(NULL::TIMESTAMP_NS)').first.first
      assert_equal '2024-01-15 10:30:00.123456000',
                   @con.execute("SELECT ts_ns_null_test('2024-01-15 10:30:00.123456'::TIMESTAMP_NS)").first.first
    end

    def test_scalar_function_create_with_timestamp_ns_symbol # rubocop:disable Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts TIMESTAMP_NS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123456'), ('2024-12-25 23:59:59.999999')")

      sf = DuckDB::ScalarFunction.create(
        name: :ts_ns_symbol_test,
        return_type: :varchar,
        parameter_type: :timestamp_ns
      ) { |ts| ts.strftime('%Y-%m-%d %H:%M:%S.%N') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_ns_symbol_test(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00.123456000', rows[0][0]
      assert_equal '2024-12-25 23:59:59.999999000', rows[1][0]
    end

    def test_scalar_function_timestamp_ns_null_handling_true
      sf = DuckDB::ScalarFunction.create(
        name: :ts_ns_null_aware,
        return_type: :varchar,
        parameter_type: :timestamp_ns,
        null_handling: true
      ) { |ts| ts.nil? ? 'NULL_VALUE' : ts.strftime('%Y-%m-%d %H:%M:%S.%N') }

      @con.register_scalar_function(sf)

      assert_equal 'NULL_VALUE', @con.execute('SELECT ts_ns_null_aware(NULL::TIMESTAMP_NS)').first.first
      assert_equal '2024-01-15 10:30:00.123456000',
                   @con.execute("SELECT ts_ns_null_aware('2024-01-15 10:30:00.123456'::TIMESTAMP_NS)").first.first
    end

    def test_scalar_function_timestamp_ns_multiple_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMP_NS, ts2 TIMESTAMP_NS)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00.123456', '2024-06-15 12:00:00.654321')")
      @con.execute("INSERT INTO test_table VALUES ('2024-12-25 23:59:59.999999', '2024-03-01 00:00:00.000001')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_ns_diff'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_NS)
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_NS)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts1, ts2| ts1 > ts2 ? 'first' : 'second' }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_ns_diff(ts1, ts2) FROM test_table ORDER BY ts1')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal 'second', rows[0][0]
      assert_equal 'first', rows[1][0]
    end

    def test_scalar_function_timestamp_tz_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('INSTALL icu; LOAD icu;')
      @con.execute("SET TimeZone='UTC'")
      @con.execute('CREATE TABLE test_table (ts TIMESTAMPTZ)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00+00'), ('2024-12-25 23:59:59+00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'add_one_hour_ts_tz'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_TZ)
      sf.return_type = DuckDB::LogicalType::TIMESTAMP_TZ
      sf.set_function { |ts| ts + 3600 }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT add_one_hour_ts_tz(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal Time.new(2024, 1, 15, 11, 30, 0, 'UTC'), rows[0][0]
      assert_equal Time.new(2024, 12, 26, 0, 59, 59, 'UTC'), rows[1][0]
    end

    def test_scalar_function_timestamp_tz_parameter_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('INSTALL icu; LOAD icu;')
      @con.execute("SET TimeZone='UTC'")
      @con.execute('CREATE TABLE test_table (ts TIMESTAMPTZ)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00+00'), ('2024-12-25 23:59:59+00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_tz_to_string'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_TZ)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_tz_to_string(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00', rows[0][0]
      assert_equal '2024-12-25 23:59:59', rows[1][0]
    end

    def test_scalar_function_timestamp_tz_varargs_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('INSTALL icu; LOAD icu;')
      @con.execute("SET TimeZone='UTC'")
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMPTZ, ts2 TIMESTAMPTZ)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00+00', '2024-06-15 12:00:00+00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'latest_ts_tz'
      sf.varargs_type = DuckDB::LogicalType::TIMESTAMP_TZ
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.max.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT latest_ts_tz(ts1, ts2) FROM test_table')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal '2024-06-15 12:00:00', rows[0][0]
    end

    def test_scalar_function_timestamp_tz_default_null_handling # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('INSTALL icu; LOAD icu;')
      @con.execute("SET TimeZone='UTC'")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_tz_null_test'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_TZ)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts| ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT ts_tz_null_test(NULL::TIMESTAMPTZ)').first.first
      assert_equal '2024-01-15 10:30:00',
                   @con.execute("SELECT ts_tz_null_test('2024-01-15 10:30:00+00'::TIMESTAMPTZ)").first.first
    end

    def test_scalar_function_create_with_timestamp_tz_symbol # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('INSTALL icu; LOAD icu;')
      @con.execute("SET TimeZone='UTC'")
      @con.execute('CREATE TABLE test_table (ts TIMESTAMPTZ)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00+00'), ('2024-12-25 23:59:59+00')")

      sf = DuckDB::ScalarFunction.create(
        name: :ts_tz_symbol_test,
        return_type: :varchar,
        parameter_type: :timestamp_tz
      ) { |ts| ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_tz_symbol_test(ts) FROM test_table ORDER BY ts')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '2024-01-15 10:30:00', rows[0][0]
      assert_equal '2024-12-25 23:59:59', rows[1][0]
    end

    def test_scalar_function_timestamp_tz_null_handling_true # rubocop:disable Metrics/MethodLength
      @con.execute('INSTALL icu; LOAD icu;')
      @con.execute("SET TimeZone='UTC'")

      sf = DuckDB::ScalarFunction.create(
        name: :ts_tz_null_aware,
        return_type: :varchar,
        parameter_type: :timestamp_tz,
        null_handling: true
      ) { |ts| ts.nil? ? 'NULL_VALUE' : ts.strftime('%Y-%m-%d %H:%M:%S') }

      @con.register_scalar_function(sf)

      assert_equal 'NULL_VALUE', @con.execute('SELECT ts_tz_null_aware(NULL::TIMESTAMPTZ)').first.first
      assert_equal '2024-01-15 10:30:00',
                   @con.execute("SELECT ts_tz_null_aware('2024-01-15 10:30:00+00'::TIMESTAMPTZ)").first.first
    end

    def test_scalar_function_timestamp_tz_multiple_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('INSTALL icu; LOAD icu;')
      @con.execute("SET TimeZone='UTC'")
      @con.execute('CREATE TABLE test_table (ts1 TIMESTAMPTZ, ts2 TIMESTAMPTZ)')
      @con.execute("INSERT INTO test_table VALUES ('2024-01-15 10:30:00+00', '2024-06-15 12:00:00+00')")
      @con.execute("INSERT INTO test_table VALUES ('2024-12-25 23:59:59+00', '2024-03-01 00:00:00+00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'ts_tz_diff'
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_TZ)
      sf.add_parameter(DuckDB::LogicalType::TIMESTAMP_TZ)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |ts1, ts2| ts1 > ts2 ? 'first' : 'second' }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT ts_tz_diff(ts1, ts2) FROM test_table ORDER BY ts1')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal 'second', rows[0][0]
      assert_equal 'first', rows[1][0]
    end

    def test_scalar_function_time_tz_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('CREATE TABLE test_table (t TIMETZ)')
      @con.execute("INSERT INTO test_table VALUES ('10:30:00+05:30'), ('23:59:59+00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'passthrough_time_tz'
      sf.add_parameter(DuckDB::LogicalType::TIME_TZ)
      sf.return_type = DuckDB::LogicalType::TIME_TZ
      sf.set_function { |t| t }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT passthrough_time_tz(t) FROM test_table ORDER BY t')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal 10, rows[0][0].hour
      assert_equal 30, rows[0][0].min
      assert_equal 0,  rows[0][0].sec
      assert_equal 23, rows[1][0].hour
      assert_equal 59, rows[1][0].min
      assert_equal 59, rows[1][0].sec
    end

    def test_scalar_function_time_tz_parameter_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (t TIMETZ)')
      @con.execute("INSERT INTO test_table VALUES ('10:30:00+05:30'), ('23:59:59+00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'time_tz_to_string'
      sf.add_parameter(DuckDB::LogicalType::TIME_TZ)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |t| t.strftime('%H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT time_tz_to_string(t) FROM test_table ORDER BY t')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '10:30:00', rows[0][0]
      assert_equal '23:59:59', rows[1][0]
    end

    def test_scalar_function_time_tz_varargs_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (t1 TIMETZ, t2 TIMETZ)')
      @con.execute("INSERT INTO test_table VALUES ('08:00:00+00', '17:30:00+00')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'latest_time_tz'
      sf.varargs_type = DuckDB::LogicalType::TIME_TZ
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.max.strftime('%H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT latest_time_tz(t1, t2) FROM test_table')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal '17:30:00', rows[0][0]
    end

    def test_scalar_function_time_tz_default_null_handling
      sf = DuckDB::ScalarFunction.new
      sf.name = 'time_tz_null_test'
      sf.add_parameter(DuckDB::LogicalType::TIME_TZ)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |t| t.strftime('%H:%M:%S') }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT time_tz_null_test(NULL::TIMETZ)').first.first
      assert_equal '10:30:00',
                   @con.execute("SELECT time_tz_null_test('10:30:00+05:30'::TIMETZ)").first.first
    end

    def test_scalar_function_create_with_time_tz_symbol # rubocop:disable Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (t TIMETZ)')
      @con.execute("INSERT INTO test_table VALUES ('10:30:00+05:30'), ('23:59:59+00')")

      sf = DuckDB::ScalarFunction.create(
        name: :time_tz_symbol_test,
        return_type: :varchar,
        parameter_type: :time_tz
      ) { |t| t.strftime('%H:%M:%S') }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT time_tz_symbol_test(t) FROM test_table ORDER BY t')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal '10:30:00', rows[0][0]
      assert_equal '23:59:59', rows[1][0]
    end

    def test_scalar_function_time_tz_null_handling_true
      sf = DuckDB::ScalarFunction.create(
        name: :time_tz_null_aware,
        return_type: :varchar,
        parameter_type: :time_tz,
        null_handling: true
      ) { |t| t.nil? ? 'NULL_VALUE' : t.strftime('%H:%M:%S') }

      @con.register_scalar_function(sf)

      assert_equal 'NULL_VALUE', @con.execute('SELECT time_tz_null_aware(NULL::TIMETZ)').first.first
      assert_equal '10:30:00',
                   @con.execute("SELECT time_tz_null_aware('10:30:00+05:30'::TIMETZ)").first.first
    end

    def test_scalar_function_time_tz_multiple_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (t1 TIMETZ, t2 TIMETZ)')
      @con.execute("INSERT INTO test_table VALUES ('10:30:00+05:30', '17:00:00+00')")
      @con.execute("INSERT INTO test_table VALUES ('23:59:59+00', '08:00:00+09')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'time_tz_diff'
      sf.add_parameter(DuckDB::LogicalType::TIME_TZ)
      sf.add_parameter(DuckDB::LogicalType::TIME_TZ)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |t1, t2| t1.hour > t2.hour ? 'first' : 'second' }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT time_tz_diff(t1, t2) FROM test_table ORDER BY t1')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal 'second', rows[0][0]
      assert_equal 'first', rows[1][0]
    end

    def test_scalar_function_interval_parameter_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (i INTERVAL)')
      @con.execute("INSERT INTO test_table VALUES (INTERVAL '1 year 2 months'), (INTERVAL '3 days 4 hours')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'interval_months_to_string'
      sf.add_parameter(DuckDB::LogicalType::INTERVAL)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |i| i.interval_months.to_s }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT interval_months_to_string(i) FROM test_table ORDER BY i')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_includes %w[0 14], rows[0][0]
      assert_includes %w[0 14], rows[1][0]
    end

    def test_scalar_function_interval_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Minitest/MultipleAssertions
      @con.execute('CREATE TABLE test_table (i INTERVAL)')
      @con.execute("INSERT INTO test_table VALUES (INTERVAL '1 year 2 months'), (INTERVAL '3 days')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'passthrough_interval'
      sf.add_parameter(DuckDB::LogicalType::INTERVAL)
      sf.return_type = DuckDB::LogicalType::INTERVAL
      sf.set_function { |i| i }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT passthrough_interval(i) FROM test_table ORDER BY i')
      rows = result.to_a

      # ORDER BY i sorts by duration: '3 days' < '1 year 2 months'
      assert_equal 2, rows.size
      assert_kind_of DuckDB::Interval, rows[0][0]
      assert_equal 0,  rows[0][0].interval_months
      assert_equal 3,  rows[0][0].interval_days
      assert_equal 0,  rows[0][0].interval_micros
      assert_kind_of DuckDB::Interval, rows[1][0]
      assert_equal 14, rows[1][0].interval_months
      assert_equal 0,  rows[1][0].interval_days
      assert_equal 0,  rows[1][0].interval_micros
    end

    def test_scalar_function_interval_varargs_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (i1 INTERVAL, i2 INTERVAL)')
      @con.execute("INSERT INTO test_table VALUES (INTERVAL '1 month', INTERVAL '2 months')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'interval_varargs_months'
      sf.varargs_type = DuckDB::LogicalType::INTERVAL
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.map(&:interval_months).sum.to_s }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT interval_varargs_months(i1, i2) FROM test_table')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal '3', rows[0][0]
    end

    def test_scalar_function_interval_default_null_handling
      sf = DuckDB::ScalarFunction.new
      sf.name = 'interval_null_test'
      sf.add_parameter(DuckDB::LogicalType::INTERVAL)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |i| i.interval_months.to_s }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT interval_null_test(NULL::INTERVAL)').first.first
      assert_equal '14',
                   @con.execute("SELECT interval_null_test(INTERVAL '1 year 2 months'::INTERVAL)").first.first
    end

    def test_scalar_function_interval_null_handling_true
      sf = DuckDB::ScalarFunction.create(
        name: :interval_null_aware,
        return_type: :varchar,
        parameter_type: :interval,
        null_handling: true
      ) { |i| i.nil? ? 'NULL_VALUE' : i.interval_months.to_s }

      @con.register_scalar_function(sf)

      assert_equal 'NULL_VALUE', @con.execute('SELECT interval_null_aware(NULL::INTERVAL)').first.first
      assert_equal '14',
                   @con.execute("SELECT interval_null_aware(INTERVAL '1 year 2 months'::INTERVAL)").first.first
    end

    def test_scalar_function_create_with_interval_symbol # rubocop:disable Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (i INTERVAL)')
      @con.execute("INSERT INTO test_table VALUES (INTERVAL '1 year 2 months'), (INTERVAL '3 days')")

      sf = DuckDB::ScalarFunction.create(
        name: :interval_symbol_test,
        return_type: :varchar,
        parameter_type: :interval
      ) { |i| i.interval_months.to_s }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT interval_symbol_test(i) FROM test_table ORDER BY i')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_includes %w[0 14], rows[0][0]
      assert_includes %w[0 14], rows[1][0]
    end

    def test_scalar_function_interval_multiple_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (i1 INTERVAL, i2 INTERVAL)')
      @con.execute("INSERT INTO test_table VALUES (INTERVAL '1 month', INTERVAL '2 months')")
      @con.execute("INSERT INTO test_table VALUES (INTERVAL '5 months', INTERVAL '3 months')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'interval_compare'
      sf.add_parameter(DuckDB::LogicalType::INTERVAL)
      sf.add_parameter(DuckDB::LogicalType::INTERVAL)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |i1, i2| i1.interval_months > i2.interval_months ? 'first' : 'second' }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT interval_compare(i1, i2) FROM test_table ORDER BY i1')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_equal 'second', rows[0][0]
      assert_equal 'first', rows[1][0]
    end

    UUID1 = '550e8400-e29b-41d4-a716-446655440000'
    UUID2 = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'

    def test_scalar_function_uuid_parameter_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (u UUID)')
      @con.execute("INSERT INTO test_table VALUES ('#{UUID1}'), ('#{UUID2}')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'uuid_passthrough_as_varchar'
      sf.add_parameter(DuckDB::LogicalType::UUID)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |u| u }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT uuid_passthrough_as_varchar(u) FROM test_table ORDER BY u')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_includes [UUID1, UUID2], rows[0][0]
      assert_includes [UUID1, UUID2], rows[1][0]
    end

    def test_scalar_function_uuid_return_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (u UUID)')
      @con.execute("INSERT INTO test_table VALUES ('#{UUID1}'), ('#{UUID2}')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'uuid_passthrough'
      sf.add_parameter(DuckDB::LogicalType::UUID)
      sf.return_type = DuckDB::LogicalType::UUID
      sf.set_function { |u| u }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT uuid_passthrough(u) FROM test_table ORDER BY u')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_includes [UUID1, UUID2], rows[0][0]
      assert_includes [UUID1, UUID2], rows[1][0]
    end

    def test_scalar_function_uuid_varargs_type # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (u1 UUID, u2 UUID)')
      @con.execute("INSERT INTO test_table VALUES ('#{UUID1}', '#{UUID2}')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'uuid_varargs_count'
      sf.varargs_type = DuckDB::LogicalType::UUID
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |*args| args.size.to_s }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT uuid_varargs_count(u1, u2) FROM test_table')
      rows = result.to_a

      assert_equal 1, rows.size
      assert_equal '2', rows[0][0]
    end

    def test_scalar_function_uuid_default_null_handling
      sf = DuckDB::ScalarFunction.new
      sf.name = 'uuid_null_test'
      sf.add_parameter(DuckDB::LogicalType::UUID)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |u| u }

      @con.register_scalar_function(sf)

      assert_nil @con.execute('SELECT uuid_null_test(NULL::UUID)').first.first
      assert_equal UUID1, @con.execute("SELECT uuid_null_test('#{UUID1}'::UUID)").first.first
    end

    def test_scalar_function_uuid_null_handling_true
      sf = DuckDB::ScalarFunction.create(
        name: :uuid_null_aware,
        return_type: :varchar,
        parameter_type: :uuid,
        null_handling: true
      ) { |u| u.nil? ? 'NULL_VALUE' : u }

      @con.register_scalar_function(sf)

      assert_equal 'NULL_VALUE', @con.execute('SELECT uuid_null_aware(NULL::UUID)').first.first
      assert_equal UUID1, @con.execute("SELECT uuid_null_aware('#{UUID1}'::UUID)").first.first
    end

    def test_scalar_function_create_with_uuid_symbol # rubocop:disable Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (u UUID)')
      @con.execute("INSERT INTO test_table VALUES ('#{UUID1}'), ('#{UUID2}')")

      sf = DuckDB::ScalarFunction.create(
        name: :uuid_symbol_test,
        return_type: :varchar,
        parameter_type: :uuid
      ) { |u| u }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT uuid_symbol_test(u) FROM test_table ORDER BY u')
      rows = result.to_a

      assert_equal 2, rows.size
      assert_includes [UUID1, UUID2], rows[0][0]
      assert_includes [UUID1, UUID2], rows[1][0]
    end

    def test_scalar_function_uuid_multiple_parameters # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      @con.execute('CREATE TABLE test_table (u1 UUID, u2 UUID)')
      @con.execute("INSERT INTO test_table VALUES ('#{UUID1}', '#{UUID2}')")
      @con.execute("INSERT INTO test_table VALUES ('#{UUID2}', '#{UUID1}')")

      sf = DuckDB::ScalarFunction.new
      sf.name = 'uuid_equal'
      sf.add_parameter(DuckDB::LogicalType::UUID)
      sf.add_parameter(DuckDB::LogicalType::UUID)
      sf.return_type = DuckDB::LogicalType::VARCHAR
      sf.set_function { |u1, u2| u1 == u2 ? 'equal' : 'different' }

      @con.register_scalar_function(sf)
      result = @con.execute('SELECT uuid_equal(u1, u2) FROM test_table ORDER BY u1')
      rows = result.to_a

      assert_equal 2, rows.size
      rows.each { |r| assert_equal 'different', r[0] }
    end
  end
end
