# frozen_string_literal: true

require 'test_helper'
require 'bigdecimal'

module DuckDBTest
  # DataChunk#set_value (TableFunction output) and ScalarFunction return
  # values share one writer, so the same Ruby value written through either
  # path must produce the same DuckDB value.
  class VectorWriterConsistencyTest < Minitest::Test
    UTC_TIME = Time.utc(2024, 3, 15, 10, 30, 45, 123_456)
    JST_TIME = Time.new(2024, 3, 15, 19, 30, 45.123456r, '+09:00') # same instant as UTC_TIME
    NS_TIME = Time.utc(2024, 3, 15, 10, 30, 45, 123_456.789r)

    CASES = {
      boolean: [:boolean, true],
      tinyint: [:tinyint, -12],
      smallint: [:smallint, -1234],
      integer: [:integer, -123_456],
      bigint: [:bigint, -(2**40)],
      utinyint: [:utinyint, 200],
      usmallint: [:usmallint, 60_000],
      uinteger: [:uinteger, 4_000_000_000],
      ubigint: [:ubigint, 2**63],
      hugeint: [:hugeint, -(2**100)],
      uhugeint: [:uhugeint, 2**127],
      float: [:float, 1.5],
      double: [:double, 1.25],
      decimal: [DuckDB::LogicalType.create_decimal(10, 2), BigDecimal('123.45')],
      varchar: [:varchar, 'héllo'],
      blob: [:blob, DuckDB::Blob.new("\xAA\x00\xFF".b)],
      date: [:date, Date.new(2024, 3, 15)],
      time: [:time, UTC_TIME],
      time_tz: [:time_tz, JST_TIME],
      interval: [:interval, DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 12_000_000)],
      uuid: [:uuid, '6ba7b810-9dad-11d1-80b4-00c04fd430c8'],
      timestamp_utc: [:timestamp, UTC_TIME],
      timestamp_jst: [:timestamp, JST_TIME],
      timestamp_s_jst: [:timestamp_s, JST_TIME],
      timestamp_ms_jst: [:timestamp_ms, JST_TIME],
      timestamp_ns: [:timestamp_ns, NS_TIME],
      timestamp_tz_utc: [:timestamp_tz, UTC_TIME],
      timestamp_tz_jst: [:timestamp_tz, JST_TIME]
    }.freeze

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
    end

    def teardown
      @con.close
      @db.close
    end

    CASES.each do |name, (type, value)|
      define_method(:"test_#{name}") do
        from_scalar = via_scalar_function(name, type, value)
        from_table = via_table_function(name, type, value)

        assert_equal from_scalar, from_table
      end
    end

    def test_null
      assert_nil via_scalar_function('null', :integer, nil)
      assert_nil via_table_function('null', :integer, nil)
    end

    # TIMESTAMP stores the Time's wall-clock components as-is, like
    # PreparedStatement#bind_timestamp and Appender#append_timestamp.
    def test_timestamp_keeps_wall_clock
      expected = @con.query("SELECT '2024-03-15 19:30:45.123456'::TIMESTAMP::VARCHAR").first.first

      assert_equal expected, as_varchar(:timestamp, JST_TIME, 'ts_wall_clock')
    end

    # TIMESTAMP_TZ stores the instant, so utc_offset is honored.
    def test_timestamp_tz_keeps_instant
      assert_equal as_epoch_us(:timestamp_tz, UTC_TIME, 'tstz_utc'), as_epoch_us(:timestamp_tz, JST_TIME, 'tstz_jst')
      assert_equal (UTC_TIME.to_i * 1_000_000) + UTC_TIME.usec, as_epoch_us(:timestamp_tz, JST_TIME, 'tstz_jst2')
    end

    def test_timestamp_ns_keeps_nanoseconds
      expected = @con.query("SELECT '2024-03-15 10:30:45.123456789'::TIMESTAMP_NS::VARCHAR").first.first

      assert_equal expected, as_varchar(:timestamp_ns, NS_TIME, 'tsns_nanos')
    end

    def test_data_chunk_set_value_replaces_null
      @con.execute('CREATE TABLE t_null (i INTEGER)')
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::INTEGER])
      chunk.set_value(0, 0, nil)
      chunk.set_value(0, 0, 42)
      chunk.size = 1
      appender = DuckDB::Appender.new(@con, 't_null')
      appender.append_data_chunk(chunk)
      appender.close

      assert_equal [[42]], @con.query('SELECT i FROM t_null').to_a
    end

    def test_timestamp_tz_out_of_range_raises
      sf = DuckDB::ScalarFunction.new
      sf.name = 'far_future_tz'
      sf.return_type = :timestamp_tz
      sf.add_parameter(:integer)
      sf.set_function { |_| Time.utc(300_000, 1, 1) }
      @con.register_scalar_function(sf)

      assert_raises(DuckDB::Error) { @con.query('SELECT far_future_tz(1)') }
    end

    def test_data_chunk_timestamp_tz_out_of_range_raises
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::TIMESTAMP_TZ])

      assert_raises(RangeError) { chunk.set_value(0, 0, Time.utc(300_000, 1, 1)) }
    end

    def test_timestamp_tz_boundaries
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::TIMESTAMP_TZ])
      max = Time.at(9_223_372_036_854, 775_807, :usec, in: 'UTC')
      min = Time.at(-9_223_372_036_855, 224_192, :usec, in: 'UTC')

      chunk.set_value(0, 0, max)
      chunk.set_value(0, 0, min)
      assert_raises(RangeError) { chunk.set_value(0, 0, max + Rational(1, 1_000_000)) }
      assert_raises(RangeError) { chunk.set_value(0, 0, min - Rational(1, 1_000_000)) }
    end

    def test_timestamp_ns_out_of_range_raises
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::TIMESTAMP_NS])

      assert_raises(RangeError) { chunk.set_value(0, 0, Time.utc(2300, 1, 1)) }
    end

    def test_timestamp_ns_upper_boundary
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::TIMESTAMP_NS])
      max = Time.at(9_223_372_036, 854_775_807, :nsec, in: 'UTC')

      chunk.set_value(0, 0, max)
      assert_raises(RangeError) { chunk.set_value(0, 0, max + Rational(1, 1_000_000_000)) }
    end

    def test_timestamp_ns_lower_boundary
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::TIMESTAMP_NS])
      min = Time.at(-9_223_372_037, 145_224_192, :nsec, in: 'UTC')

      chunk.set_value(0, 0, min)
      assert_raises(RangeError) { chunk.set_value(0, 0, min - Rational(1, 1_000_000_000)) }
    end

    def test_data_chunk_set_value_invalid_utf8_stays_null
      # Before 2.0, DuckDB stores invalid UTF-8 as is and the append fails instead.
      if ::DuckDBTest.duckdb_library_version < Gem::Version.new('1.5.6')
        skip 'NULL for invalid UTF-8 requires DuckDB >= 1.5.6'
      end

      @con.execute('CREATE TABLE t_utf8 (s VARCHAR)')
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::VARCHAR])
      chunk.set_value(0, 0, nil)
      chunk.set_value(0, 0, "\xFF".b)
      chunk.set_value(0, 1, 'ok')
      chunk.set_value(0, 1, "\xFF".b)
      chunk.size = 2
      appender = DuckDB::Appender.new(@con, 't_utf8')
      appender.append_data_chunk(chunk)
      appender.close

      assert_equal [[nil], [nil]], @con.query('SELECT s FROM t_utf8').to_a
    end

    def test_data_chunk_set_value_raises_for_out_of_range_row
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::INTEGER])

      assert_raises(IndexError) { chunk.set_value(0, 1_000_000, 1) }
    end

    def test_data_chunk_set_value_raises_for_out_of_range_column
      chunk = DuckDB::DataChunk.new([DuckDB::LogicalType::INTEGER])

      assert_raises(IndexError) { chunk.set_value(1, 0, 1) }
    end

    private

    def via_scalar_function(name, type, value)
      func_name = "sf_#{name}"
      sf = DuckDB::ScalarFunction.new
      sf.name = func_name
      sf.return_type = type
      sf.add_parameter(:integer)
      sf.set_function { |_| value }
      @con.register_scalar_function(sf)
      @con.query("SELECT #{func_name}(1)::VARCHAR").first.first
    end

    def via_table_function(name, type, value)
      as_varchar(type, value, "tf_#{name}")
    end

    def as_varchar(type, value, func_name)
      @con.register_table_function(one_row_table_function(func_name, type, value))
      @con.query("SELECT v::VARCHAR FROM #{func_name}()").first.first
    end

    def as_epoch_us(type, value, func_name)
      @con.register_table_function(one_row_table_function(func_name, type, value))
      @con.query("SELECT epoch_us(v) FROM #{func_name}()").first.first
    end

    def one_row_table_function(func_name, type, value)
      done = false
      tf = DuckDB::TableFunction.new
      tf.name = func_name
      tf.bind { |b| b.add_result_column('v', DuckDB::LogicalType.resolve(type)) }
      tf.init { |_| done = false }
      tf.execute do |_, output|
        if done
          output.size = 0
        else
          output.set_value(0, 0, value)
          output.size = 1
          done = true
        end
      end
      tf
    end
  end
end
