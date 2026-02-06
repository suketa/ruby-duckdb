# frozen_string_literal: true

require 'test_helper'
require 'time'

module DuckDBTest
  class AppenderTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open # FIXME
      @con = @db.connect
    end

    def safe_drop_table
      @con.execute("DROP TABLE #{table};")
    rescue DuckDB::Error
      # ignore DuckDB::Error
    end

    def teardown
      safe_drop_table
      @con.close
      @db.close
    rescue DuckDB::Error
      # ignore DuckDB::Error
    end

    def table
      @table ||= 't'
    end

    def create_table(column)
      @con.execute("CREATE TABLE #{table} (#{column});")
    end

    def create_appender(column)
      create_table(column)
      @appender = DuckDB::Appender.new(@con, '', table)
    end

    def test_s_new
      create_table('id INT')
      appender = DuckDB::Appender.new(@con, '', table)

      assert_instance_of(DuckDB::Appender, appender)
      appender = DuckDB::Appender.new(@con, nil, table)

      assert_instance_of(DuckDB::Appender, appender)
    end

    def test_s_new_with_schema
      @con.execute('CREATE SCHEMA a; CREATE TABLE a.b (id INT);')
      appender = DuckDB::Appender.new(@con, 'a', 'b')

      assert_instance_of(DuckDB::Appender, appender)

      assert_raises(DuckDB::Error) { appender = DuckDB::Appender.new(@con, 'b', 'b') }
    end

    def test_s_create_query
      unless DuckDB::Appender.respond_to?(:create_query)
        skip 'DuckDB::Appender.create_query is not supported in this DuckDB version'
      end

      query = 'INSERT OR REPLACE INTO t SELECT i, val FROM my_appended_data'
      types = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]
      appender = DuckDB::Appender.create_query(@con, query, types, 'my_appended_data', %w[i val])

      assert_instance_of(DuckDB::Appender, appender)
    end

    # test for alias of create_query
    def test_s_from_query
      unless DuckDB::Appender.respond_to?(:create_from_query)
        skip 'DuckDB::Appender.create_query is not supported in this DuckDB version'
      end

      query = 'INSERT OR REPLACE INTO t SELECT i, val FROM my_appended_data'
      types = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]
      appender = DuckDB::Appender.from_query(@con, query, types, 'my_appended_data', %w[i val])

      assert_instance_of(DuckDB::Appender, appender)
    end

    def test_s_create_query_append_test
      unless DuckDB::Appender.respond_to?(:create_query)
        skip 'DuckDB::Appender.create_query is not supported in this DuckDB version'
      end

      setup_table_with_initial_data
      appender = create_query_appender

      append_row_to_appender(appender, 1, 'hello world')
      append_row_to_appender(appender, 2, 'bye bye')

      r = @con.query('SELECT * FROM t ORDER BY i')

      assert_equal([[1, 'hello world'], [2, 'bye bye']], r.to_a)
    end

    def setup_table_with_initial_data
      @con.query('CREATE TABLE t (i INT PRIMARY KEY, value VARCHAR)')
      @con.query("INSERT INTO t VALUES (1, 'hello')")
    end

    def create_query_appender
      query = 'INSERT OR REPLACE INTO t SELECT i, val FROM my_appended_data'
      types = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]
      DuckDB::Appender.create_query(@con, query, types, 'my_appended_data', %w[i val])
    end

    def append_row_to_appender(appender, int_val, varchar_val)
      appender.begin_row
      appender.append_int32(int_val)
      appender.append_varchar(varchar_val)
      appender.end_row
      appender.flush
    end

    def sub_test_append_column2(method, type, values:, expected:)
      create_appender("col #{type}")
      @appender.send(method, *values)
      @appender.end_row
      @appender.flush
      r = @con.execute("SELECT col FROM #{table}")

      assert_equal(expected, r.first.first, "in #{caller[0]}")
    ensure
      safe_drop_table
    end

    def sub_assert_equal(expected, actual)
      if expected.nil?
        assert_nil(actual, "in #{caller[0]}")
      else
        assert_equal(expected, actual, "in #{caller[0]}")
      end
    end

    def assert_duckdb_appender(expected, type, &block)
      create_appender("col #{type}")

      block.call(@appender) if block_given?

      @appender.end_row

      r = @con.execute("SELECT col FROM #{table}")

      assert_nil(r.first)
      @appender.flush

      r = @con.execute("SELECT col FROM #{table}")
      sub_assert_equal(expected, r.first.first)
    ensure
      safe_drop_table
    end

    def test_begin_row
      appender = create_appender('col BOOLEAN')

      assert_equal(appender.__id__, appender.begin_row.__id__)
    end

    def test_flush
      appender = create_appender('col BOOLEAN')
      appender
        .append_bool(true)
        .end_row

      assert_equal(appender.__id__, appender.flush.__id__)
    end

    def test_flush_with_exception
      appender = create_appender('col BOOLEAN NOT NULL')
      appender
        .append_null
        .end_row
      exception = assert_raises(DuckDB::Error) { appender.flush }
      assert_match(/NOT NULL constraint failed/, exception.message)
    end

    def test_end_row
      appender = create_appender('col BOOLEAN')
      appender
        .append_bool(true)

      assert_equal(appender.__id__, appender.end_row.__id__)
    end

    def test_end_row_with_exception
      appender = create_appender('col BOOLEAN')
      exception = assert_raises(DuckDB::Error) { appender.end_row }
      assert_match(/Call to EndRow/, exception.message)
    end

    def test_close
      appender = create_appender('col BOOLEAN')
      appender
        .append_bool(true)
        .end_row

      assert_equal(appender.__id__, appender.close.__id__)
    end

    def test_close_with_exception
      appender = create_appender('col BOOLEAN NOT NULL')
      appender
        .append_null
        .end_row
      exception = assert_raises(DuckDB::Error) { appender.close }
      assert_match(/NOT NULL constraint failed/, exception.message)
    end

    def test_append_bool
      assert_duckdb_appender(true, 'BOOLEAN') { |a| a.append_bool(true) }
      assert_duckdb_appender(false, 'BOOLEAN') { |a| a.append_bool(false) }
    end

    def test_append_int8
      assert_duckdb_appender(127, 'SMALLINT') { |a| a.append_int8(127) }
      assert_duckdb_appender(127, 'INTEGER') { |a| a.append_int8(127) }
    end

    def test_append_int8_negative
      assert_duckdb_appender(-128, 'INTEGER') { |a| a.append_int8(-128) }
    end

    def test_append_utint8
      assert_duckdb_appender(255, 'INTEGER') { |a| a.append_uint8(255) }
    end

    def test_append_int16
      assert_duckdb_appender(32_767, 'SMALLINT') { |a| a.append_int16(32_767) }
    end

    def test_append_int16_negative
      assert_duckdb_appender(-32_768, 'SMALLINT') { |a| a.append_int16(-32_768) }
    end

    def test_append_uint16
      assert_duckdb_appender(65_535, 'INTEGER') { |a| a.append_uint16(65_535) }
    end

    def test_append_int32
      assert_duckdb_appender(2_147_483_647, 'INTEGER') { |a| a.append_int32(2_147_483_647) }
    end

    def test_append_int32_negative
      assert_duckdb_appender(-2_147_483_648, 'INTEGER') { |a| a.append_int32(-2_147_483_648) }
    end

    def test_append_uint32
      assert_duckdb_appender(4_294_967_295, 'BIGINT') { |a| a.append_uint32(4_294_967_295) }
    end

    def test_append_int64
      assert_duckdb_appender(9_223_372_036_854_775_807, 'BIGINT') do |appender|
        appender.append_int64(9_223_372_036_854_775_807)
      end
    end

    def test_append_int64_negative
      assert_duckdb_appender(-9_223_372_036_854_775_808, 'BIGINT') do |appender|
        appender.append_int64(-9_223_372_036_854_775_808)
      end
    end

    def test_append_uint64
      assert_duckdb_appender(9_223_372_036_854_775_807, 'BIGINT') do |appender|
        appender.append_uint64(9_223_372_036_854_775_807)
      end
    end

    def test_append_hugeint_positive
      assert_duckdb_appender(18_446_744_073_709_551_615, 'HUGEINT') do |appender|
        appender.append_hugeint(18_446_744_073_709_551_615)
      end
    end

    def test_append_hugeint_negative
      assert_duckdb_appender(-170_141_183_460_469_231_731_687_303_715_884_105_727, 'HUGEINT') do |appender|
        appender.append_hugeint(-170_141_183_460_469_231_731_687_303_715_884_105_727)
      end
    end

    def test_append_hugeint_max
      assert_duckdb_appender(170_141_183_460_469_231_731_687_303_715_884_105_727, 'HUGEINT') do |appender|
        appender.append_hugeint(170_141_183_460_469_231_731_687_303_715_884_105_727)
      end
    end

    def test_append_hugeint_with_invalid_type
      e = assert_raises(ArgumentError) do
        assert_duckdb_appender(18.555, 'HUGEINT') { |a| a.append_hugeint(18.555) }
      end
      assert_equal('The argument `18.555` must be Integer.', e.message)
    end

    def test_append_uhugeint_mid
      assert_duckdb_appender(170_141_183_460_469_231_731_687_303_715_884_105_727, 'UHUGEINT') do |appender|
        appender.append_uhugeint(170_141_183_460_469_231_731_687_303_715_884_105_727)
      end
    end

    def test_append_uhugeint_max
      assert_duckdb_appender(340_282_366_920_938_463_463_374_607_431_768_211_455, 'UHUGEINT') do |appender|
        appender.append_uhugeint(340_282_366_920_938_463_463_374_607_431_768_211_455)
      end
    end

    def test_append_uhugeint_small
      assert_duckdb_appender(1, 'UHUGEINT') do |appender|
        appender.append_uhugeint(1)
      end
    end

    def test_append_uhugeint_with_invalid_type
      e = assert_raises(ArgumentError) do
        assert_duckdb_appender(18.555, 'UHUGEINT') { |a| a.append_hugeint(18.555) }
      end
      assert_equal('The argument `18.555` must be Integer.', e.message)
    end

    def test_append_varchar
      assert_duckdb_appender('foobarbaz', 'VARCHAR') { |a| a.append_varchar('foobarbaz') }
    end

    def test_append_varchar_length
      assert_duckdb_appender('foo', 'VARCHAR') do |appender|
        appender.append_varchar_length('foobarbaz', 3)
      end
    end

    def test_append_blob
      data = "\0\1\2\3\4\5"
      value = DuckDB::Blob.new(data)
      expected = data.encode(Encoding::BINARY)

      assert_duckdb_appender(expected, 'BLOB') { |a| a.append_blob(value) }
    end

    def test_append_null
      assert_duckdb_appender(nil, 'VARCHAR', &:append_null)
    end

    def test_append_default_without_default
      assert_duckdb_appender(nil, 'VARCHAR', &:append_default)
    end

    def test_append_default_with_default
      assert_duckdb_appender('foobar', "VARCHAR DEFAULT 'foobar'", &:append_default)
    end

    def test_append_interval
      value = DuckDB::Interval.new(interval_months: 1)
      assert_duckdb_appender(value, 'INTERVAL') { |a| a.append_interval(value) }
    end

    class Foo
      def initialize(time)
        @time = time
      end

      def to_str
        @time.strftime('%Y-%m-%d')
      end
    end

    def test__append_date_valid
      t = Time.now
      sub_test_append_column2(:_append_date,
                              'DATE',
                              values: [t.year, t.month, t.day],
                              expected: Date.new(t.year, t.month, t.day))
    end

    def test__append_date_invalid_year
      t = Time.now
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: ['a', t.month, t.day],
                                expected: t.strftime('%Y-%m-%d'))
      end
    end

    def test__append_date_invalid_month
      t = Time.now
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: [t.year, 'a', t.day],
                                expected: t.strftime('%Y-%m-%d'))
      end
    end

    def test__append_date_invalid_day
      t = Time.now
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: [t.year, t.month, 'c'],
                                expected: t.strftime('%Y-%m-%d'))
      end
    end

    def test__append_date_insufficient_args
      t = Time.now
      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: [t.year, t.month],
                                expected: t.strftime('%Y-%m-%d'))
      end
    end

    def test_append_date_with_date_object
      today = Date.today
      sub_test_append_column2(:append_date, 'DATE', values: [today], expected: today)
    end

    def test_append_date_with_time_object
      now = Time.now
      sub_test_append_column2(:append_date, 'DATE', values: [now], expected: Date.parse(now.strftime('%Y-%m-%d')))
    end

    def test_append_date_with_custom_object
      now = Time.now
      foo = Foo.new(now)
      sub_test_append_column2(:append_date, 'DATE', values: [foo], expected: Date.parse(now.strftime('%Y-%m-%d')))
    end

    def test_append_date_with_string
      sub_test_append_column2(:append_date, 'DATE', values: ['2021-10-10'], expected: Date.parse('2021-10-10'))
    end

    def test_append_date_with_invalid_integer
      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_date, 'DATE', values: [20_211_010], expected: Date.parse('2021-10-10'))
      end
      assert_match(/Cannot parse `20211010` to Date/, e.message)
    end

    def test_append_date_with_invalid_string
      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_date, 'DATE', values: ['2021-1010'], expected: Date.parse('2021-10-10'))
      end
      assert_match(/Cannot parse `"2021-1010"` to Date/, e.message)
    end

    def test__append_interval_simple
      sub_test_append_column2(
        :_append_interval,
        'INTERVAL',
        values: [2, 3, 4],
        expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 4)
      )
    end

    def test__append_interval_months
      sub_test_append_column2(
        :_append_interval,
        'INTERVAL',
        values: [14, 3, 4],
        expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 4)
      )
    end

    def test__append_interval_with_time
      micros = (((12 * 3600) + (34 * 60) + 56) * 1_000_000) + 987_654
      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [14, 3, micros],
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3,
                                                             interval_micros: 45_296_987_654))
    end

    def test__append_interval_negative
      micros = (((12 * 3600) + (34 * 60) + 56) * 1_000_000) + 987_654
      expected_value = DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -45_296_987_654)
      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [-14, -3, -micros],
                              expected: expected_value)
    end

    def test__append_interval_many_days
      micros = (((12 * 3600) + (34 * 60) + 56) * 1_000_000) + 987_654
      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [14, 32, micros],
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 32,
                                                             interval_micros: 45_296_987_654))
    end

    def test__append_interval_invalid_month
      micros = (((12 * 3600) + (34 * 60) + 56) * 1_000_000) + 987_654
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: ['a', 1, micros], expected: '')
      end
    end

    def test__append_interval_invalid_day
      micros = (((12 * 3600) + (34 * 60) + 56) * 1_000_000) + 987_654
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: [1, 'a', micros], expected: '')
      end
    end

    def test__append_interval_invalid_micros
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: [1, 1, 'a'], expected: '')
      end
    end

    def test__append_interval_insufficient_args
      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: [1, 1], expected: '')
      end
    end

    def test_append_interval_basic
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P2M3DT0.000004S',
                              expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 4))
    end

    def test_append_interval_rounded
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P2M3DT0.00000401S',
                              expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 4))
    end

    def test_append_interval_40_micros
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P2M3DT0.00004S',
                              expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 40))
    end

    def test_append_interval_with_years
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P1Y2M3DT0.000004S',
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 4))
    end

    def test_append_interval_full_format
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P1Y2M3DT12H34M56.987654S',
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3,
                                                             interval_micros: 45_296_987_654))
    end

    def test_append_interval_negative_values
      expected_value = DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -45_296_987_654)
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P-1Y-2M-3DT-12H-34M-56.987654S',
                              expected: expected_value)
    end

    def test_append_interval_months_format
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P14M32DT12H34M56.987654S',
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 32,
                                                             interval_micros: 45_296_987_654))
    end

    def test_append_interval_time_only
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'PT12H34M56.987654S',
                              expected: DuckDB::Interval.new(interval_months: 0, interval_days: 0,
                                                             interval_micros: 45_296_987_654))
    end

    def test_append_interval_days_only
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P3D',
                              expected: DuckDB::Interval.new(interval_months: 0, interval_days: 3, interval_micros: 0))
    end

    def test_append_interval_with_invalid_integer
      assert_raises(ArgumentError) do
        sub_test_append_column2(:append_interval, 'INTERVAL', values: 1, expected: '')
      end
    end

    def test_append_interval_with_invalid_array
      assert_raises(ArgumentError) do
        sub_test_append_column2(:append_interval, 'INTERVAL', values: [1, 1], expected: '')
      end
    end

    def test__append_time_valid
      sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1, 1], expected: Time.parse('01:01:01.000001'))
    end

    def test__append_time_invalid_hour
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: ['a', 1, 1, 1], expected: '')
      end
    end

    def test__append_time_invalid_minute
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 'a', 1, 1], expected: '')
      end
    end

    def test__append_time_invalid_second
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 'a', 1], expected: '')
      end
    end

    def test__append_time_invalid_microsecond
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1, 'a'], expected: '')
      end
    end

    def test__append_time_insufficient_args
      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1], expected: '')
      end
    end

    class Bar
      def to_str
        '01:01:01.123456'
      end
    end

    def test_append_time_with_time_object
      sub_test_append_column2(:append_time,
                              'TIME',
                              values: [Time.parse('01:01:01.123456')],
                              expected: Time.parse('01:01:01.123456'))
    end

    def test_append_time_with_string_micros
      sub_test_append_column2(:append_time, 'TIME', values: ['01:01:01.123456'],
                                                    expected: Time.parse('01:01:01.123456'))
    end

    def test_append_time_with_string_no_micros
      sub_test_append_column2(:append_time, 'TIME', values: ['01:01:01'], expected: Time.parse('01:01:01'))
    end

    def test_append_time_with_custom_object
      obj = Bar.new
      sub_test_append_column2(:append_time, 'TIME', values: [obj], expected: Time.parse('01:01:01.123456'))
    end

    def test_append_time_with_invalid_integer
      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_time, 'TIME', values: [101_010], expected: '10:10:10')
      end
      assert_match(/Cannot parse `101010` to Time/, e.message)
    end

    def test_append_time_with_invalid_string
      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_time, 'TIME', values: ['abc'], expected: '10:10:10')
      end
      assert_match(/Cannot parse `"abc"` to Time/, e.message)
    end

    def test__append_timestamp_valid
      t = Time.now
      expected = build_expected_timestamp(t)
      sub_test_append_column2(:_append_timestamp,
                              'TIMESTAMP',
                              values: [t.year, t.month, t.day, t.hour, t.min, t.sec, t.nsec / 1000],
                              expected: expected)
    end

    def test__append_timestamp_invalid_year
      t = Time.now
      expected = build_expected_timestamp(t)
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: ['a', t.month, t.day, t.hour, t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end
    end

    def test__append_timestamp_invalid_month
      t = Time.now
      expected = build_expected_timestamp(t)
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, 'a', t.day, t.hour, t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end
    end

    def test__append_timestamp_invalid_day
      t = Time.now
      expected = build_expected_timestamp(t)
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, 'a', t.hour, t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end
    end

    def test__append_timestamp_invalid_hour
      t = Time.now
      expected = build_expected_timestamp(t)
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, 'a', t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end
    end

    def test__append_timestamp_invalid_minute
      t = Time.now
      expected = build_expected_timestamp(t)
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, t.hour, 'a', t.sec, t.nsec / 1000],
                                expected: expected)
      end
    end

    def test__append_timestamp_invalid_second
      t = Time.now
      expected = build_expected_timestamp(t)
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, t.hour, t.min, 'a', t.nsec / 1000],
                                expected: expected)
      end
    end

    def test__append_timestamp_invalid_microsecond
      t = Time.now
      expected = build_expected_timestamp(t)
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, t.hour, t.min, t.sec, 'a'],
                                expected: expected)
      end
    end

    def test__append_timestamp_insufficient_args
      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1, 1, 1, 1], expected: '')
      end
    end

    def build_expected_timestamp(time)
      msec = format('%06d', time.nsec / 1000).to_s.sub(/0+$/, '')
      Time.parse(time.strftime("%Y-%m-%d %H:%M:%S.#{msec}"))
    end

    def test_append_timestamp_with_time
      now = Time.now
      msec = format('%06d', now.nsec / 1000).to_s.sub(/0+$/, '')
      expected = Time.parse(now.strftime("%Y-%m-%d %H:%M:%S.#{msec}"))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [now], expected: expected)
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [expected], expected: expected)
    end

    def test_append_timestamp_with_custom_object
      now = Time.now
      obj = Bar.new
      expected = Time.parse(now.strftime('%Y-%m-%d 01:01:01.123456'))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [obj], expected: expected)
    end

    def test_append_timestamp_with_date
      d = Date.today
      expected = Time.parse(d.strftime('%Y-%m-%d 00:00:00'))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [d], expected: expected)
      dstr = d.strftime('%Y-%m-%d')
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [dstr], expected: expected)
    end

    def test_append_timestamp_with_foo_object
      now = Time.now
      foo = Foo.new(now)
      expected = Time.parse(now.strftime('%Y-%m-%d 00:00:00'))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [foo], expected: expected)
    end

    def test_append_timestamp_with_invalid_integer
      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [20_211_116], expected: '2021-11-16')
      end
      assert_match(/Cannot parse `20211116` to Time/, e.message)
    end

    def test_append_timestamp_with_invalid_string
      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: ['abc'], expected: '10:10:10')
      end
      assert_match(/Cannot parse `"abc"` to Time/, e.message)
    end

    def test__append_hugeint
      expected = -170_141_183_460_469_231_731_687_303_715_884_105_727
      sub_test_append_column2(:_append_hugeint,
                              'HUGEINT',
                              values: [1, -9_223_372_036_854_775_808],
                              expected: expected)

      expected = 170_141_183_460_469_231_731_687_303_715_884_105_727
      sub_test_append_column2(:_append_hugeint,
                              'HUGEINT',
                              values: [18_446_744_073_709_551_615, 9_223_372_036_854_775_807],
                              expected: expected)
    end

    def test_append_boolean
      assert_duckdb_appender(true, 'BOOLEAN') do |appender|
        appender.append(true)
      end
    end

    def test_append_smallint_value_one_hundred_twenty_seven
      assert_duckdb_appender(127, 'SMALLINT') do |appender|
        appender.append(127)
      end
    end

    def test_append_smallint_value_two_hundred_fifty_five
      assert_duckdb_appender(255, 'SMALLINT') do |appender|
        appender.append(255)
      end
    end

    def test_append_smallint_value_max
      assert_duckdb_appender(32_767, 'SMALLINT') do |appender|
        appender.append(32_767)
      end
    end

    def test_append_smallint_value_negative
      assert_duckdb_appender(-32_768, 'SMALLINT') do |appender|
        appender.append(-32_768)
      end
    end

    def test_append_integer_value_negative_small
      assert_duckdb_appender(-128, 'INTEGER') do |appender|
        appender.append(-128)
      end
    end

    def test_append_integer_value_large
      assert_duckdb_appender(65_535, 'INTEGER') do |appender|
        appender.append(65_535)
      end
    end

    def test_append_integer_value_negative_large
      assert_duckdb_appender(-32_769, 'INTEGER') do |appender|
        appender.append(-32_769)
      end
    end

    def test_append_integer_max
      assert_duckdb_appender(2_147_483_647, 'INTEGER') do |appender|
        appender.append(2_147_483_647)
      end
    end

    def test_append_integer_min
      assert_duckdb_appender(-2_147_483_648, 'INTEGER') do |appender|
        appender.append(-2_147_483_648)
      end
    end

    def test_append_bigint_values
      assert_duckdb_appender(4_294_967_295, 'BIGINT') do |appender|
        appender.append(4_294_967_295)
      end

      assert_duckdb_appender(9_223_372_036_854_775_807, 'BIGINT') do |appender|
        appender.append(9_223_372_036_854_775_807)
      end

      assert_duckdb_appender(-9_223_372_036_854_775_807, 'BIGINT') do |appender|
        appender.append(-9_223_372_036_854_775_807)
      end
    end

    def test_append_hugeint_values
      assert_duckdb_appender(18_446_744_073_709_551_615, 'HUGEINT') do |appender|
        appender.append(18_446_744_073_709_551_615)
      end

      assert_duckdb_appender(-18_446_744_073_709_551_616, 'HUGEINT') do |appender|
        appender.append(-18_446_744_073_709_551_616)
      end
    end

    def test_append_varchar_string
      assert_duckdb_appender('foobarbaz', 'VARCHAR') do |appender|
        appender.append('foobarbaz')
      end
    end

    def test_append_blob_value
      data = "\0\1\2\3\4\5"
      value = DuckDB::Blob.new(data)
      expected = data.encode(Encoding::BINARY)

      assert_duckdb_appender(expected, 'BLOB') do |appender|
        appender.append(value)
      end
    end

    def test_append_nil
      assert_duckdb_appender(nil, 'VARCHAR') do |appender|
        appender.append(nil)
      end
    end

    def test_append_unsupported_type
      e = assert_raises(DuckDB::Error) do
        assert_duckdb_appender(nil, 'VARCHAR') do |appender|
          appender.append([127])
        end
      end
      assert_equal('not supported type [127] (Array)', e.message)
    end

    def test_append_date_value
      d = Date.today

      assert_duckdb_appender(d, 'DATE') do |appender|
        appender.append(d)
      end
    end

    def test_append_timestamp_value
      t = Time.now
      msec = format('%06d', t.nsec / 1000).to_s.sub(/0+$/, '')
      expected = Time.parse(t.strftime("%Y-%m-%d %H:%M:%S.#{msec}"))
      assert_duckdb_appender(expected, 'TIMESTAMP') do |appender|
        appender.append(t)
      end
    end

    def test_append_interval_value
      value = DuckDB::Interval.new(interval_months: 1)
      assert_duckdb_appender(value, 'INTERVAL') { |a| a.append(value) }
    end

    def test_append_row
      @con.query('CREATE TABLE t (col1 INTEGER, col2 VARCHAR)')
      appender = @con.appender('t')
      appender.append_row(1, 'foo')
      appender.flush
      appender.close
      r = @con.query('SELECT * FROM t')

      assert_equal([1, 'foo'], r.first)
    end
  end
end
