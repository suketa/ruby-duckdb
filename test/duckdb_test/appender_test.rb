require 'test_helper'
require 'time'

module DuckDBTest
  class AppenderTest < Minitest::Test
    def self.con
      @db ||= DuckDB::Database.open # FIXME
      @db.connect
    end

    def setup
      @con = AppenderTest.con
    end

    def teardown
      @con.execute("DROP TABLE #{table};")
    rescue DuckDB::Error
      # ignore
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

    def sub_test_append_column2(method, type, values:, expected:)
      create_appender("col #{type}")
      @appender.begin_row
      @appender.send(method, *values)
      @appender.end_row
      @appender.flush
      r = @con.execute("SELECT col FROM #{table}")
      assert_equal(expected, r.first.first)
    ensure
      teardown
    end

    def sub_assert_equal(expected, actual)
      if expected.nil?
        assert_nil(actual)
      else
        assert_equal(expected, actual)
      end
    end

    def assert_duckdb_appender(expected, type, &block)
      create_appender("col #{type}")

      @appender.begin_row

      block.call(@appender) if block_given?

      @appender.end_row

      r = @con.execute("SELECT col FROM #{table}")
      assert_nil(r.first)
      @appender.flush

      r = @con.execute("SELECT col FROM #{table}")

      sub_assert_equal(expected, r.first.first)
    ensure
      teardown
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

    def test_append_hugeint
      assert_duckdb_appender(18_446_744_073_709_551_615, 'HUGEINT') do |appender|
        appender.append_hugeint(18_446_744_073_709_551_615)
      end

      assert_duckdb_appender(-170_141_183_460_469_231_731_687_303_715_884_105_727, 'HUGEINT') do |appender|
        appender.append_hugeint(-170_141_183_460_469_231_731_687_303_715_884_105_727)
      end

      assert_duckdb_appender(170_141_183_460_469_231_731_687_303_715_884_105_727, 'HUGEINT') do |appender|
        appender.append_hugeint(170_141_183_460_469_231_731_687_303_715_884_105_727)
      end

      e = assert_raises(ArgumentError) do
        assert_duckdb_appender(18.555, 'HUGEINT') { |a| a.append_hugeint(18.555) }
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
      # duckdb 0.4.0 has append_blob issue.
      # https://github.com/duckdb/duckdb/issues/3960
      data =  DuckDBVersion.duckdb_version == '0.4.0' ? "\1\2\3\4\5" : "\0\1\2\3\4\5"
      value = DuckDB::Blob.new(data)
      expected = data.force_encoding(Encoding::BINARY)

      assert_duckdb_appender(expected, 'BLOB') { |a| a.append_blob(value) }
    end

    def test_append_null
      assert_duckdb_appender(nil, 'VARCHAR', &:append_null)
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

    def test__append_date
      t = Time.now
      sub_test_append_column2(:_append_date,
                              'DATE',
                              values: [t.year, t.month, t.day],
                              expected: Date.new(t.year, t.month, t.day))

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: ['a', t.month, t.day],
                                expected: t.strftime('%Y-%m-%d'))
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: [t.year, 'a', t.day],
                                expected: t.strftime('%Y-%m-%d'))
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: [t.year, t.month, 'c'],
                                expected: t.strftime('%Y-%m-%d'))
      end

      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_date,
                                'DATE',
                                values: [t.year, t.month],
                                expected: t.strftime('%Y-%m-%d'))
      end
    end

    def test_append_date
      today = Date.today
      sub_test_append_column2(:append_date, 'DATE', values: [today], expected: today)

      now = Time.now
      sub_test_append_column2(:append_date, 'DATE', values: [now], expected: Date.parse(now.strftime('%Y-%m-%d')))

      foo = Foo.new(now)
      sub_test_append_column2(:append_date, 'DATE', values: [foo], expected: Date.parse(now.strftime('%Y-%m-%d')))

      sub_test_append_column2(:append_date, 'DATE', values: ['2021-10-10'], expected: Date.parse('2021-10-10'))

      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_date, 'DATE', values: [20_211_010], expected: '2021-10-10')
      end
      assert_equal('Cannot parse argument `20211010` to Date.', e.message)

      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_date, 'DATE', values: ['2021-1010'], expected: '2021-10-10')
      end
      assert_equal('Cannot parse argument `2021-1010` to Date.', e.message)
    end

    def test__append_interval
      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [2, 3, 4],
                              expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 4))

      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [14, 3, 4],
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 4))

      micros = (12 * 3600 + 34 * 60 + 56) * 1_000_000 + 987_654
      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [14, 3, micros],
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 45_296_987_654))

      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [-14, -3, -micros],
                              expected: DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -45_296_987_654))

      sub_test_append_column2(:_append_interval,
                              'INTERVAL',
                              values: [14, 32, micros],
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 32, interval_micros: 45_296_987_654))

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: ['a', 1, micros], expected: '')
      end
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: [1, 'a', micros], expected: '')
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: [1, 1, 'a'], expected: '')
      end

      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_interval, 'INTERVAL', values: [1, 1], expected: '')
      end
    end

    def test_append_itnerval
      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P2M3DT0.000004S',
                              expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 4))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P2M3DT0.00000401S',
                              expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 4))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P2M3DT0.00004S',
                              expected: DuckDB::Interval.new(interval_months: 2, interval_days: 3, interval_micros: 40))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P1Y2M3DT0.000004S',
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 4))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P1Y2M3DT12H34M56.987654S',
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 45_296_987_654))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P-1Y-2M-3DT-12H-34M-56.987654S',
                              expected: DuckDB::Interval.new(interval_months: -14, interval_days: -3, interval_micros: -45_296_987_654))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P14M32DT12H34M56.987654S',
                              expected: DuckDB::Interval.new(interval_months: 14, interval_days: 32, interval_micros: 45_296_987_654))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'PT12H34M56.987654S',
                              expected: DuckDB::Interval.new(interval_months: 0, interval_days: 0, interval_micros: 45_296_987_654))

      sub_test_append_column2(:append_interval,
                              'INTERVAL',
                              values: 'P3D',
                              expected: DuckDB::Interval.new(interval_months: 0, interval_days: 3, interval_micros: 0))

      assert_raises(ArgumentError) do
        sub_test_append_column2(:append_interval, 'INTERVAL', values: 1, expected: '')
      end
      assert_raises(ArgumentError) do
        sub_test_append_column2(:append_interval, 'INTERVAL', values: [1, 1], expected: '')
      end
    end

    def test__append_time
      # FIXME: Time column is not supported yet.
      # sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1, 1], expected: '01:01:01.000001')
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: ['a', 1, 1, 1], expected: '')
      end
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 'a', 1, 1], expected: '')
      end
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 'a', 1], expected: '')
      end
      assert_raises(TypeError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1, 'a'], expected: '')
      end
      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1], expected: '')
      end
    end

    class Bar
      def to_str
        '01:01:01.123456'
      end
    end

    # FIXME
    def xtest_append_time
      sub_test_append_column2(:append_time,
                              'TIME',
                              values: [Time.parse('01:01:01.123456')],
                              expected: '01:01:01.123456')
      sub_test_append_column2(:append_time, 'TIME', values: ['01:01:01.123456'], expected: '01:01:01.123456')
      sub_test_append_column2(:append_time, 'TIME', values: ['01:01:01'], expected: '01:01:01')
      obj = Bar.new
      sub_test_append_column2(:append_time, 'TIME', values: [obj], expected: '01:01:01.123456')

      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_time, 'TIME', values: [101_010], expected: '10:10:10')
      end
      assert_match(/Cannot parse `101010` to Time/, e.message)

      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_time, 'TIME', values: ['abc'], expected: '10:10:10')
      end
      assert_match(/Cannot parse `"abc"` to Time/, e.message)
    end

    def test__append_timestamp
      t = Time.now
      msec = format('%06d', t.nsec / 1000).to_s.sub(/0+$/, '')
      expected = Time.parse(t.strftime("%Y-%m-%d %H:%M:%S.#{msec}"))
      sub_test_append_column2(:_append_timestamp,
                              'TIMESTAMP',
                              values: [t.year, t.month, t.day, t.hour, t.min, t.sec, t.nsec / 1000],
                              expected: expected)

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: ['a', t.month, t.day, t.hour, t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, 'a', t.day, t.hour, t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, 'a', t.hour, t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, 'a', t.min, t.sec, t.nsec / 1000],
                                expected: expected)
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, t.hour, 'a', t.sec, t.nsec / 1000],
                                expected: expected)
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, t.hour, t.min, 'a', t.nsec / 1000],
                                expected: expected)
      end

      assert_raises(TypeError) do
        sub_test_append_column2(:_append_timestamp,
                                'TIMESTAMP',
                                values: [t.year, t.month, t.day, t.hour, t.min, t.sec, 'a'],
                                expected: expected)
      end

      assert_raises(ArgumentError) do
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1, 1, 1, 1], expected: '')
      end
    end

    def test_append_timestamp
      now = Time.now
      msec = format('%06d', now.nsec / 1000).to_s.sub(/0+$/, '')
      expected = Time.parse(now.strftime("%Y-%m-%d %H:%M:%S.#{msec}"))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [now], expected: expected)

      value = now.strftime("%Y-%m-%d %H:%M:%S.#{msec}")
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [value], expected: expected)

      obj = Bar.new
      expected = Time.parse(now.strftime('%Y-%m-%d 01:01:01.123456'))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [obj], expected: expected)

      d = Date.today
      expected = Time.parse(d.strftime('%Y-%m-%d 00:00:00'))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [d], expected: expected)

      dstr = d.strftime('%Y-%m-%d')
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [dstr], expected: expected)

      foo = Foo.new(now)
      expected = Time.parse(now.strftime('%Y-%m-%d 00:00:00'))
      sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [foo], expected: expected)

      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [20_211_116], expected: '2021-11-16')
      end
      assert_equal('Cannot parse argument `20211116` to Time or Date.', e.message)

      e = assert_raises(ArgumentError) do
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: ['abc'], expected: '10:10:10')
      end
      assert_equal('Cannot parse argument `abc` to Time or Date.', e.message)
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

    def test_append
      assert_duckdb_appender(true, 'BOOLEAN') do |appender|
        appender.append(true)
      end

      assert_duckdb_appender(127, 'SMALLINT') do |appender|
        appender.append(127)
      end

      assert_duckdb_appender(-128, 'INTEGER') do |appender|
        appender.append(-128)
      end

      assert_duckdb_appender(255, 'SMALLINT') do |appender|
        appender.append(255)
      end

      assert_duckdb_appender(32_767, 'SMALLINT') do |appender|
        appender.append(32_767)
      end

      assert_duckdb_appender(65_535, 'INTEGER') do |appender|
        appender.append(65_535)
      end

      assert_duckdb_appender(-32_768, 'SMALLINT') do |appender|
        appender.append(-32_768)
      end

      assert_duckdb_appender(-32_769, 'INTEGER') do |appender|
        appender.append(-32_769)
      end

      assert_duckdb_appender(2_147_483_647, 'INTEGER') do |appender|
        appender.append(2_147_483_647)
      end

      assert_duckdb_appender(-2_147_483_648, 'INTEGER') do |appender|
        appender.append(-2_147_483_648)
      end

      assert_duckdb_appender(4_294_967_295, 'BIGINT') do |appender|
        appender.append(4_294_967_295)
      end

      assert_duckdb_appender(9_223_372_036_854_775_807, 'BIGINT') do |appender|
        appender.append(9_223_372_036_854_775_807)
      end

      assert_duckdb_appender(-9_223_372_036_854_775_807, 'BIGINT') do |appender|
        appender.append(-9_223_372_036_854_775_807)
      end

      assert_duckdb_appender(18_446_744_073_709_551_615, 'HUGEINT') do |appender|
        appender.append(18_446_744_073_709_551_615)
      end

      assert_duckdb_appender(-18_446_744_073_709_551_616, 'HUGEINT') do |appender|
        appender.append(-18_446_744_073_709_551_616)
      end

      assert_duckdb_appender('foobarbaz', 'VARCHAR') do |appender|
        appender.append('foobarbaz')
      end

      data = "\0\1\2\3\4\5"
      value = DuckDB::Blob.new(data)
      expected = data.force_encoding(Encoding::BINARY)

      assert_duckdb_appender(expected, 'BLOB') do |appender|
        appender.append(value)
      end

      assert_duckdb_appender(nil, 'VARCHAR') do |appender|
        appender.append(nil)
      end

      e = assert_raises(DuckDB::Error) do
        assert_duckdb_appender(nil, 'VARCHAR') do |appender|
          appender.append([127])
        end
      end
      assert_equal('not supported type [127] (Array)', e.message)

      d = Date.today

      assert_duckdb_appender(d, 'DATE') do |appender|
        appender.append(d)
      end

      t = Time.now
      msec = format('%06d', t.nsec / 1000).to_s.sub(/0+$/, '')
      expected = Time.parse(t.strftime("%Y-%m-%d %H:%M:%S.#{msec}"))

      assert_duckdb_appender(expected, 'TIMESTAMP') do |appender|
        appender.append(t)
      end

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
