require 'test_helper'
require 'time'

if defined?(DuckDB::Appender)
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

      def sub_test_append_column(method, type, value = nil, len = nil, expected = nil)
        create_appender("col #{type}")
        @appender.begin_row
        if method == :append
          @appender.send(method, value)
        elsif len
          @appender.send(method, value, len)
        elsif value
          @appender.send(method, value)
        else
          @appender.send(method)
        end
        @appender.end_row
        r = @con.execute("SELECT col FROM #{table}")
        assert_nil(r.first)
        @appender.flush
        expected = value if expected.nil?
        r = @con.execute("SELECT col FROM #{table}")
        expected ? assert_equal(expected, r.first.first) : assert_nil(r.first.first)
      ensure
        teardown
      end

      def sub_test_append_column_(method, type, value)
        create_appender("col #{type}")
        @appender.begin_row
        @appender.send(method, value)
        @appender.end_row
        r = @con.execute("SELECT col FROM #{table}")
        assert_nil(r.first)
        @appender.flush
        r = @con.execute("SELECT col FROM #{table}")
        assert_equal(value, r.first.first)
      ensure
        teardown
      end

      def test_append_bool
        sub_test_append_column_(:append_bool, 'BOOLEAN', true)
      end

      def test_append_int8
        sub_test_append_column_(:append_int8, 'SMALLINT', 127)
        sub_test_append_column_(:append_int8, 'INTEGER', 127)
      end

      def test_append_int8_negative
        sub_test_append_column_(:append_int8, 'SMALLINT', -128)
      end

      def test_append_utint8
        sub_test_append_column_(:append_uint8, 'SMALLINT', 255)
      end

      def test_append_int16
        sub_test_append_column_(:append_int16, 'SMALLINT', 32_767)
      end

      def test_append_int16_negative
        sub_test_append_column_(:append_int16, 'SMALLINT', -32_768)
      end

      def test_append_uint16
        sub_test_append_column_(:append_uint16, 'INTEGER', 65_535)
      end

      def test_append_int32
        sub_test_append_column_(:append_int32, 'INTEGER', 2_147_483_647)
      end

      def test_append_int32_negative
        sub_test_append_column_(:append_int32, 'INTEGER', -2_147_483_648)
      end

      def test_append_uint32
        sub_test_append_column_(:append_uint32, 'BIGINT', 4_294_967_295)
      end

      def test_append_int64
        sub_test_append_column_(:append_int64, 'BIGINT', 9_223_372_036_854_775_807)
      end

      def test_append_int64_negative
        sub_test_append_column_(:append_int64, 'BIGINT', -9_223_372_036_854_775_808)
      end

      def test_append_uint64
        sub_test_append_column_(:append_uint64, 'BIGINT', 9_223_372_036_854_775_807)
      end

      def test_append_hugeint
        sub_test_append_column_(:append_hugeint, 'HUGEINT', 18_446_744_073_709_551_615)
        sub_test_append_column_(:append_hugeint, 'HUGEINT', -170_141_183_460_469_231_731_687_303_715_884_105_727)
        sub_test_append_column_(:append_hugeint, 'HUGEINT', 170_141_183_460_469_231_731_687_303_715_884_105_727)

        e = assert_raises(ArgumentError) { sub_test_append_column_(:append_hugeint, 'HUGEINT', 18.555) }
        assert_equal('2nd argument `18.555` must be Integer.', e.message)
      end

      def test_append_varchar
        sub_test_append_column_(:append_varchar, 'VARCHAR', 'foobarbaz')
      end

      def test_append_varchar_length
        sub_test_append_column(:append_varchar_length, 'VARCHAR', 'foobarbazbaz', 9, 'foobarbaz')
      end

      def test_append_blob
        # duckdb 0.4.0 has append_blob issue.
        # https://github.com/duckdb/duckdb/issues/3960
        data =  DuckDBVersion.duckdb_version == '0.4.0' ? "\1\2\3\4\5" : "\0\1\2\3\4\5"
        value = DuckDB::Blob.new(data)
        expected = data.force_encoding(Encoding::BINARY)

        sub_test_append_column(:append_blob, 'BLOB', value, nil, expected)
      end

      def test_append_null
        sub_test_append_column(:append_null, 'VARCHAR', nil, nil, nil)
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
                                expected: t.strftime('%Y-%m-%d'))

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
        sub_test_append_column2(:append_date, 'DATE', values: [today], expected: today.strftime('%Y-%m-%d'))

        now = Time.now
        sub_test_append_column2(:append_date, 'DATE', values: [now], expected: now.strftime('%Y-%m-%d'))

        foo = Foo.new(now)
        sub_test_append_column2(:append_date, 'DATE', values: [foo], expected: now.strftime('%Y-%m-%d'))

        sub_test_append_column2(:append_date, 'DATE', values: ['2021-10-10'], expected: '2021-10-10')

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
                                expected: '2 months 3 days 00:00:00.000004')

        sub_test_append_column2(:_append_interval,
                                'INTERVAL',
                                values: [14, 3, 4],
                                expected: '1 year 2 months 3 days 00:00:00.000004')

        micros = (12 * 3600 + 34 * 60 + 56) * 1_000_000 + 987_654
        sub_test_append_column2(:_append_interval,
                                'INTERVAL',
                                values: [14, 3, micros],
                                expected: '1 year 2 months 3 days 12:34:56.987654')

        sub_test_append_column2(:_append_interval,
                                'INTERVAL',
                                values: [-14, -3, -micros],
                                expected: '-1 years -2 months -3 days -12:34:56.987654')

        sub_test_append_column2(:_append_interval,
                                'INTERVAL',
                                values: [14, 32, micros],
                                expected: '1 year 2 months 32 days 12:34:56.987654')

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
                                expected: '2 months 3 days 00:00:00.000004')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'P2M3DT0.00000401S',
                                expected: '2 months 3 days 00:00:00.000004')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'P2M3DT0.00004S',
                                expected: '2 months 3 days 00:00:00.00004')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'P1Y2M3DT0.000004S',
                                expected: '1 year 2 months 3 days 00:00:00.000004')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'P1Y2M3DT12H34M56.987654S',
                                expected: '1 year 2 months 3 days 12:34:56.987654')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'P-1Y-2M-3DT-12H-34M-56.987654S',
                                expected: '-1 years -2 months -3 days -12:34:56.987654')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'P14M32DT12H34M56.987654S',
                                expected: '1 year 2 months 32 days 12:34:56.987654')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'PT12H34M56.987654S',
                                expected: '12:34:56.987654')

        sub_test_append_column2(:append_interval,
                                'INTERVAL',
                                values: 'P3D',
                                expected: '3 days')

        assert_raises(ArgumentError) do
          sub_test_append_column2(:append_interval, 'INTERVAL', values: 1, expected: '')
        end
        assert_raises(ArgumentError) do
          sub_test_append_column2(:append_interval, 'INTERVAL', values: [1, 1], expected: '')
        end
      end

      def test__append_time
        sub_test_append_column2(:_append_time, 'TIME', values: [1, 1, 1, 1], expected: '01:01:01.000001')
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

      def test_append_time
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
        assert_equal('Cannot parse argument `101010` to Time.', e.message)

        e = assert_raises(ArgumentError) do
          sub_test_append_column2(:append_time, 'TIME', values: ['abc'], expected: '10:10:10')
        end
        assert_equal('Cannot parse argument `abc` to Time.', e.message)
      end

      def test__append_timestamp
        t = Time.now
        msec = format('%06d', t.nsec / 1000).to_s.sub(/0+$/, '')
        expected = t.strftime("%Y-%m-%d %H:%M:%S.#{msec}")
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
        expected = now.strftime("%Y-%m-%d %H:%M:%S.#{msec}")
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [now], expected: expected)
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [expected], expected: expected)

        obj = Bar.new
        expected = now.strftime('%Y-%m-%d 01:01:01.123456')
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [obj], expected: expected)

        d = Date.today
        expected = d.strftime('%Y-%m-%d 00:00:00')
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [d], expected: expected)
        dstr = expected.split(' ')[0]
        sub_test_append_column2(:append_timestamp, 'TIMESTAMP', values: [dstr], expected: expected)
        foo = Foo.new(now)
        expected = now.strftime('%Y-%m-%d 00:00:00')
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
        sub_test_append_column_(:append, 'BOOLEAN', true)
        sub_test_append_column_(:append, 'SMALLINT', 127)
        sub_test_append_column_(:append, 'SMALLINT', -128)
        sub_test_append_column_(:append, 'SMALLINT', 255)
        sub_test_append_column_(:append, 'SMALLINT', 32_767)
        sub_test_append_column_(:append, 'SMALLINT', -32_768)
        sub_test_append_column_(:append, 'INTEGER', -32_769)
        sub_test_append_column_(:append, 'INTEGER', 65_535)
        sub_test_append_column_(:append, 'INTEGER', 2_147_483_647)

        sub_test_append_column_(:append, 'INTEGER', -2_147_483_648)
        sub_test_append_column_(:append, 'BIGINT', 4_294_967_295)
        sub_test_append_column_(:append, 'BIGINT', 9_223_372_036_854_775_807)
        sub_test_append_column_(:append, 'BIGINT', -9_223_372_036_854_775_808)
        sub_test_append_column_(:append, 'HUGEINT', 18_446_744_073_709_551_615)
        sub_test_append_column_(:append, 'HUGEINT', -18_446_744_073_709_551_616)
        sub_test_append_column_(:append, 'VARCHAR', 'foobarbaz')

        # duckdb 0.4.0 has append_blob issue.
        # https://github.com/duckdb/duckdb/issues/3960
        data =  DuckDBVersion.duckdb_version == '0.4.0' ? "\1\2\3\4\5" : "\0\1\2\3\4\5"
        value = DuckDB::Blob.new(data)
        expected = data.force_encoding(Encoding::BINARY)

        sub_test_append_column(:append, 'BLOB', value, nil, expected)

        sub_test_append_column(:append, 'VARCHAR', nil, nil, nil)

        e = assert_raises(DuckDB::Error) do
          sub_test_append_column(:append, 'INTEGER', [127])
        end
        assert_equal('not supported type [127] (Array)', e.message)

        d = Date.today
        sub_test_append_column(:append, 'DATE', d, nil, d.strftime('%Y-%m-%d'))

        t = Time.now
        msec = format('%06d', t.nsec / 1000).to_s.sub(/0+$/, '')
        expected = t.strftime("%Y-%m-%d %H:%M:%S.#{msec}")
        sub_test_append_column(:append, 'TIMESTAMP', t, nil, expected)
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
end
