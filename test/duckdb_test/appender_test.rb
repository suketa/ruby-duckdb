require 'test_helper'

if defined?(DuckDB::Appender)
  module DuckDBTest

    def self.less_than_equal_version_028?
      db = DuckDB::Database.open
      con = db.connect
      r = con.query('SELECT version()')
      version = r.first.first.sub(/\Av/, '')
      Gem::Version.new(version) <= Gem::Version.new('0.2.8')
    end
    LessThanEqualVersion028 = less_than_equal_version_028?

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

        assert_raises(DuckDB::Error) {
          appender = DuckDB::Appender.new(@con, 'b', 'b')
        }
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
        teardown
      end

      def test_append_bool
        sub_test_append_column(:append_bool, 'BOOLEAN', true)
      end

      def test_append_int8
        sub_test_append_column(:append_int8, 'SMALLINT', 127)
        sub_test_append_column(:append_int8, 'INTEGER', 127)
      end

      def test_append_int8_negative
        sub_test_append_column(:append_int8, 'SMALLINT', -128)
      end

      def test_append_utint8
        sub_test_append_column(:append_uint8, 'SMALLINT', 255)
      end

      def test_append_int16
        sub_test_append_column(:append_int16, 'SMALLINT', 32_767)
      end

      def test_append_int16_negative
        sub_test_append_column(:append_int16, 'SMALLINT', -32_768)
      end

      def test_append_uint16
        sub_test_append_column(:append_uint16, 'INTEGER', 65_535)
      end

      def test_append_int32
        sub_test_append_column(:append_int32, 'INTEGER', 2_147_483_647)
      end

      def test_append_int32_negative
        sub_test_append_column(:append_int32, 'INTEGER', -2_147_483_648)
      end

      def test_append_uint32
        sub_test_append_column(:append_uint32, 'BIGINT', 4_294_967_295)
      end

      def test_append_int64
        sub_test_append_column(:append_int64, 'BIGINT', 9_223_372_036_854_775_807)
      end

      def test_append_int64_negative
        sub_test_append_column(:append_int64, 'BIGINT', -9_223_372_036_854_775_808)
      end

      def test_append_uint64
        sub_test_append_column(:append_uint64, 'BIGINT', 9_223_372_036_854_775_807)
      end

      def test_append_hugeint
        sub_test_append_column(:append_hugeint, 'HUGEINT', 18_446_744_073_709_551_615)
      end

      def test_append_varchar
        sub_test_append_column(:append_varchar, 'VARCHAR', 'foobarbaz')
      end

      def test_append_varchar_length
        sub_test_append_column(:append_varchar_length, 'VARCHAR', 'foobarbazbaz', 9, 'foobarbaz')
      end

      def test_append_blob
        value = DuckDB::Blob.new("\0\1\2\3\4\5")
        expected = "\0\1\2\3\4\5".force_encoding(Encoding::BINARY)
        sub_test_append_column(:append_blob, 'BLOB', value, nil, expected)
      end

      def test_append_null
        sub_test_append_column(:append_null, 'VARCHAR', nil, nil, nil)
      end

      # FIXME issue https://github.com/suketa/ruby-duckdb/issues/176
      if LessThanEqualVersion028
        def test_append_varchar_with_date_column
          t = Time.now
          sub_test_append_column(:append_varchar, 'DATE', t.to_s, nil, t.strftime('%Y-%m-%d'))
        end

        def test_append_varchar_with_time_column
          t = Time.now
          sub_test_append_column(:append_varchar, 'TIME', t.strftime('%H:%M:%S'), nil, t.strftime('%H:%M:%S'))
        end

        def test_append_varchar_with_timestamp_column
          t = Time.now
          sub_test_append_column(:append_varchar, 'TIMESTAMP', t.strftime('%Y-%m-%d %H:%M:%S'), nil, t.strftime('%Y-%m-%d %H:%M:%S'))
        end
      else
        def test__append_date
          t = Time.now
          sub_test_append_column2(:_append_date, 'DATE', values: [t.year, t.month, t.day], expected: t.strftime('%Y-%m-%d'))
          assert_raises(TypeError) {
            sub_test_append_column2(:_append_date, 'DATE', values: ['a', t.month, t.day], expected: t.strftime('%Y-%m-%d'))
          }
          assert_raises(TypeError) {
            sub_test_append_column2(:_append_date, 'DATE', values: [t.year, 'a', t.day], expected: t.strftime('%Y-%m-%d'))
          }
          assert_raises(TypeError) {
            sub_test_append_column2(:_append_date, 'DATE', values: [t.year, t.month, 'c'], expected: t.strftime('%Y-%m-%d'))
          }
          assert_raises(ArgumentError) {
            sub_test_append_column2(:_append_date, 'DATE', values: [t.year, t.month], expected: t.strftime('%Y-%m-%d'))
          }
        end
      end

      def test_append
        sub_test_append_column(:append, 'BOOLEAN', true)
        sub_test_append_column(:append, 'SMALLINT', 127)
        sub_test_append_column(:append, 'SMALLINT', -128)
        sub_test_append_column(:append, 'SMALLINT', 255)
        sub_test_append_column(:append, 'SMALLINT', 32_767)
        sub_test_append_column(:append, 'SMALLINT', -32_768)
        sub_test_append_column(:append, 'INTEGER', -32_769)
        sub_test_append_column(:append, 'INTEGER', 65_535)
        sub_test_append_column(:append, 'INTEGER', 2_147_483_647)

        sub_test_append_column(:append, 'INTEGER', -2_147_483_648)
        sub_test_append_column(:append, 'BIGINT', 4_294_967_295)
        sub_test_append_column(:append, 'BIGINT', 9_223_372_036_854_775_807)
        sub_test_append_column(:append, 'BIGINT', -9_223_372_036_854_775_808)
        sub_test_append_column(:append, 'HUGEINT', 18_446_744_073_709_551_615)
        sub_test_append_column(:append, 'HUGEINT', -18_446_744_073_709_551_616)
        sub_test_append_column(:append, 'VARCHAR', 'foobarbaz')

        value = DuckDB::Blob.new("\0\1\2\3\4\5")
        expected = "\0\1\2\3\4\5".force_encoding(Encoding::BINARY)
        sub_test_append_column(:append, 'BLOB', value, nil, expected)

        sub_test_append_column(:append, 'VARCHAR', nil, nil, nil)

        # FIXME: issue https://github.com/suketa/ruby-duckdb/issues/176
        if LessThanEqualVersion028
          t = Time.now
          sub_test_append_column(:append, 'DATE', t.to_s, nil, t.strftime('%Y-%m-%d'))

          sub_test_append_column(:append, 'TIME', t.strftime('%H:%M:%S'), nil, t.strftime('%H:%M:%S'))

          sub_test_append_column(:append, 'TIMESTAMP', t.strftime('%Y-%m-%d %H:%M:%S'), nil, t.strftime('%Y-%m-%d %H:%M:%S'))
        end
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
