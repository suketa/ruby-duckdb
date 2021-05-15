require 'test_helper'

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

      def sub_test_append_column(method, type, value = nil, len = nil, expected = nil)
        create_appender("col #{type}")
        @appender.begin_row
        if len
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
      end

      def test_append_bool
        sub_test_append_column(:append_bool, 'BOOLEAN', true)
      end

      def test_append_int8
        sub_test_append_column(:append_int8, 'SMALLINT', 127)
      end

      def test_append_int8_negative
        sub_test_append_column(:append_int8, 'SMALLINT', -128)
      end

      def test_append_utint8
        sub_test_append_column(:append_uint8, 'SMALLINT', 255)
      end

      def test_append_int16
        sub_test_append_column(:append_int16, 'SMALLINT', 32767)
      end

      def test_append_int16_negative
        sub_test_append_column(:append_int16, 'SMALLINT', -32768)
      end

      def test_append_uint16
        sub_test_append_column(:append_uint16, 'INTEGER', 65535)
      end

      def test_append_int32
        sub_test_append_column(:append_int32, 'INTEGER', 2147483647)
      end

      def test_append_int32_negative
        sub_test_append_column(:append_int32, 'INTEGER', -2147483648)
      end

      def test_append_uint32
        sub_test_append_column(:append_uint32, 'BIGINT', 4294967295)
      end

      def test_append_int64
        sub_test_append_column(:append_int64, 'BIGINT', 9223372036854775807)
      end

      def test_append_int64_negative
        sub_test_append_column(:append_int64, 'BIGINT', -9223372036854775808)
      end

      def test_append_uint64
        # sub_test_append_column(:append_uint64, 'HUGEINT', 18446744073709551615) # FIXME
        sub_test_append_column(:append_uint64, 'BIGINT', 9223372036854775807)
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
    end
  end
end
