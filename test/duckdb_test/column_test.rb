require 'test_helper'

module DuckDBTest
  class ColumnTest < Minitest::Test
    def setup
      @@con ||= create_data
      @result = @@con.query('SELECT * from table1')
      @columns = @result.columns
    end

    def test_type
      expected = %i[
        boolean
        tinyint
        smallint
        integer
        bigint
        utinyint
        usmallint
        uinteger
        ubigint
        float
        double
        date
        time
        timestamp
        interval
        hugeint
        varchar
        varchar
      ]
      if DuckDBVersion.duckdb_version >= '0.3.3'
        expected.push(:decimal)
        expected.push(:enum)
        expected.push(:list)
        expected.push(:list)
      end
      assert_equal(
        expected,
        @columns.map(&:type)
      )
    end

    def test_name
      expected = %w[
        boolean_col
        tinyint_col
        smallint_col
        integer_col
        bigint_col
        utinyint_col
        usmallint_col
        uinteger_col
        ubigint_col
        real_col
        double_col
        date_col
        time_col
        timestamp_col
        interval_col
        hugeint_col
        varchar_col
        ủȵȋɕṓ𝓭е_𝒄𝗈ł
      ]
      if DuckDBVersion.duckdb_version >= '0.3.3'
        expected.push('decimal_col')
        expected.push('enum_col')
        expected.push('int_list_col')
        expected.push('varchar_list_col')
      end
      assert_equal(
        expected,
        @columns.map(&:name)
      )
    end

    private

    def create_data
      @@db ||= DuckDB::Database.open # FIXME
      con = @@db.connect
      con.query(create_type_enum_sql) if DuckDBVersion.duckdb_version >= '0.3.3'
      con.query(create_table_sql)
      con.query(insert_sql)
      con
    end

    def create_type_enum_sql
      "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', '𝘾𝝾օɭ 😎');"
    end

    def create_table_sql
      sql = <<-SQL
        CREATE TABLE table1(
          boolean_col BOOLEAN,
          tinyint_col TINYINT,
          smallint_col SMALLINT,
          integer_col INTEGER,
          bigint_col BIGINT,
          utinyint_col UTINYINT,
          usmallint_col USMALLINT,
          uinteger_col UINTEGER,
          ubigint_col UBIGINT,
          real_col REAL,
          double_col DOUBLE,
          date_col DATE,
          time_col TIME,
          timestamp_col timestamp,
          interval_col INTERVAL,
          hugeint_col HUGEINT,
          varchar_col VARCHAR,
          ủȵȋɕṓ𝓭е_𝒄𝗈ł VARCHAR
      SQL

      if DuckDBVersion.duckdb_version >= '0.3.3'
        sql += ', decimal_col DECIMAL'
        sql += ', enum_col mood'
        sql += ', int_list_col INT[]'
        sql += ', varchar_list_col VARCHAR[]'
      end
      sql += ')'
      sql
    end

    def insert_sql
      sql = <<-SQL
        INSERT INTO table1 VALUES
        (
          true,
          1,
          32767,
          2147483647,
          9223372036854775807,
          1,
          32767,
          2147483647,
          9223372036854775807,
          12345.375,
          123.456789,
          '2019-11-03',
          '12:34:56',
          '2019-11-03 12:34:56',
          '1 day',
          170141183460469231731687303715884105727,
          'string',
          'ȕɲᎥᴄⲟ𝑑ẽ 𝑠τᵲïņ𝕘 😃'
      SQL

      if DuckDBVersion.duckdb_version >= '0.3.3'
        sql += ', 1'
        sql += ', NULL'
        sql += ', [1, 2, 3]'
        sql += ", ['a', 'b', 'c']"
      end
      sql += ')'
      sql
    end
  end
end
