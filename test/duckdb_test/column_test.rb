# frozen_string_literal: true

require 'test_helper'
require 'securerandom'

module DuckDBTest
  class ColumnTest < Minitest::Test
    CREATE_TYPE_ENUM_SQL = <<~SQL
      CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', '𝘾𝝾օɭ 😎');
    SQL

    CREATE_TABLE_SQL = <<~SQL
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
        ủȵȋɕṓ𝓭е_𝒄𝗈ł VARCHAR,
        decimal_col DECIMAL,
        enum_col mood,
        int_list_col INT[],
        varchar_list_col VARCHAR[],
        struct_col STRUCT(word VARCHAR, length INTEGER),
        uuid_col UUID
      );
    SQL

    INSERT_SQL = <<~SQL
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
        'ȕɲᎥᴄⲟ𝑑ẽ 𝑠τᵲïņ𝕘 😃',
        1,
        NULL,
        [1, 2, 3],
        ['a', 'b', 'c'],
        ROW('Ruby', 4),
        '#{SecureRandom.uuid}'
      )
    SQL

    SELECT_SQL = 'SELECT * FROM table1'

    EXPECTED_TYPES = %i[
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
      decimal
      enum
      list
      list
      struct
      uuid
    ].freeze

    EXPECTED_NAMES = %w[
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
      decimal_col
      enum_col
      int_list_col
      varchar_list_col
      struct_col
      uuid_col
    ].freeze

    def setup
      @@con ||= create_data
      @result = @@con.query(SELECT_SQL)
      @columns = @result.columns
    end

    def test_type
      assert_equal(EXPECTED_TYPES, @columns.map(&:type))
    end

    def test_name
      assert_equal(EXPECTED_NAMES, @columns.map(&:name))
    end

    private

    def create_data
      @@db ||= DuckDB::Database.open # FIXME
      con = @@db.connect
      con.query(CREATE_TYPE_ENUM_SQL)
      con.query(CREATE_TABLE_SQL)
      con.query(INSERT_SQL)
      con
    end
  end
end
