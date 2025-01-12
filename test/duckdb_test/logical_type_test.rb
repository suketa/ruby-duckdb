# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class LogicalTypeTest < Minitest::Test
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
        decimal_col DECIMAL(9, 6),
        enum_col mood,
        int_list_col INT[],
        varchar_list_col VARCHAR[],
        struct_col STRUCT(word VARCHAR, length INTEGER),
        uuid_col UUID,
        map_col MAP(INTEGER, VARCHAR)
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
        123.456789,
        'sad',
        [1, 2, 3],
        ['a', 'b', 'c'],
        ROW('Ruby', 4),
        '#{SecureRandom.uuid}',
        MAP{1: 'foo'}
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
      decimal
      enum
      list
      list
      struct
      uuid
      map
    ].freeze

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      create_data(@con)
      result = @con.query(SELECT_SQL)
      @columns = result.columns
    end

    def test_defined_klass
      assert(DuckDB.const_defined?(:LogicalType))
    end

    def test_type
      logical_types = @columns.map(&:logical_type)
      assert_equal(EXPECTED_TYPES, logical_types.map(&:type))
    end

    def test_decimal_width
      decimal_column = @columns.find { |column| column.type == :decimal }
      assert_equal(9, decimal_column.logical_type.width)
    end

    def test_decimal_scale
      decimal_column = @columns.find { |column| column.type == :decimal }
      assert_equal(6, decimal_column.logical_type.scale)
    end

    private

    def create_data(con)
      con.query(CREATE_TYPE_ENUM_SQL)
      con.query(CREATE_TABLE_SQL)
      con.query(INSERT_SQL)
    end
  end
end
