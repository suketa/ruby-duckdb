require 'test_helper'

module DuckDBTest
  class ResultTest < Minitest::Test
    def setup
      @con ||= create_data
      @result ||= @con.query('SELECT * from table1')
      @ary ||= first_record
    end

    def test_result
      assert_instance_of(DuckDB::Result, @result)
    end

    def test_each
      assert_instance_of(Array, @ary)
    end

    def test_each_without_block
      assert_instance_of(Enumerator, @result.each)
      expected_ary = [
        'True',
        '32767',
        '2147483647',
        '9223372036854775807',
        '12345.678711',
        '12345.678900',
        'string',
        '2019-11-03',
        '2019-11-03 12:34:56'
      ]
      assert_equal([[expected_ary, 0]], @result.each.with_index.to_a)
    end

    def test_result_boolean
      # TODO: should be true
      assert_equal('True', @ary[0])
    end

    def test_result_smallint
      # TODO: should be Integer
      assert_equal('32767', @ary[1])
    end

    def test_result_integer
      # TODO: should be Integer
      assert_equal('2147483647', @ary[2])
    end

    def test_result_bigint
      # TODO: should be Integer
      assert_equal('9223372036854775807', @ary[3])
    end

    def test_result_float
      # TODO: should be Float
      assert_equal('12345.678711', @ary[4])
    end

    def test_result_double
      # TODO: should be Float
      assert_equal('12345.678900', @ary[5])
    end

    def test_result_varchar
      assert_equal('string', @ary[6])
    end

    def test_result_date
      assert_equal('2019-11-03', @ary[7])
    end

    def test_result_timestamp
      assert_equal('2019-11-03 12:34:56', @ary[8])
    end

    def test_including_enumerable
      assert_includes(DuckDB::Result.ancestors, Enumerable)
    end

    private

    def create_data
      con = DuckDB::Database.open.connect
      con.query(create_table_sql)
      con.query(insert_sql)
      con
    end

    def create_table_sql
      <<-SQL
        CREATE TABLE table1(
          boolean_col BOOLEAN,
          smallint_col SMALLINT,
          integer_col INTEGER,
          bigint_col BIGINT,
          real_col REAL,
          double_col DOUBLE,
          varchar_col VARCHAR,
          date_col DATE,
          timestamp_col timestamp
        )
      SQL
    end

    def insert_sql
      <<-SQL
        INSERT INTO table1 VALUES(
          TRUE,
          32767,
          2147483647,
          9223372036854775807,
          12345.6789,
          12345.6789,
          'string',
          '2019-11-03',
          '2019-11-03 12:34:56'
        )
      SQL
    end

    def first_record
      @result.first
    end
  end
end
