require 'test_helper'

module DuckDBTest
  class ResultTest < Minitest::Test
    def setup
      @@con ||= create_data
      @result = @@con.query('SELECT * from table1')
      @ary = first_record
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
        expected_boolean,
        expected_smallint,
        expected_integer,
        exptected_bigint,
        expected_float,
        expected_double,
        expected_string,
        expected_date,
        expected_timestamp
      ]
      assert_equal([expected_ary, 0], @result.each.with_index.to_a.first)
    end

    def test_result_boolean
      assert_equal(expected_boolean, @ary[0])
    end

    def test_result_smallint
      assert_equal(expected_smallint, @ary[1])
    end

    def test_result_integer
      assert_equal(expected_integer, @ary[2])
    end

    def test_result_bigint
      assert_equal(exptected_bigint, @ary[3])
    end

    def test_result_float
      assert_equal(expected_float, @ary[4])
    end

    def test_result_double
      assert_equal(expected_double, @ary[5])
    end

    def test_result_varchar
      assert_equal(expected_string, @ary[6])
    end

    def test_result_date
      assert_equal(expected_date, @ary[7])
    end

    def test_result_timestamp
      assert_equal(expected_timestamp, @ary[8])
    end

    def test_result_null
      assert_equal(Array.new(9), @result.reverse_each.first)
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
        INSERT INTO table1 VALUES
        (
          #{expected_boolean},
          #{expected_smallint},
          #{expected_integer},
          #{exptected_bigint},
          #{expected_float},
          #{expected_double},
          '#{expected_string}',
          '#{expected_date}',
          '#{expected_timestamp}'
        ),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
      SQL
    end

    def expected_boolean
      true
    end

    def expected_smallint
      32767
    end

    def expected_integer
      2147483647
    end

    def exptected_bigint
      9223372036854775807
    end

    def expected_float
      12345.375
    end

    def expected_double
      123.456789
    end

    def expected_string
      'string'
    end

    def expected_date
      '2019-11-03'
    end

    def expected_timestamp
      '2019-11-03 12:34:56'
    end

    def first_record
      @result.first
    end
  end
end
