# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultTest < Minitest::Test
    def setup
      @con ||= create_data
      @result = @con.query('SELECT * from table1')

      # fix for using duckdb_fetch_chunk in Result#chunk_each
      @all_records = @result.to_a

      @ary = first_record
    end

    def test_s_new
      assert_raises(DuckDB::Error) { DuckDB::Result.new.each.to_a }
    end

    def test_result
      assert_instance_of(DuckDB::Result, @result)
    end

    def test_each
      assert_instance_of(Array, @ary)
    end

    def test_each_without_block_is_enumerator
      assert_instance_of(Enumerator, @result.each)
    end

    def test_each_without_block
      # fix for using duckdb_fetch_chunk in Result#chunk_each
      result = @con.query('SELECT * from table1')

      expected_ary = [
        expected_boolean,
        expected_smallint,
        expected_integer,
        expected_bigint,
        expected_hugeint,
        expected_float,
        expected_double,
        expected_string,
        Date.parse(expected_date),
        Time.parse(expected_timestamp),
        expected_blob,
        expected_boolean_false
      ]

      assert_equal([expected_ary, 0], result.each.with_index.to_a.first)
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
      assert_equal(expected_bigint, @ary[3])
    end

    def test_result_hugeint
      assert_equal(expected_hugeint, @ary[4])
    end

    def test_result_float
      assert_equal(expected_float, @ary[5])
    end

    def test_result_double
      assert_equal(expected_double, @ary[6])
    end

    def test_result_varchar
      assert_equal(expected_string, @ary[7])
    end

    def test_result_date
      assert_equal(Date.parse(expected_date), @ary[8])
    end

    def test_result_timestamp
      assert_equal(Time.parse(expected_timestamp), @ary[9])
    end

    def test_result_null
      # fix for using duckdb_fetch_chunk in Result#chunk_each
      result = @con.query('SELECT * from table1')

      assert_equal(Array.new(12), result.reverse_each.first)
    end

    def test_including_enumerable
      assert_includes(DuckDB::Result.ancestors, Enumerable)
    end

    def test_rows_changed
      DuckDB::Database.open do |db|
        db.connect do |con|
          r = con.query('CREATE TABLE t2 (id INT)')

          assert_equal(0, r.rows_changed)
          r = con.query('INSERT INTO t2 VALUES (1), (2), (3)')

          assert_equal(3, r.rows_changed)
          r = con.query('UPDATE t2 SET id = id + 1 WHERE id > 1')

          assert_equal(2, r.rows_changed)
          r = con.query('DELETE FROM t2 WHERE id = 0')

          assert_equal(0, r.rows_changed)
          r = con.query('DELETE FROM t2 WHERE id = 4')

          assert_equal(1, r.rows_changed)
        end
      end
    end

    def test_column_count
      assert_equal(12, @result.column_count)
      assert_equal(12, @result.column_size)
      r = @con.query('SELECT boolean_col, smallint_col from table1')

      assert_equal(2, r.column_count)
      assert_equal(2, r.column_size)
    end

    def test_columns
      assert_instance_of(DuckDB::Column, @result.columns.first)
    end

    def test__column_type
      assert_equal(1, @result.send(:_column_type, 0))
      assert_equal(3, @result.send(:_column_type, 1))
      assert_equal(4, @result.send(:_column_type, 2))
      assert_equal(5, @result.send(:_column_type, 3))
      assert_equal(16, @result.send(:_column_type, 4))
      assert_equal(10, @result.send(:_column_type, 5))
      assert_equal(11, @result.send(:_column_type, 6))
      assert_equal(17, @result.send(:_column_type, 7))
      assert_equal(13, @result.send(:_column_type, 8))
      assert_equal(12, @result.send(:_column_type, 9))
    end

    def test_return_type
      result = @con.query('SELECT * from table1')

      assert_equal(:query_result, result.return_type)

      result = @con.query('CREATE TABLE t2 (id INT)')

      assert_equal(:nothing, result.return_type)

      result = @con.query('INSERT INTO t2 VALUES (1)')

      assert_equal(:changed_rows, result.return_type)
    end

    def test_statement_type
      assert_equal(:select, @result.statement_type)
    end

    # rubocop:disable Minitest/TestMethodName
    def xtest__to_hugeint
      assert_only_without_chunk_each do
        assert_equal(expected_hugeint, @result.send(:_to_hugeint, 0, 4))
      end
    end
    # rubocop:enable Minitest/TestMethodName

    private

    def create_data
      @db ||= DuckDB::Database.open # FIXME
      con = @db.connect
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
          hugeint_col HUGEINT,
          real_col REAL,
          double_col DOUBLE,
          varchar_col VARCHAR,
          date_col DATE,
          timestamp_col timestamp,
          blob_col BLOB,
          boolean_col2 BOOLEAN,
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
          #{expected_bigint},
          #{expected_hugeint},
          #{expected_float},
          #{expected_double},
          '#{expected_string}',
          '#{expected_date}',
          '#{expected_timestamp}',
          '#{expected_blob}',
          '#{expected_boolean_false}'
        ),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
      SQL
    end

    def expected_boolean
      true
    end

    def expected_boolean_false
      false
    end

    def expected_smallint
      32_767
    end

    def expected_integer
      2_147_483_647
    end

    def expected_bigint
      9_223_372_036_854_775_807
    end

    def expected_hugeint
      170_141_183_460_469_231_731_687_303_715_884_105_727
    end

    def expected_float
      12_345.375
    end

    def expected_double
      123.456789
    end

    def expected_string
      '𝘶ñîҫȫ𝘥ẹ 𝖘ţ𝗋ⅰɲ𝓰 😃'
    end

    def expected_date
      '2019-11-03'
    end

    def expected_timestamp
      '2019-11-03 12:34:56'
    end

    def expected_blob
      'blob'.encode('ASCII-8BIT')
    end

    def first_record
      # fix for using duckdb_fetch_chunk in Result#chunk_each
      # @result.first
      @all_records.first
    end
  end
end
