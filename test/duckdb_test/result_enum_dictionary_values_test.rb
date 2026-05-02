# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class ResultEnumDictionaryValuesTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      @con.query(create_enum_sql)
      @con.query(create_table_sql)
      @con.query('INSERT INTO enum_test (id, mood) VALUES (1, $1)', 'sad')
      @result = @con.query('SELECT * FROM enum_test WHERE id = 1')
    end

    def teardown
      @con&.close
      @db&.close
    end

    def create_enum_sql
      <<~SQL
        CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', '𝘾𝝾օɭ 😎')
      SQL
    end

    def create_table_sql
      <<~SQL
        CREATE TABLE enum_test (
          id INTEGER PRIMARY KEY,
          mood mood
        )
      SQL
    end

    def test_result_enum_dictionary_values
      assert_equal(['sad', 'ok', 'happy', '𝘾𝝾օɭ 😎'], @result.enum_dictionary_values(1))
    end

    def test_enum_insert_select
      assert_equal([1, 'sad'], @result.first)
    end

    def test_result_enum_dictionary_values_with_invalid_index
      assert_raises(DuckDB::Error) do
        @result.enum_dictionary_values(0)
      end
    end

    def test_result_enum_dictionary_values_with_out_of_range_index
      assert_raises(ArgumentError) do
        @result.enum_dictionary_values(2)
      end
    end
  end
end
