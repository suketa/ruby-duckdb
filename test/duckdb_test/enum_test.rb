# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class EnumTest < Minitest::Test
    def self.create_table
      @db ||= DuckDB::Database.open # FIXME
      con = @db.connect
      con.query(create_enum_sql)
      con.query(create_table_sql)
      con.query('INSERT INTO enum_test (id, mood) VALUES (1, $1)', 'sad')
      con
    end

    def self.con
      @con ||= create_table
    end

    def self.create_enum_sql
      <<~SQL
      CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', '𝘾𝝾օɭ 😎')
      SQL
    end

    def self.create_table_sql
      <<~SQL
      CREATE TABLE enum_test (
        id INTEGER PRIMARY KEY,
        mood mood
      )
      SQL
    end

    def setup
      if DuckDB::Result.instance_methods.include?(:chunk_each)
        DuckDB::Result.use_chunk_each = true
      end
      con = self.class.con
      @result = con.query('SELECT * FROM enum_test WHERE id = 1')
    end

    def teardown
      if DuckDB::Result.instance_methods.include?(:chunk_each)
        DuckDB::Result.use_chunk_each = false
      end
    end

    def test_result__enum_dictionary_size
      assert_equal(4, @result.send(:_enum_dictionary_size, 1))
    end

    def test_result__enum_dictionary_value
      assert_equal('sad', @result.send(:_enum_dictionary_value, 1, 0))
      assert_equal('ok', @result.send(:_enum_dictionary_value, 1, 1))
      assert_equal('𝘾𝝾օɭ 😎', @result.send(:_enum_dictionary_value, 1, 3))
    end

    def test_result_enum_dictionary_values
      assert_equal(['sad', 'ok', 'happy', '𝘾𝝾օɭ 😎'], @result.enum_dictionary_values(1))
    end

    def test_enum_insert_select
      if DuckDB::Result.instance_methods.include?(:chunk_each)
        # TODO: fix memory leak during chunk_each (dubkdb_result_get_chunk)
        assert_equal([1, 'sad'], @result.first)
      end
    end
  end
end
