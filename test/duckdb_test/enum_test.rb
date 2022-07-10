require 'test_helper'

module DuckDBTest
  if DuckDBVersion.duckdb_version >= '0.3.3'
    class EnumTest < Minitest::Test
      def self.create_table
        @db ||= DuckDB::Database.open # FIXME
        con = @db.connect
        con.query(create_enum_sql)
        con.query(create_table_sql)
        con
      end

      def self.con
        @con ||= create_table
      end

      def self.create_enum_sql
        <<~SQL
          CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
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

      # FIXME
      def NG_test_enum_insert_select
        con = self.class.con
        con.query('INSERT INTO enum_test (id, mood) VALUES (1, $1)', 'sad')
        r = con.query('SELECT * FROM enum_test WHERE id = 1')
        assert_equal([1, 'sad'], r.first)
      end
    end
  end
end
