# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class LogicalTypeTest < Minitest::Test
    CREATE_TABLE_SQL = <<~SQL
      CREATE TABLE table1
      (
        decimal_col DECIMAL(9, 6)
      );
    SQL

    INSERT_SQL = <<~SQL
      INSERT INTO table1 VALUES
      (
        123.456789
      )
    SQL

    SELECT_SQL = 'SELECT * FROM table1'

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
      con.query(CREATE_TABLE_SQL)
      con.query(INSERT_SQL)
    end
  end
end
