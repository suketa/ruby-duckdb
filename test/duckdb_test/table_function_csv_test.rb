# frozen_string_literal: true

require 'test_helper'
require 'csv'
require 'stringio'

module DuckDBTest
  class TableFunctionCSVTest < Minitest::Test
    class CSVTableAdapter
      def initialize(columns: nil)
        @columns = columns
      end

      def call(csv, name, columns: nil)
        columns ||= @columns
        columns ||= columns(csv)

        DuckDB::TableFunction.create(
          name:,
          columns:
        ) do |_func_info, output|
          csv_to_duckdb_data(csv, output)
        end
      end

      private

      # define columns from csv headers, all as VARCHAR for simplicity
      def columns(csv)
        headers = csv.first.headers
        csv.rewind
        headers.to_h { |header| [header, :varchar] }
      end

      # read a line from the csv and write to output, return number of rows written
      def csv_to_duckdb_data(csv, output)
        line = csv.readline
        if line
          line.each_with_index do |cell, index|
            output.set_value(index, 0, cell[1])
          end
          1
        else
          csv.rewind
          0
        end
      end
    end

    def setup
      skip 'TableFunction tests with Ruby callbacks hang on Windows' if Gem.win_platform?

      @db = DuckDB::Database.open
      @con = @db.connect
      @con.execute('SET threads=1') # Required for Ruby callbacks
    end

    def teardown
      @con.close
      @db.close
    end

    def test_csv_table_function

      csv_io = StringIO.new("id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35")
      csv = CSV.new(csv_io, headers: true)

      adapter = CSVTableAdapter.new
      DuckDB::TableFunction.add_table_adapter(CSV, adapter)

      @con.expose_as_table(csv, 'csv_table')
      result = @con.query('SELECT * FROM csv_table()').to_a

      assert_equal %w[1 Alice 30], result[0]
      assert_equal %w[2 Bob 25], result[1]
      assert_equal %w[3 Charlie 35], result[2]
    end

    def test_csv_table_function_returns_date # rubocop:disable Metrics/AbcSize

      csv_io = StringIO.new("value\n2023-01-02\n2024-03-04\n2025-05-06")
      csv = CSV.new(csv_io, headers: true, converters: :date)

      adapter = CSVTableAdapter.new(columns: { 'value' => :date })
      DuckDB::TableFunction.add_table_adapter(CSV, adapter)

      @con.expose_as_table(csv, 'csv_table')
      result = @con.query('SELECT * FROM csv_table()').to_a

      assert_equal [Date.new(2023, 1, 2)], result[0]
      assert_equal [Date.new(2024, 3, 4)], result[1]
      assert_equal [Date.new(2025, 5, 6)], result[2]
    end
  end
end
