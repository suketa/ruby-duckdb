# frozen_string_literal: true

module DuckDB
  # DuckDB::TableDescription provides metadata about a table in DuckDB.
  #
  # Use it to retrieve column descriptions — including name, logical type, and
  # whether a column has a default value — for any accessible table.
  #
  #   require 'duckdb'
  #   db = DuckDB::Database.open
  #   con = db.connect
  #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR DEFAULT \'anonymous\')')
  #
  #   td = DuckDB::TableDescription.new(con, 'users')
  #   td.column_descriptions.each do |cd|
  #     puts "#{cd.name}: #{cd.logical_type.type}, default=#{cd.has_default?}"
  #   end
  #   # id: integer, default=false
  #   # name: varchar, default=true
  class TableDescription
    # Creates a new TableDescription for the given table.
    #
    # +con+ must be a DuckDB::Connection. +table+ is the table name (String).
    # Optionally pass +schema:+ and/or +catalog:+ to qualify the table.
    #
    # Raises DuckDB::Error if the connection is invalid, the table name is nil,
    # or the table (or schema/catalog) does not exist.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE t (i INTEGER)')
    #
    #   DuckDB::TableDescription.new(con, 't')
    #   DuckDB::TableDescription.new(con, 't', schema: 'main')
    def initialize(con, table, schema: nil, catalog: nil)
      raise DuckDB::Error, '1st argument must be DuckDB::Connection object.' unless con.is_a?(DuckDB::Connection)
      raise DuckDB::Error, '2nd argument must be table name.' if table.nil?

      raise DuckDB::Error, error_message unless _initialize(con, catalog, schema, table)
    end

    # Returns an array of DuckDB::ColumnDescription objects, one per column.
    #
    # Each element describes a single column's name, logical type, and whether
    # it has a default value defined.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query("CREATE TABLE t (i INTEGER, j INTEGER DEFAULT 5)")
    #
    #   td = DuckDB::TableDescription.new(con, 't')
    #   td.column_descriptions
    #   #=> [#<data DuckDB::ColumnDescription name="i" ...>, ...]
    def column_descriptions
      Array.new(_column_count) do |i|
        ColumnDescription.new(
          name: _column_name(i),
          logical_type: _column_logical_type(i),
          has_default: _column_has_default?(i)
        )
      end
    end

    private

    def _column_has_default?(idx)
      ret = _column_has_default(idx)
      raise DuckDB::Error, error_message || 'failed to determine column has default' if ret.nil?

      ret
    end
  end
end
