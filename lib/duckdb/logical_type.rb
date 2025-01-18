# frozen_string_literal: true

module DuckDB
  class LogicalType
    # returns logical type's type symbol
    # `:unknown` means that the logical type's type is unknown/unsupported by ruby-duckdb.
    # `:invalid` means that the logical type's type is invalid in duckdb.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE climates (id INTEGER, temperature DECIMAIL)')
    #
    #   users = con.query('SELECT * FROM climates')
    #   columns = users.columns
    #   columns.second.logical_type.type #=> :decimal
    def type
      type_id = _type
      DuckDB::Converter::IntToSym.type_to_sym(type_id)
    end
  end
end
