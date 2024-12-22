# frozen_string_literal: true

module DuckDB
  class Column
    # returns column type symbol
    # `:unknown` means that the column type is unknown/unsupported by ruby-duckdb.
    # `:invalid` means that the column type is invalid in duckdb.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
    #
    #   users = con.query('SELECT * FROM users')
    #   columns = users.columns
    #   columns.first.type #=> :integer
    def type
      type_id = _type
      DuckDB::Converter::IntToSym.type_to_sym(type_id)
    end

    def logical_type
      _logical_type
    end
  end
end
