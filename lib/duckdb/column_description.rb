# frozen_string_literal: true

module DuckDB
  if defined?(DuckDB::TableDescription)
    # DuckDB::ColumnDescription is an immutable value object describing a single
    # column returned by DuckDB::TableDescription#column_descriptions.
    #
    # It is defined using +Data.define+ and exposes three attributes:
    #
    # - +name+ — the column name as a String
    # - +logical_type+ — a DuckDB::LogicalType representing the column's type
    # - +has_default+ — +true+ if the column has a DEFAULT value, +false+ otherwise
    #
    # A predicate alias +has_default?+ is provided for idiomatic Ruby usage.
    #
    # Requires DuckDB >= 1.5.0.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query("CREATE TABLE t (id INTEGER, name VARCHAR DEFAULT 'anon')")
    #
    #   td = DuckDB::TableDescription.new(con, 't')
    #   cd = td.column_descriptions.last
    #   cd.name               #=> "name"
    #   cd.logical_type.type  #=> :varchar
    #   cd.has_default?       #=> true
    ColumnDescription = Data.define(:name, :logical_type, :has_default) do
      alias_method :has_default?, :has_default
    end
  end
end
