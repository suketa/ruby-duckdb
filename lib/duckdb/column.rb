# frozen_string_literal: true

module DuckDB
  class Column
    #
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
    #
    def type
      types = %i[
        invalid
        boolean
        tinyint
        smallint
        integer
        bigint
        utinyint
        usmallint
        uinteger
        ubigint
        float
        double
        timestamp
        date
        time
        interval
        hugeint
        varchar
        blob
        decimal
        timestamp_s
        timestamp_ms
        timestamp_ns
        enum
        list
        struct
        map
        uuid
        json
      ]
      index = _type
      return :unknown if index >= types.size

      types[index]
    end
  end
end
