module DuckDB
  # The Result class encapsulates a execute result of DuckDB database.
  #
  # The usage is as follows:
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.open # database in memory
  #   con = db.connect
  #
  #   con.execute('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
  #
  #   con.execute("INSERT into users VALUES(1, 'Alice')")
  #   con.execute("INSERT into users VALUES(2, 'Bob')")
  #   con.execute("INSERT into users VALUES(3, 'Cathy')")
  #
  #   result = con.execute('SELECT * from users')
  #   result.each do |row|
  #     p row
  #   end
  class Result
    include Enumerable

    ToRuby = {
      1 => :_to_boolean,
      3 => :_to_smallint,
      4 => :_to_integer,
      5 => :_to_bigint,
      10 => :_to_float,
      11 => :_to_double,
      16 => :_to_hugeint,
      18 => :_to_blob,
    }

    ToRuby.default = :_to_string

    alias column_size column_count
    alias row_size row_count

    def each
      return to_enum { row_size } unless block_given?

      row_count.times do |row_index|
        yield row(row_index)
      end
    end

    def row(row_index)
      row = []
      column_count.times do |col_index|
        row << (_null?(row_index, col_index) ? nil : to_value(row_index, col_index))
      end
      row
    end

    def to_value(row_index, col_index)
      send(ToRuby[_column_type(col_index)], row_index, col_index)
    end

    private

    def _to_hugeint(row, col)
      _to_string(row, col).to_i
    end
  end
end
