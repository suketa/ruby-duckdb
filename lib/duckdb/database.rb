module DuckDB
  # The Database class encapsulates a DuckDB database.
  #
  # The usage is as follows:
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.open # database in memory
  #   con = db.connect
  #
  #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
  #
  #   con.query("INSERT into users VALUES(1, 'Alice')")
  #   con.query("INSERT into users VALUES(2, 'Bob')")
  #   con.query("INSERT into users VALUES(3, 'Cathy')")
  #
  #   result = con.query('SELECT * from users')
  #   result.each do |row|
  #     p row
  #   end
  #
  class Database
    private_class_method :_open

    def self.open(*args)
      _open(*args)
    end
  end
end
