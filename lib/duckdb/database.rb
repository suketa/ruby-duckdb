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
    private_class_method :_open_ext if defined?(DuckDB::Config)

    class << self
      ##
      # Opens database.
      # The first argument is DuckDB database file path to open.
      # If there is no argument, the method opens DuckDB database in memory.
      # The method yields block if block is given.
      #
      #   DuckDB::Database.open('duckdb_database.db') #=> DuckDB::Database
      #
      #   DuckDB::Database.open #=> opens DuckDB::Database in memory.
      #
      #   DuckDB::Database.open do |db|
      #     con = db.connect
      #     con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
      #   end
      #
      def open(dbpath = nil, config = nil)
        db = _db_open(dbpath, config)
        return db unless block_given?

        begin
          yield db
        ensure
          db.close
        end
      end

      private

      def _db_open(dbpath, config)
        if defined?(DuckDB::Config) && config
          _open_ext(dbpath, config)
        elsif config
          _open(dbpath, config)
        else
          _open(dbpath)
        end
      end
    end

    ##
    # connects database.
    #
    # The method yields block and disconnects the database if block given
    #
    #   db = DuckDB::Database.open
    #
    #   con = db.connect # => DuckDB::Connection
    #
    #   db.connect do |con|
    #     con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
    #   end
    #
    def connect
      conn = _connect
      return conn unless block_given?

      begin
        yield conn
      ensure
        conn.disconnect
      end
    end
  end
end
