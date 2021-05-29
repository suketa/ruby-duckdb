module DuckDB
  # The DuckDB::Connection encapsulates connection with DuckDB database.
  #
  #   require 'duckdb'
  #   db = DuckDB::Database.open
  #   con = db.connect
  #   con.query(sql)
  class Connection
    #
    # executes sql with args.
    # The first argument sql must be SQL string.
    # The rest arguments are parameters of SQL string.
    # The parameters must be '?' in SQL statement.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_file')
    #   con = db.connect
    #   users = con.query('SELECT * FROM users')
    #   sql = 'SELECT * FROM users WHERE name = ? AND email = ?'
    #   dave = con.query(sql, 'Dave', 'dave@example.com')
    #
    def query(sql, *args)
      return query_sql(sql) if args.empty?

      stmt = PreparedStatement.new(self, sql)
      args.each_with_index do |arg, i|
        stmt.bind(i + 1, arg)
      end
      stmt.execute
    end

    #
    # connects DuckDB database
    # The first argument is DuckDB::Database object
    #
    def connect(db)
      conn = _connect(db)
      return conn unless block_given?

      begin
        yield conn
      ensure
        conn.disconnect
      end
    end

    #
    # returns PreparedStatement object.
    # The first argument is SQL string.
    #
    def prepared_statement(str)
      PreparedStatement.new(self, str)
    end

    #
    # returns Appender object.
    # The first argument is table name
    #
    def appender(table)
      t1, t2 = table.split('.')
      if t2
        Appender.new(self, t1, t2)
      else
        Appender.new(self, t2, t1)
      end
    end

    alias execute query
    alias open connect
    alias close disconnect
  end
end
