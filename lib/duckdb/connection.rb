# frozen_string_literal: true

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
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_file')
    #   con = db.connect
    #   users = con.query('SELECT * FROM users')
    #   sql = 'SELECT * FROM users WHERE name = ? AND email = ?'
    #   dave = con.query(sql, 'Dave', 'dave@example.com')
    #
    #   # or You can use named parameter.
    #
    #   sql = 'SELECT * FROM users WHERE name = $name AND email = $email'
    #   dave = con.query(sql, name: 'Dave', email: 'dave@example.com')
    #
    def query(sql, *args, **kwargs)
      return query_sql(sql) if args.empty? && kwargs.empty?

      stmt = PreparedStatement.new(self, sql)
      stmt.bind_args(*args, **kwargs)
      stmt.execute
    end

    #
    # executes sql with args asynchronously.
    # The first argument sql must be SQL string.
    # The rest arguments are parameters of SQL string.
    # This method returns DuckDB::PendingResult object.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_file')
    #   con = db.connect
    #   pending_result = con.async_query('SELECT * FROM users')
    #   sql = 'SELECT * FROM users WHERE name = ? AND email = ?'
    #   pending_result = con.async_query(sql, 'Dave', 'dave@example.com')
    #
    #   # or You can use named parameter.
    #
    #   sql = 'SELECT * FROM users WHERE name = $name AND email = $email'
    #   pending_result = con.async_query(sql, name: 'Dave', email: 'dave@example.com')
    #
    def async_query(sql, *args, **kwargs)
      stmt = PreparedStatement.new(self, sql)
      stmt.bind_args(*args, **kwargs)
      stmt.pending_prepared
    end

    def async_query_stream(sql, *args, **kwargs)
      stmt = PreparedStatement.new(self, sql)
      stmt.bind_args(*args, **kwargs)
      stmt.pending_prepared_stream
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
      appender = create_appender(table)

      return appender unless block_given?

      yield appender
      appender.flush
      appender.close
    end

    private

    def create_appender(table)
      t1, t2 = table.split('.')
      t2 ? Appender.new(self, t1, t2) : Appender.new(self, t2, t1)
    end

    alias execute query
    alias async_execute async_query
    alias open connect
    alias close disconnect
  end
end
