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

      prepare(sql) do |stmt|
        stmt.bind_args(*args, **kwargs)
        stmt.execute
      end
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
    #
    #   sql = 'SELECT * FROM users WHERE name = $name AND email = $email'
    #   pending_result = con.async_query(sql, name: 'Dave', email: 'dave@example.com')
    #   pending_result.execute_task while pending_result.state == :not_ready
    #   result = pending_result.execute_pending
    #   result.each.first
    #
    def async_query(sql, *args, **kwargs)
      prepare(sql) do |stmt|
        stmt.bind_args(*args, **kwargs)
        stmt.pending_prepared
      end
    end

    #
    # executes sql with args asynchronously and provides streaming result.
    # The first argument sql must be SQL string.
    # The rest arguments are parameters of SQL string.
    # This method returns DuckDB::PendingResult object.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_file')
    #   con = db.connect
    #
    #   sql = 'SELECT * FROM users WHERE name = $name AND email = $email'
    #   pending_result = con.async_query_stream(sql, name: 'Dave', email: 'dave@example.com')
    #
    #   pending_result.execute_task while pending_result.state == :not_ready
    #   result = pending_result.execute_pending
    #   result.each.first
    #
    def async_query_stream(sql, *args, **kwargs)
      prepare(sql) do |stmt|
        stmt.bind_args(*args, **kwargs)
        stmt.pending_prepared_stream
      end
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
    # If block is given, the block is executed with PreparedStatement object
    # and the object is cleaned up immediately.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_file')
    #   con = db.connect
    #
    #   sql = 'SELECT * FROM users WHERE name = $name AND email = $email'
    #   stmt = con.prepared_statement(sql)
    #   stmt.bind_args(name: 'Dave', email: 'dave@example.com')
    #   result = stmt.execute
    #
    #   # or
    #   result = con.prepared_statement(sql) do |stmt|
    #              stmt.bind_args(name: 'Dave', email: 'dave@example.com')
    #              stmt.execute
    #            end
    #
    def prepared_statement(str, &)
      return PreparedStatement.new(self, str) unless block_given?

      PreparedStatement.prepare(self, str, &)
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
    alias prepare prepared_statement
  end
end
