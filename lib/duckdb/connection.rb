# frozen_string_literal: true

module DuckDB
  # The DuckDB::Connection encapsulates connection with DuckDB database.
  #
  #   require 'duckdb'
  #   db = DuckDB::Database.open
  #   con = db.connect
  #   con.query(sql)
  class Connection
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
    def query(sql, *args, **kwargs)
      return query_multi_sql(sql) if args.empty? && kwargs.empty?

      prepare(sql) do |stmt|
        stmt.bind_args(*args, **kwargs)
        stmt.execute
      end
    end

    def query_multi_sql(sql)
      stmts = ExtractedStatements.new(self, sql)
      result = nil
      stmts.each do |stmt|
        result = stmt.execute
        stmt.destroy
      end
      result
    ensure
      stmts&.destroy
    end

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
    def async_query(sql, *args, **kwargs)
      prepare(sql) do |stmt|
        stmt.bind_args(*args, **kwargs)
        stmt.pending_prepared
      end
    end

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
    def async_query_stream(sql, *, **)
      warn("`#{self.class}#{__method__}` will be deprecated. Use `#{self.class}#async_query` instead.")

      async_query(sql, *, **)
    end

    # connects DuckDB database
    # The first argument is DuckDB::Database object
    def connect(db)
      conn = _connect(db)
      return conn unless block_given?

      begin
        yield conn
      ensure
        conn.disconnect
      end
    end

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
    def prepared_statement(str, &)
      return PreparedStatement.new(self, str) unless block_given?

      PreparedStatement.prepare(self, str, &)
    end

    # returns Appender object.
    # The first argument is table name
    def appender(table, &)
      appender = create_appender(table)
      run_appender_block(appender, &)
    end

    if Appender.respond_to?(:create_query)
      # :call-seq:
      #   connection.appender_from_query(query, types, table_name = nil, column_names = nil) -> DuckDB::Appender
      #
      # Creates an appender object that executes the given query with any data appended to it.
      # The `table_name` parameter is used to refer to the appended data in the query. If omitted, it defaults
      # to "appended_data".
      # The `column_names` parameter provides names for the columns of the appended data. If omitted, it
      # defaults to "col1", "col2", etc.
      #
      #   require 'duckdb'
      #   db = DuckDB::Database.open
      #   con = db.connect
      #   con.query('CREATE TABLE t (i INT PRIMARY KEY, value VARCHAR)')
      #   query = 'INSERT OR REPLACE INTO t SELECT i, val FROM my_appended_data'
      #   types = [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::VARCHAR]
      #   appender = con.appender_from_query(query, types, 'my_appended_data', %w[i val])
      #   appender.append_row(1, 'hello world')
      #   appender.close
      def appender_from_query(query, types, table_name = nil, column_names = nil, &)
        appender = Appender.create_query(self, query, types, table_name, column_names)
        run_appender_block(appender, &)
      end
    end

    # Registers a scalar function with the connection.
    # :nodoc:
    #
    # Scalar functions with Ruby callbacks require single-threaded execution.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #
    #   # IMPORTANT: Set single-threaded execution before registering
    #   con.execute('SET threads=1')
    #
    #   sf = DuckDB::ScalarFunction.new
    #   sf.name = 'add_one'
    #   sf.return_type = DuckDB::LogicalType.new(4) # INTEGER
    #   sf.set_function { |val| val + 1 }
    #
    #   con.register_scalar_function(sf)
    #
    # Raises DuckDB::Error if threads setting is not 1.
    def register_scalar_function(scalar_function)
      # Check threads setting
      result = execute("SELECT current_setting('threads')")
      thread_count = result.first.first.to_i

      if thread_count > 1
        raise DuckDB::Error,
              'Scalar functions with Ruby callbacks require single-threaded execution. ' \
              "Current threads setting: #{thread_count}. " \
              "Execute 'SET threads=1' before registering scalar functions."
      end

      _register_scalar_function(scalar_function)
    end

    private

    def run_appender_block(appender, &)
      return appender unless block_given?

      yield appender
      appender.flush
      appender.close
    end

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
