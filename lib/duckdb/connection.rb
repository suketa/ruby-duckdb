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
      return query_sql(sql) if stmts.size == 1

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
    #
    # @overload register_scalar_function(scalar_function)
    #   Register a pre-created ScalarFunction object.
    #   @param scalar_function [DuckDB::ScalarFunction] the scalar function to register
    #
    # @overload register_scalar_function(name:, return_type:, **kwargs, &block)
    #   Create and register a scalar function inline.
    #   @param name [String, Symbol] the function name
    #   @param return_type [DuckDB::LogicalType] the return type
    #   @param parameter_type [DuckDB::LogicalType, nil] single parameter type
    #   @param parameter_types [Array<DuckDB::LogicalType>, nil] multiple parameter types
    #   @yield [*args] the function implementation
    #
    # @raise [ArgumentError] if both object and keywords/block are provided
    # @return [void]
    #
    # @example Register pre-created function
    #   sf = DuckDB::ScalarFunction.create(
    #     name: :triple,
    #     return_type: DuckDB::LogicalType::INTEGER,
    #     parameter_type: DuckDB::LogicalType::INTEGER
    #   ) { |v| v * 3 }
    #   con.register_scalar_function(sf)
    #
    # @example Register inline (single parameter)
    #   con.register_scalar_function(
    #     name: :triple,
    #     return_type: DuckDB::LogicalType::INTEGER,
    #     parameter_type: DuckDB::LogicalType::INTEGER
    #   ) { |v| v * 3 }
    #
    # @example Register inline (multiple parameters)
    #   con.register_scalar_function(
    #     name: :add,
    #     return_type: DuckDB::LogicalType::INTEGER,
    #     parameter_types: [DuckDB::LogicalType::INTEGER, DuckDB::LogicalType::INTEGER]
    #   ) { |a, b| a + b }
    def register_scalar_function(scalar_function = nil, **kwargs, &)
      # Validate: can't pass both object and inline arguments
      if scalar_function.is_a?(ScalarFunction)
        raise ArgumentError, 'Cannot pass both ScalarFunction object and keyword arguments' if kwargs.any?

        raise ArgumentError, 'Cannot pass both ScalarFunction object and block' if block_given?
      end

      sf = scalar_function || ScalarFunction.create(**kwargs, &)
      _register_scalar_function(sf)
    end

    # Registers a scalar function set with the connection.
    # A scalar function set groups multiple overloads of a function under one name,
    # allowing DuckDB to dispatch to the correct implementation based on argument types.
    #
    # @param scalar_function_set [DuckDB::ScalarFunctionSet] the function set to register
    # @return [void]
    # @raise [TypeError] if argument is not a DuckDB::ScalarFunctionSet
    #
    # @example Register multiple overloads under one name
    #   add_int = DuckDB::ScalarFunction.create(return_type: :integer, parameter_types: %i[integer integer]) { |a, b| a + b }
    #   add_dbl = DuckDB::ScalarFunction.create(return_type: :double,  parameter_types: %i[double  double])  { |a, b| a + b }
    #   set = DuckDB::ScalarFunctionSet.new(:add)
    #   set.add(add_int).add(add_dbl)
    #   con.register_scalar_function_set(set)
    def register_scalar_function_set(scalar_function_set)
      unless scalar_function_set.is_a?(ScalarFunctionSet)
        raise TypeError, "#{scalar_function_set.class} is not a DuckDB::ScalarFunctionSet"
      end

      _register_scalar_function_set(scalar_function_set)
    end

    #
    # Registers a table function with the database connection.
    #
    #   table_function = DuckDB::TableFunction.new
    #   table_function.name = 'my_function'
    #   table_function.bind { |bind_info| ... }
    #   table_function.execute { |func_info, output| ... }
    #   connection.register_table_function(table_function)
    #
    def register_table_function(table_function)
      raise ArgumentError, 'table_function must be a TableFunction' unless table_function.is_a?(TableFunction)

      check_threads
      _register_table_function(table_function)
    end

    # Exposes a Ruby object as a queryable DuckDB table function via a registered adapter.
    #
    # Looks up a table adapter registered for the object's class via
    # +DuckDB::TableFunction.add_table_adapter+, then uses it to create and register
    # a table function under the given name.
    #
    # @param object [Object] the Ruby object to expose as a table (e.g. a CSV instance)
    # @param name [String] the SQL name of the table function
    # @param columns [Hash{String => DuckDB::LogicalType}, nil] optional column schema override;
    #   if omitted, the adapter determines the columns (e.g. from headers or inference)
    # @raise [ArgumentError] if no adapter is registered for the object's class
    # @raise [DuckDB::Error] if threads setting is not 1
    # @return [void]
    #
    # @example Expose a CSV as a table
    #   require 'csv'
    #   con.execute('SET threads=1')
    #   DuckDB::TableFunction.add_table_adapter(CSV, CSVTableAdapter.new)
    #   csv = CSV.new(File.read('data.csv'), headers: true)
    #   con.expose_as_table(csv, 'csv_table')
    #   con.query('SELECT * FROM csv_table()').to_a
    #
    # @example With explicit column types
    #   con.expose_as_table(csv, 'csv_table', columns: {
    #     'id'   => DuckDB::LogicalType::BIGINT,
    #     'name' => DuckDB::LogicalType::VARCHAR
    #   })
    #
    def expose_as_table(object, name, columns: nil)
      adapter = TableFunction.table_adapter_for(object.class)
      raise ArgumentError, "No table adapter registered for #{object.class}" if adapter.nil?

      tf = adapter.call(object, name, columns:)
      register_table_function(tf)
    end

    private

    def check_threads
      result = execute("SELECT current_setting('threads')")
      thread_count = result.first.first.to_i

      return unless thread_count > 1

      raise DuckDB::Error,
            'Functions with Ruby callbacks require single-threaded execution. ' \
            "Current threads setting: #{thread_count}. " \
            "Execute 'SET threads=1' before registering functions."
    end

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
