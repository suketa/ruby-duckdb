# frozen_string_literal: true

module DuckDB
  # The DuckDB::Connection encapsulates connection with DuckDB database.
  #
  #   require 'duckdb'
  #   db = DuckDB::Database.open
  #   con = db.connect
  #   con.query(sql)
  class Connection
    include DuckDB::TableNameParser

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
      return _query_sql(sql) if stmts.size == 1

      result = nil
      stmts.each do |stmt|
        result = stmt.execute
        stmt.destroy
      end
      result
    ensure
      stmts&.destroy
    end

    # Extracts multiple SQL statements and returns them as prepared statements
    # without executing them. The caller controls when to execute and destroy
    # each statement.
    #
    # @param sql [String] a string containing one or more SQL statements separated by semicolons
    # @return [Array<DuckDB::PreparedStatement>] the extracted prepared statements
    # @raise [DuckDB::Error] if the SQL is invalid
    #
    # @example Execute each statement and destroy
    #   stmts = con.extract('SELECT 1; SELECT 2; SELECT 3;')
    #   stmts.each do |stmt|
    #     result = stmt.execute
    #     # process result...
    #     stmt.destroy
    #   end
    def extract(sql)
      stmts = ExtractedStatements.new(self, sql)
      stmts.to_a
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

    # :call-seq:
    #   connection.appender(table, schema: nil, catalog: nil) -> DuckDB::Appender
    #   connection.appender(table, schema: nil, catalog: nil) { |appender| ... } -> self
    #
    # Returns a DuckDB::Appender for bulk-inserting rows into +table+.
    # If a block is given, the appender is flushed and closed automatically after the block.
    #
    # +schema:+ and +catalog:+ optionally qualify the table.
    #
    # Raises DuckDB::Error if the table (or schema/catalog) does not exist.
    #
    # Table name parsing (quoting, dot-notation) is handled by DuckDB::Appender.new.
    # See DuckDB::Appender.new for details on quoting and dot-notation.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
    #
    #   # block form (recommended) — flushes and closes automatically
    #   con.appender('users') do |a|
    #     a.append_row(1, 'Alice')
    #     a.append_row(2, 'Bob')
    #   end
    #
    #   # with schema
    #   con.appender('users', schema: 'main') do |a|
    #     a.append_row(3, 'Carol')
    #   end
    #
    #   # manual form
    #   appender = con.appender('users')
    #   appender.append_row(4, 'Dave')
    #   appender.close
    def appender(table, schema: nil, catalog: nil, &)
      table, schema, catalog = parse_connection_appender_table(table, schema, catalog)
      run_appender_block(Appender.new(self, table, schema: schema, catalog: catalog), &)
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

    # Registers a custom logical type with the connection.
    # The logical type must have an alias set via {LogicalType#alias=} before registration.
    # The alias becomes the SQL type name.
    #
    # @param logical_type [DuckDB::LogicalType] the logical type to register
    # @raise [TypeError] if argument is not a DuckDB::LogicalType
    # @raise [DuckDB::Error] if the type has no alias set or registration fails
    # @return [self]
    #
    # @example Register an enum type
    #   mood = DuckDB::LogicalType.create_enum('happy', 'sad', 'neutral')
    #   mood.alias = 'mood'
    #   con.register_logical_type(mood)
    #   con.query('CREATE TABLE t (m mood)')
    def register_logical_type(logical_type)
      raise TypeError, "#{logical_type.class} is not a DuckDB::LogicalType" unless logical_type.is_a?(LogicalType)

      _register_logical_type(logical_type)
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
    # @return [self]
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

    # Registers an aggregate function set with the connection.
    # An aggregate function set groups multiple overloads of an aggregate function under one name,
    # allowing DuckDB to dispatch to the correct implementation based on argument types.
    #
    # @param aggregate_function_set [DuckDB::AggregateFunctionSet] the function set to register
    # @return [self]
    # @raise [TypeError] if argument is not a DuckDB::AggregateFunctionSet
    #
    # @example Register multiple overloads under one name
    #   af_bigint = DuckDB::AggregateFunction.new
    #   af_bigint.name = 'my_sum'
    #   af_bigint.return_type = DuckDB::LogicalType::BIGINT
    #   af_bigint.add_parameter(DuckDB::LogicalType::BIGINT)
    #   af_bigint.set_init   { 0 }
    #   af_bigint.set_update  { |state, val| state + val }
    #   af_bigint.set_combine { |s1, s2| s1 + s2 }
    #
    #   af_double = DuckDB::AggregateFunction.new
    #   af_double.name = 'my_sum'
    #   af_double.return_type = DuckDB::LogicalType::DOUBLE
    #   af_double.add_parameter(DuckDB::LogicalType::DOUBLE)
    #   af_double.set_init   { 0.0 }
    #   af_double.set_update  { |state, val| state + val }
    #   af_double.set_combine { |s1, s2| s1 + s2 }
    #
    #   set = DuckDB::AggregateFunctionSet.new(:my_sum)
    #   set.add(af_bigint).add(af_double)
    #   con.register_aggregate_function_set(set)
    def register_aggregate_function_set(aggregate_function_set)
      unless aggregate_function_set.is_a?(AggregateFunctionSet)
        raise TypeError, "#{aggregate_function_set.class} is not a DuckDB::AggregateFunctionSet"
      end

      _register_aggregate_function_set(aggregate_function_set)
    end

    # Registers an aggregate function with the connection.
    #
    # @param aggregate_function [DuckDB::AggregateFunction] the aggregate function to register
    # @raise [TypeError] if argument is not a DuckDB::AggregateFunction
    # @return [self]
    def register_aggregate_function(aggregate_function)
      unless aggregate_function.is_a?(AggregateFunction)
        raise TypeError, "#{aggregate_function.class} is not a DuckDB::AggregateFunction"
      end

      _register_aggregate_function(aggregate_function)
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
    # @return [void]
    #
    # @example Expose a CSV as a table
    #   require 'csv'
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

    # [EXPERIMENTAL] Appends an Arrow producer into an existing table.
    #
    # Reads +producer+ (any object responding to +#arrow_c_stream+, such as a
    # ruby-polars +DataFrame+ or a +DuckDB::Result+) as an Arrow C stream and
    # appends its chunks into the existing table +table+. The producer's Arrow
    # columns must line up with the table's columns positionally and by count.
    # DuckDB casts compatible column types (e.g. INTEGER into a BIGINT column);
    # a type that cannot be cast (e.g. a non-numeric VARCHAR into an INTEGER
    # column) or a column-count mismatch raises +DuckDB::Error+.
    #
    # This is not transactional: a schema mismatch fails before any rows are
    # written, but a rarer mid-stream failure can leave earlier chunks
    # appended. Wrap the call in your own transaction for all-or-nothing.
    #
    # This API is built on DuckDB's unstable Arrow C API and may change in any
    # minor release.
    #
    # @param table [String] the name of the existing target table
    # @param producer [#arrow_c_stream] the Arrow producer
    # @raise [TypeError] if +producer+ does not respond to +#arrow_c_stream+
    # @return [Integer] the number of rows appended
    #
    # @example Load a Polars DataFrame into a table
    #   con.query('CREATE TABLE t (id INTEGER, name VARCHAR)')
    #   con.append_arrow('t', polars_df)
    #
    def append_arrow(table, producer)
      unless producer.respond_to?(:arrow_c_stream)
        raise TypeError, "Arrow producer must respond to #arrow_c_stream, got #{producer.class}"
      end

      stream = producer.arrow_c_stream # keep the producer's stream alive for the duration
      address = stream.to_i
      begin
        append_arrow_chunks(table, address)
      ensure
        _arrow_release(address)
      end
    end

    # Returns the names of the tables referenced by the given SQL query,
    # without executing it.
    #
    # @param query [String] the SQL query to inspect
    # @param qualified [Boolean] if true, returns each table reference as
    #   written in the query, keeping any catalog/schema qualification;
    #   if false (default), bare table names only
    # @return [Array<String>] the referenced table names
    # @raise [DuckDB::Error] if the query cannot be parsed
    #
    # @example
    #   con.table_names('SELECT * FROM users u JOIN orders o ON u.id = o.user_id')
    #   #=> ["users", "orders"]
    #   con.table_names('SELECT * FROM memory.main.users', qualified: true)
    #   #=> ["memory.main.users"]
    def table_names(query, qualified: false)
      _get_table_names(query, qualified).to_ruby
    end

    private

    # Drives the Arrow stream at +address+ chunk by chunk into +table+,
    # returning the number of rows appended.
    def append_arrow_chunks(table, address)
      converted_schema = _arrow_converted_schema(address)
      rows = 0
      appender(table) do |app|
        while (chunk = _arrow_next_chunk(address, converted_schema))
          rows += chunk.size
          app.append_data_chunk(chunk)
        end
      end
      rows
    end

    def run_appender_block(appender, &)
      return appender unless block_given?

      yield appender
      appender.flush
      appender.close
    end

    # Silently pre-parses dot-notation so Appender.new receives clean values
    # and does not emit a misleading "DuckDB::Appender.new" warning.
    # con.appender('a.b') has always split on dot — no warning needed.
    # Quoted table names pass through unchanged for Appender.new to handle.
    def parse_connection_appender_table(table, schema, catalog)
      return [table, schema, catalog] if quoted_table_name?(table)
      return [table, schema, catalog] unless table.include?('.')

      dot_notation_split(table, schema, catalog)
    end

    alias execute query
    alias async_execute async_query
    alias open connect
    alias close disconnect
    alias prepare prepared_statement
  end
end
