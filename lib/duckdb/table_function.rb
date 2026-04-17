# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::TableFunction encapsulates a DuckDB table function.
  #
  # NOTE: DuckDB::TableFunction is experimental now.
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.new
  #   conn = db.connect
  #
  #   # Low-level API:
  #   tf = DuckDB::TableFunction.new
  #   tf.name = 'my_function'
  #   tf.add_parameter(DuckDB::LogicalType::BIGINT)
  #
  #   tf.bind do |bind_info|
  #     bind_info.add_result_column('value', DuckDB::LogicalType::BIGINT)
  #   end
  #
  #   tf.execute do |func_info, output|
  #     # Fill output data...
  #     0  # Return 0 to signal done
  #   end
  #
  #   conn.register_table_function(tf)
  #
  #   # High-level API (recommended):
  #   tf = DuckDB::TableFunction.create(
  #     name: 'my_function',
  #     parameters: [DuckDB::LogicalType::BIGINT],
  #     columns: { 'value' => DuckDB::LogicalType::BIGINT }
  #   ) do |func_info, output|
  #     # Fill output data...
  #     0  # Return row count (0 when done)
  #   end
  #
  class TableFunction
    # TableFunction#initialize is defined in C extension

    @table_adapters = {}

    class << self
      #
      # Creates a new table function with a declarative API.
      #
      # @param name [String] The name of the table function
      # @param parameters [Array<LogicalType>, Hash<String, LogicalType>] Function parameters (optional)
      # @param columns [Hash<String, LogicalType>] Output columns (required)
      # @yield [func_info, output] The execute block that generates data
      # @yieldparam func_info [FunctionInfo] Function execution context
      # @yieldparam output [DataChunk] Output data chunk to fill
      # @yieldreturn [Integer] Number of rows generated (0 when done)
      # @return [TableFunction] The configured table function
      #
      # @example Simple range function
      #   tf = TableFunction.create(
      #     name: 'my_range',
      #     parameters: [LogicalType::BIGINT],
      #     columns: { 'value' => LogicalType::BIGINT }
      #   ) do |func_info, output|
      #     # Generate data...
      #     0  # Signal done
      #   end
      #
      # @example Function that returns data
      #   tf = TableFunction.create(
      #     name: 'my_function',
      #     columns: { 'value' => LogicalType::BIGINT }
      #   ) do |func_info, output|
      #     vec = output.get_vector(0)
      #     # Fill vector...
      #     3  # Return row count
      #   end
      #
      # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      def create(name:, columns:, parameters: nil, &)
        raise ArgumentError, 'name is required' unless name
        raise ArgumentError, 'columns are required' unless columns
        raise ArgumentError, 'block is required' unless block_given?

        tf = new
        tf.name = name

        # Add parameters (positional or named)
        if parameters
          case parameters
          when Array
            parameters.each { |type| tf.add_parameter(type) }
          when Hash
            parameters.each { |param_name, type| tf.add_named_parameter(param_name, type) }
          else
            raise ArgumentError, 'parameters must be Array or Hash'
          end
        end

        # Set bind callback to add result columns
        tf.bind do |bind_info|
          columns.each do |col_name, col_type|
            bind_info.add_result_column(col_name, col_type)
          end
        end

        # Set init callback (required by DuckDB)
        tf.init do |_init_info|
          # No-op
        end

        # Set execute callback - user's block returns row count
        tf.execute do |func_info, output|
          size = yield(func_info, output)
          output.size = Integer(size)
        end

        tf
      end
      # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity

      # Registers a table adapter for a Ruby class.
      #
      # The adapter is used by +DuckDB::Connection#expose_as_table+ to convert
      # instances of +klass+ into a DuckDB table function. The adapter must respond
      # to +call(object, name, columns: nil)+ and return a +DuckDB::TableFunction+.
      #
      # == Implementing a Table Adapter
      #
      # An adapter is any object that responds to +call(object, name, columns: nil)+.
      # The +columns:+ keyword argument allows callers to override the column schema;
      # the adapter should fall back to its own schema detection when it is +nil+.
      #
      # The execute block passed to +DuckDB::TableFunction.create+ must:
      # - Write one batch of rows into +output+ per call
      # - Return the number of rows written as an +Integer+
      # - Return +0+ to signal that all data has been exhausted
      #
      # @example Minimal adapter for CSV objects
      #   class CSVTableAdapter
      #     def call(csv, name, columns: nil)
      #       columns ||= infer_columns(csv)
      #
      #       DuckDB::TableFunction.create(name:, columns:) do |_func_info, output|
      #         row = csv.readline
      #         if row
      #           row.each_with_index { |cell, i| output.set_value(i, 0, cell[1]) }
      #           1  # wrote one row
      #         else
      #           csv.rewind
      #           0  # signal end of data
      #         end
      #       end
      #     end
      #
      #     private
      #
      #     def infer_columns(csv)
      #       headers = csv.first.headers
      #       csv.rewind
      #       headers.each_with_object({}) { |h, hsh| hsh[h] = DuckDB::LogicalType::VARCHAR }
      #     end
      #   end
      #
      #   # Register and use:
      #   DuckDB::TableFunction.add_table_adapter(CSV, CSVTableAdapter.new)
      #   con.expose_as_table(csv, 'csv_table')
      #   con.query('SELECT * FROM csv_table()').to_a
      #
      # @param klass [Class] the Ruby class to register an adapter for (e.g. +CSV+)
      # @param adapter [#call] the adapter object
      # @return [void]
      #
      def add_table_adapter(klass, adapter)
        @table_adapters[klass] = adapter
      end

      # Returns the table adapter registered for the given class, or +nil+ if none.
      #
      # @param klass [Class] the Ruby class to look up
      # @return [#call, nil] the registered adapter, or +nil+ if not found
      #
      # @example
      #   adapter = DuckDB::TableFunction.table_adapter_for(CSV)
      #
      def table_adapter_for(klass)
        @table_adapters[klass]
      end
    end
  end
end
