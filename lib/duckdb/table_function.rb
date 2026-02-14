# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::TableFunction encapsulates a DuckDB table function.
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.new
  #   conn = db.connect
  #
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
  #     output.size = 0
  #   end
  #
  #   conn.register_table_function(tf)
  #
  class TableFunction
    # TableFunction#initialize is defined in C extension

    #
    # Creates a new table function with a declarative API.
    #
    # The create method automatically handles the "done flag" pattern
    # required by table functions. The block should return the number
    # of rows generated (return 0 when done).
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
    def self.create(name:, columns:, parameters: nil, &)
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

      # Wrap execute block with done flag pattern
      done = false

      tf.init do |_init_info|
        done = false
      end

      tf.execute do |func_info, output|
        if done
          output.size = 0
          next
        end

        # Call user's block and get returned size
        size = yield(func_info, output)

        # Set output size and check if done
        output.size = size
        done = true if size.zero?
      end

      tf
    end
    # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
  end
end
