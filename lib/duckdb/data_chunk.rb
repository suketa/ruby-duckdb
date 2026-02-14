# frozen_string_literal: true

module DuckDB
  #
  # The DuckDB::DataChunk represents a chunk of data for table function output.
  #
  # During table function execution, data chunks are used to return rows.
  #
  # Example:
  #
  #   done = false
  #   table_function.init { |_init_info| done = false }
  #
  #   table_function.execute do |func_info, output|
  #     if done
  #       output.size = 0  # Signal completion
  #     else
  #       # High-level API
  #       output.set_value(0, 0, 42)        # column 0, row 0, value 42
  #       output.set_value(1, 0, 'Alice')   # column 1, row 0, value 'Alice'
  #       output.size = 1
  #       done = true
  #     end
  #   end
  #
  class DataChunk
    # Most methods are defined in C extension (ext/duckdb/data_chunk.c)

    #
    # Sets a value at the specified column and row index.
    # Type conversion is automatic based on the column's logical type.
    #
    # @param col_idx [Integer] Column index (0-based)
    # @param row_idx [Integer] Row index (0-based)
    # @param value [Object] Value to set (Integer, Float, String, Time, Date, nil)
    # @return [Object] The value that was set
    #
    # @example Set integer value
    #   output.set_value(0, 0, 42)
    #
    # @example Set string value
    #   output.set_value(1, 0, 'hello')
    #
    # @example Set NULL value
    #   output.set_value(0, 1, nil)
    #
    # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
    def set_value(col_idx, row_idx, value)
      vector = get_vector(col_idx)
      logical_type = vector.logical_type
      type_id = logical_type.type

      # Handle NULL
      if value.nil?
        vector.set_validity(row_idx, false)
        return value
      end

      case type_id
      when :boolean
        data = vector.get_data
        MemoryHelper.write_boolean(data, row_idx, value)
      when :tinyint
        data = vector.get_data
        MemoryHelper.write_tinyint(data, row_idx, value)
      when :smallint
        data = vector.get_data
        MemoryHelper.write_smallint(data, row_idx, value)
      when :integer
        data = vector.get_data
        MemoryHelper.write_integer(data, row_idx, value)
      when :bigint
        data = vector.get_data
        MemoryHelper.write_bigint(data, row_idx, value)
      when :utinyint
        data = vector.get_data
        MemoryHelper.write_utinyint(data, row_idx, value)
      when :usmallint
        data = vector.get_data
        MemoryHelper.write_usmallint(data, row_idx, value)
      when :uinteger
        data = vector.get_data
        MemoryHelper.write_uinteger(data, row_idx, value)
      when :ubigint
        data = vector.get_data
        MemoryHelper.write_ubigint(data, row_idx, value)
      when :float
        data = vector.get_data
        MemoryHelper.write_float(data, row_idx, value)
      when :double
        data = vector.get_data
        MemoryHelper.write_double(data, row_idx, value)
      when :varchar
        vector.assign_string_element(row_idx, value.to_s)
      when :blob
        vector.assign_string_element_len(row_idx, value.to_s)
      else
        raise ArgumentError, "Unsupported type for DataChunk#set_value: #{type_id}"
      end

      value
    end
    # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
  end
end
