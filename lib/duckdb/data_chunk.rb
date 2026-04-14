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
      vector = cached_vector(col_idx)
      type_id = cached_type_id(col_idx, vector)

      # Handle NULL
      if value.nil?
        vector.set_validity(row_idx, false)
        return value
      end

      case type_id
      when :boolean
        MemoryHelper.write_boolean(cached_data(col_idx, vector), row_idx, value)
      when :tinyint
        MemoryHelper.write_tinyint(cached_data(col_idx, vector), row_idx, value)
      when :smallint
        MemoryHelper.write_smallint(cached_data(col_idx, vector), row_idx, value)
      when :integer
        MemoryHelper.write_integer(cached_data(col_idx, vector), row_idx, value)
      when :bigint
        MemoryHelper.write_bigint(cached_data(col_idx, vector), row_idx, value)
      when :utinyint
        MemoryHelper.write_utinyint(cached_data(col_idx, vector), row_idx, value)
      when :usmallint
        MemoryHelper.write_usmallint(cached_data(col_idx, vector), row_idx, value)
      when :uinteger
        MemoryHelper.write_uinteger(cached_data(col_idx, vector), row_idx, value)
      when :ubigint
        MemoryHelper.write_ubigint(cached_data(col_idx, vector), row_idx, value)
      when :float
        MemoryHelper.write_float(cached_data(col_idx, vector), row_idx, value)
      when :double
        MemoryHelper.write_double(cached_data(col_idx, vector), row_idx, value)
      when :varchar
        vector.assign_string_element(row_idx, value.to_s)
      when :blob
        vector.assign_string_element_len(row_idx, value.to_s)
      when :timestamp
        MemoryHelper.write_timestamp(cached_data(col_idx, vector), row_idx, value)
      when :timestamp_tz
        MemoryHelper.write_timestamp_tz(cached_data(col_idx, vector), row_idx, value)
      when :date
        MemoryHelper.write_date(cached_data(col_idx, vector), row_idx, value)
      else
        raise ArgumentError, "Unsupported type for DataChunk#set_value: #{type_id} for value `#{value.inspect}`"
      end

      value
    end
    # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength

    #
    # Resets the data chunk so it can be reused for another batch of rows.
    #
    # @return [DuckDB::DataChunk] self
    #
    def reset
      _reset
      # duckdb_data_chunk_reset may invalidate previously returned data pointers,
      # so drop the cache; vector/type caches remain valid across resets.
      @data_cache = nil
      self
    end

    private

    def cached_vector(col_idx)
      @vector_cache ||= {}
      @vector_cache[col_idx] ||= get_vector(col_idx)
    end

    def cached_type_id(col_idx, vector)
      @type_id_cache ||= {}
      @type_id_cache[col_idx] ||= vector.logical_type.type
    end

    def cached_data(col_idx, vector)
      @data_cache ||= {}
      @data_cache[col_idx] ||= vector.get_data
    end
  end
end
