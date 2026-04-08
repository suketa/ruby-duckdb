# frozen_string_literal: true

module DuckDB
  class Value
    class << self
      include DuckDB::Converter

      # Creates a new DuckDB::Value with a boolean value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_bool(true)
      #
      # @param value [TrueClass, FalseClass] the boolean value
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not a boolean
      def create_bool(value)
        check_type!(value, [TrueClass, FalseClass])

        _create_bool(value)
      end

      # Creates a new DuckDB::Value with an INT8 (TINYINT) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_int8(127)
      #
      # @param value [Integer] the integer value (-128..127)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_int8(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_INT8, 'INT8')

        _create_int8(value)
      end

      # Creates a new DuckDB::Value with an INT16 (SMALLINT) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_int16(32_767)
      #
      # @param value [Integer] the integer value (-32768..32767)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_int16(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_INT16, 'INT16')

        _create_int16(value)
      end

      # Creates a new DuckDB::Value with an INT32 (INTEGER) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_int32(2_147_483_647)
      #
      # @param value [Integer] the integer value (-2147483648..2147483647)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_int32(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_INT32, 'INT32')

        _create_int32(value)
      end

      # Creates a new DuckDB::Value with an INT64 (BIGINT) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_int64(9_223_372_036_854_775_807)
      #
      # @param value [Integer] the integer value (-9223372036854775808..9223372036854775807)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_int64(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_INT64, 'INT64')

        _create_int64(value)
      end

      private

      def check_range!(value, range, type_name)
        raise ArgumentError, "value out of range for #{type_name} (#{range})" unless range.cover?(value)
      end

      def check_type!(value, expected)
        types = Array(expected)
        return if types.any? { |type| value.is_a?(type) }

        raise ArgumentError, "expected #{types.map(&:name).join(' or ')}, got #{value.class.name}"
      end
    end
  end
end
