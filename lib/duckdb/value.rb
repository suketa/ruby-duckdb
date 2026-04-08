# frozen_string_literal: true

module DuckDB
  class Value
    class << self
      include DuckDB::Converter

      def create_bool(value)
        check_type!(value, [TrueClass, FalseClass])

        _create_bool(value)
      end

      def create_int8(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_INT8, 'INT8')

        _create_int8(value)
      end

      def create_int16(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_INT16, 'INT16')

        _create_int16(value)
      end

      def create_int32(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_INT32, 'INT32')

        _create_int32(value)
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
