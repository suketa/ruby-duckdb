# frozen_string_literal: true

module DuckDB
  module Casting
    def cast(value, type) # rubocop:disable Metrics/MethodLength
      type = type.type if type.is_a?(DuckDB::LogicalType)
      case type
      when :integer, :bigint, :hugeint
        Integer(value)
      when :float, :double
        Float(value)
      when :varchar
        value.to_s
      when :timestamp
        cast_as_timestamp(value)
      when :date
        cast_as_date(value)
      else
        raise ArgumentError, "Unsupported type: #{type} for value: #{value}"
      end
    end

    private

    def cast_as_timestamp(value)
      DuckDB::Converter._parse_time(value)
    end

    def cast_as_date(value)
      DuckDB::Converter._parse_date(value)
    end
  end

  extend Casting
end
