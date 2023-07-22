# frozen_string_literal: true

require 'date'
require_relative 'interval'

module DuckDB
  module Converter
    HALF_HUGEINT_BIT = 64
    HALF_HUGEINT = 1 << HALF_HUGEINT_BIT
    FLIP_HUGEINT = 1 << 63

    module_function

    def _to_date(year, month, day)
      Date.new(year, month, day)
    end

    def _to_time(year, month, day, hour, minute, second, microsecond)
      Time.local(year, month, day, hour, minute, second, microsecond)
    end

    def _to_hugeint_from_vector(lower, upper)
      (upper << HALF_HUGEINT_BIT) + lower
    end

    def _to_decimal_from_vector(_width, scale, lower, upper)
      v = _to_hugeint_from_vector(lower, upper).to_s
      v = v.rjust(scale + 1, '0') if v.length < scale
      v[-scale, 0] = '.'
      BigDecimal(v)
    end

    def _to_interval_from_vector(months, days, micros)
      Interval.new(interval_months: months, interval_days: days, interval_micros: micros)
    end

    def _to_uuid_from_vector(lower, upper)
      upper = upper ^ FLIP_HUGEINT
      upper += HALF_HUGEINT if upper.negative?

      str = _to_hugeint_from_vector(lower, upper).to_s(16).rjust(32, '0')
      "#{str[0, 8]}-#{str[8, 4]}-#{str[12, 4]}-#{str[16, 4]}-#{str[20, 12]}"
    end

    private

    def integer_to_hugeint(value)
      case value
      when Integer
        upper = value >> HALF_HUGEINT_BIT
        lower = value - (upper << HALF_HUGEINT_BIT)
        [lower, upper]
      else
        raise(ArgumentError, "2nd argument `#{value}` must be Integer.")
      end
    end
  end
end
