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

    def _to_time_from_duckdb_time(hour, minute, second, microsecond)
      Time.parse(
        format(
          '%<hour>02d:%<minute>02d:%<second>02d.%<microsecond>06d',
          hour: hour,
          minute: minute,
          second: second,
          microsecond: microsecond
        )
      )
    end

    def _to_hugeint_from_vector(lower, upper)
      (upper << HALF_HUGEINT_BIT) + lower
    end

    def _to_decimal_from_hugeint(width, scale, upper, lower = nil)
      v = lower.nil? ? upper : _to_hugeint_from_vector(lower, upper)
      _to_decimal_from_value(width, scale, v)
    end

    def _to_decimal_from_value(_width, scale, value)
      v = value.to_s
      v = v.rjust(scale + 1, '0') if v.length < scale
      v[-scale, 0] = '.' if scale.positive?
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

    def _parse_date(value)
      case value
      when Date, Time
        value
      else
        begin
          Date.parse(value)
        rescue StandardError => e
          raise(ArgumentError, "Cannot parse `#{value.inspect}` to Date object. #{e.message}")
        end
      end
    end

    def _parse_time(value)
      case value
      when Time
        value
      else
        begin
          Time.parse(value)
        rescue StandardError => e
          raise(ArgumentError, "Cannot parse `#{value.inspect}` to Time object. #{e.message}")
        end
      end
    end

    private

    def integer_to_hugeint(value)
      case value
      when Integer
        upper = value >> HALF_HUGEINT_BIT
        lower = value - (upper << HALF_HUGEINT_BIT)
        [lower, upper]
      else
        raise(ArgumentError, "The argument `#{value.inspect}` must be Integer.")
      end
    end
  end
end
