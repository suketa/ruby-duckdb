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
      interval = Interval.new
      interval.year = months / 12
      interval.month = months % 12
      interval.day = days
      interval.hour = micros / 3_600_000_000
      interval.min = (micros % 3_600_000_000) / 60_000_000
      interval.sec = (micros % 60_000_000) / 1_000_000
      interval.usec = micros % 1_000_000
      interval
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

    def iso8601_interval_to_hash(value)
      hash = {}
      if /(-{0,1})P(-{0,1}\d+Y){0,1}(-{0,1}\d+M){0,1}(-{0,1}\d+D){0,1}T{0,1}(-{0,1}\d+H){0,1}(-{0,1}\d+M){0,1}((-{0,1}\d+)\.{0,1}(\d*)S){0,1}/ =~ value
        hash['Y'] = Regexp.last_match[2].to_i
        hash['M'] = Regexp.last_match[3].to_i
        hash['D'] = Regexp.last_match[4].to_i
        hash['H'] = Regexp.last_match[5].to_i
        hash['TM'] = Regexp.last_match[6].to_i
        hash['S'] = Regexp.last_match[8].to_i
        hash['MS'] = Regexp.last_match[9].to_s.ljust(6, '0')[0, 6].to_i
        hash['MS'] *= -1 if hash['S'].negative?
      else
        raise ArgumentError, "The argument `#{value}` can't be parse."
      end
      hash
    end

    def hash_to__append_interval_args(hash)
      months = (hash['Y'] * 12) + hash['M']
      days = hash['D']
      micros = (((hash['H'] * 3600) + (hash['TM'] * 60) + hash['S']) * 1_000_000) + hash['MS']
      [months, days, micros]
    end
  end
end
