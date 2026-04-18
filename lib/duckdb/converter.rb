# frozen_string_literal: true

require 'date'
require_relative 'interval'
require_relative 'converter/int_to_sym'

module DuckDB
  QueryProgress = Struct.new(:percentage, :rows_processed, :total_rows_to_process)

  module Converter # :nodoc: all
    RANGE_INT8 = -128..127
    RANGE_INT16 = -32_768..32_767
    RANGE_INT32 = -2_147_483_648..2_147_483_647
    RANGE_INT64 = -9_223_372_036_854_775_808..9_223_372_036_854_775_807
    RANGE_UINT8 = 0..255
    RANGE_UINT16 = 0..65_535
    RANGE_UINT32 = 0..4_294_967_295
    RANGE_UINT64 = 0..18_446_744_073_709_551_615
    RANGE_HUGEINT = (-(1 << 127)..((1 << 127) - 1))
    RANGE_UHUGEINT = (0..((1 << 128) - 1))
    RANGE_DECIMAL_WIDTH = 1..38

    HALF_HUGEINT_BIT = 64
    HALF_HUGEINT = 1 << HALF_HUGEINT_BIT
    LOWER_HUGEINT_MASK = HALF_HUGEINT - 1
    EPOCH = Time.local(1970, 1, 1)
    EPOCH_UTC = Time.utc(1970, 1, 1)

    module_function

    def default_timezone_utc?
      defined?(DuckDB.default_timezone) && DuckDB.default_timezone == :utc
    end

    def _to_infinity(value)
      if value.positive?
        DuckDB::Infinity::POSITIVE
      else
        DuckDB::Infinity::NEGATIVE
      end
    end

    def _to_date(year, month, day)
      Date.new(year, month, day)
    end

    # rubocop:disable Metrics/ParameterLists
    def _to_time(year, month, day, hour, minute, second, microsecond)
      Time.public_send(
        default_timezone_utc? ? :utc : :local,
        year, month, day, hour, minute, second, microsecond
      )
    end
    # rubocop:enable Metrics/ParameterLists

    def _to_time_from_duckdb_time(hour, minute, second, microsecond)
      return Time.utc(1970, 1, 1, hour, minute, second, microsecond) if default_timezone_utc?

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

    def _to_time_from_duckdb_timestamp_s(time)
      if default_timezone_utc?
        EPOCH_UTC + time
      else
        EPOCH + time
      end
    end

    def _to_time_from_duckdb_timestamp_ms(time)
      _to_time_from_duckdb_timestamp_s(time / 1000).then do |tm|
        _to_time(tm.year, tm.month, tm.day, tm.hour, tm.min, tm.sec, time % 1000 * 1000)
      end
    end

    def _to_time_from_duckdb_timestamp_ns(time)
      _to_time_from_duckdb_timestamp_s(time / 1_000_000_000).then do |tm|
        _to_time(tm.year, tm.month, tm.day, tm.hour, tm.min, tm.sec, time % 1_000_000_000 / 1000)
      end
    end

    def _to_time_from_duckdb_time_ns(nanos)
      hour = nanos / 3_600_000_000_000
      nanos %= 3_600_000_000_000
      min = nanos / 60_000_000_000
      nanos %= 60_000_000_000
      sec = nanos / 1_000_000_000
      microsecond = (nanos % 1_000_000_000) / 1_000
      _to_time_from_duckdb_time(hour, min, sec, microsecond)
    end

    def _to_time_from_duckdb_time_tz(hour, min, sec, micro, timezone)
      tz_offset = format_timezone_offset(timezone)
      time_str = format('%<hour>02d:%<min>02d:%<sec>02d.%<micro>06d%<tz>s',
                        hour: hour, min: min, sec: sec, micro: micro, tz: tz_offset)
      Time.parse(time_str)
    end

    def _to_time_from_duckdb_timestamp_tz(bits)
      micro = bits % 1_000_000
      time = EPOCH_UTC + (bits / 1_000_000)
      timestamp_str = format_timestamp_with_micro(time, micro)
      Time.parse(timestamp_str)
    end

    def _to_hugeint_from_vector(lower, upper)
      (upper << HALF_HUGEINT_BIT) + lower
    end

    def _hugeint_lower(value)
      value & LOWER_HUGEINT_MASK
    end

    def _hugeint_upper(value)
      value >> HALF_HUGEINT_BIT
    end

    def _decimal_width(value)
      value.to_s('F').gsub(/[^0-9]/, '').length
    end

    def _to_decimal_from_hugeint(width, scale, upper, lower = nil)
      v = lower.nil? ? upper : _to_hugeint_from_vector(lower, upper)
      _to_decimal_from_value(width, scale, v)
    end

    def _to_decimal_from_value(_width, scale, value)
      BigDecimal("#{value}e-#{scale}")
    end

    def _decimal_to_unscaled(value, scale)
      (value * (10**scale)).to_i
    end

    def _to_interval_from_vector(months, days, micros)
      Interval.new(interval_months: months, interval_days: days, interval_micros: micros)
    end

    def _parse_date(value)
      case value
      when Date, Time
        value
      else
        Date.parse(value)
      end
    rescue StandardError => e
      raise(ArgumentError, "Cannot parse `#{value.inspect}` to Date object. #{e.message}")
    end

    def _parse_time(value)
      case value
      when Time
        value
      when DateTime
        value.to_time
      else
        Time.parse(value)
      end
    rescue StandardError => e
      raise(ArgumentError, "Cannot parse `#{value.inspect}` to Time object. #{e.message}")
    end

    def _parse_deciaml(value)
      case value
      when BigDecimal
        value
      else
        begin
          BigDecimal(value.to_s)
        rescue StandardError => e
          raise(ArgumentError, "Cannot parse `#{value.inspect}` to BigDecimal object. #{e.message}")
        end
      end
    end

    def _to_query_progress(percentage, rows_processed, total_rows_to_process)
      DuckDB::QueryProgress.new(percentage, rows_processed, total_rows_to_process).freeze
    end

    def format_timezone_offset(timezone)
      sign = timezone.negative? ? '-' : '+'
      offset = timezone.abs
      tzhour = offset / 3600
      tzmin = (offset % 3600) / 60
      format('%<sign>s%<hour>02d:%<min>02d', sign: sign, hour: tzhour, min: tzmin)
    end

    def format_timestamp_with_micro(time, micro)
      format('%<year>04d-%<mon>02d-%<day>02d %<hour>02d:%<min>02d:%<sec>02d.%<micro>06d +0000',
             year: time.year, mon: time.month, day: time.day,
             hour: time.hour, min: time.min, sec: time.sec, micro: micro)
    end

    private

    def integer_to_hugeint(value)
      case value
      when Integer
        [_hugeint_lower(value), _hugeint_upper(value)]
      else
        raise(ArgumentError, "The argument `#{value.inspect}` must be Integer.")
      end
    end

    def decimal_to_hugeint(value)
      integer_value = (value * (10**value.scale)).to_i
      integer_to_hugeint(integer_value)
    rescue FloatDomainError => e
      raise(ArgumentError, "The argument `#{value.inspect}` must be converted to Integer. #{e.message}")
    end
  end
end
