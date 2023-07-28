# frozen_string_literal: true

module DuckDB
  # Interval class represents DuckDB's interval type value.
  #
  # The usage is as follows:
  #
  #   require 'duckdb'
  #
  #   interval = DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 14706123456)
  #   # or
  #   # interval = DuckDB::Interval.mk_interval(year: 1, month: 2, day: 3, hour: 4, min: 5, sec: 6, usec: 123456)
  #   # or
  #   # interval = DuckDB::Interval.iso8601_parse('P1Y2M3DT4H5M6.123456S')
  #
  #   db = DuckDB::Database.open # database in memory
  #   con = db.connect
  #
  #   con.execute('CREATE TABLE users (value INTERVAL)')
  #
  #   require 'duckdb'
  #   db = DuckDB::Database.open
  #   con = db.connect
  #   con.query('CREATE TABLE intervals (interval_value INTERVAL)')
  #   appender = con.appender('intervals')
  #   appender
  #     .begin_row
  #     .append_interval(interval)
  #     .end_row
  #     .flush
  class Interval
    ISO8601_REGEXP = Regexp.compile(
      '(?<negativ>-{0,1})P
      (?<year>-{0,1}\d+Y){0,1}
      (?<month>-{0,1}\d+M){0,1}
      (?<day>-{0,1}\d+D){0,1}
      T{0,1}
      (?<hour>-{0,1}\d+H){0,1}
      (?<min>-{0,1}\d+M){0,1}
      ((?<sec>-{0,1}\d+)\.{0,1}(?<usec>\d*)S){0,1}',
      Regexp::EXTENDED
    )

    class << self
      # parses the ISO8601 format string and return the Interval object.
      #
      #   DuckDB::Interval.iso8601_parse('P1Y2M3DT4H5M6.123456S')
      #   => #<DuckDB::Interval:0x00007f9b9c0b3b60 @interval_months=14, @interval_days=3, @interval_micros=14706123456>
      def iso8601_parse(value)
        m = ISO8601_REGEXP.match(value)

        raise ArgumentError, "The argument `#{value}` can't be parse." if m.nil?

        year, month, day, hour, min, sec, usec = matched_to_i(m)

        mk_interval(year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec)
      end

      # creates the Interval object.
      #
      #   DuckDB::Interval.mk_interval(year: 1, month: 2, day: 3, hour: 4, min: 5, sec: 6, usec: 123456)
      #   => #<DuckDB::Interval:0x00007f9b9c0b3b60 @interval_months=14, @interval_days=3, @interval_micros=14706123456>
      def mk_interval(year: 0, month: 0, day: 0, hour: 0, min: 0, sec: 0, usec: 0)
        Interval.new(
          interval_months: (year * 12) + month,
          interval_days: day,
          interval_micros: (((hour * 3600) + (min * 60) + sec) * 1_000_000) + usec
        )
      end

      # Convert the value to the Interval object.
      # The value can be String or Interval object.
      # If the value is String, it is parsed as ISO8601 format.
      # If the value is Interval object, it is returned as is.
      # Otherwise, ArgumentError is raised.
      #
      #   DuckDB::Interval.to_interval('P1Y2M3DT4H5M6.123456S')
      #   => #<DuckDB::Interval:0x00007f9b9c0b3b60 @interval_months=14, @interval_days=3, @interval_micros=14706123456>
      #
      #   interval = DuckDB::Interval.to_interval('P1Y2M3DT4H5M6.123456S')
      #   DuckDB::Interval.to_interval(interval)
      #   => #<DuckDB::Interval:0x00007f9b9c0b3b60 @interval_months=14, @interval_days=3, @interval_micros=14706123456>
      def to_interval(value)
        case value
        when String
          iso8601_parse(value)
        when Interval
          value
        else
          raise ArgumentError, "The argument `#{value}` can't be parse."
        end
      end

      private

      def matched_to_i(matched)
        sign = to_sign(matched)
        sec = to_sec(matched)
        usec = to_usec(matched)
        usec *= -1 if sec.negative?
        value = [
          to_year(matched), to_month(matched), to_day(matched), to_hour(matched), to_min(matched), sec, usec
        ]
        sign.positive? ? value : value.map { |v| v * sign }
      end

      def to_sign(matched)
        matched[:negativ] == '-' ? -1 : 1
      end

      def to_year(matched)
        matched[:year].to_i
      end

      def to_month(matched)
        matched[:month].to_i
      end

      def to_day(matched)
        matched[:day].to_i
      end

      def to_hour(matched)
        matched[:hour].to_i
      end

      def to_min(matched)
        matched[:min].to_i
      end

      def to_sec(matched)
        matched[:sec].to_i
      end

      def to_usec(matched)
        matched[:usec].to_s.ljust(6, '0')[0, 6].to_i
      end
    end

    attr_reader :interval_months, :interval_days, :interval_micros

    # creates the Interval object.
    # The arguments are the number of months, days, and microseconds.
    # The default value is 0.
    #
    #   DuckDB::Interval.new(interval_months: 1, interval_days: 2, interval_micros: 3)
    #   => #<DuckDB::Interval:0x00007f9b9c0b3b60 @interval_months=1, @interval_days=2, @interval_micros=3>
    def initialize(interval_months: 0, interval_days: 0, interval_micros: 0)
      @interval_months = interval_months
      @interval_days = interval_days
      @interval_micros = interval_micros
    end

    def ==(other)
      other.is_a?(Interval) &&
        @interval_months == other.interval_months &&
        @interval_days == other.interval_days &&
        @interval_micros == other.interval_micros
    end
  end
end
