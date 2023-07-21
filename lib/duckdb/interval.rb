module DuckDB
  class Interval
    def self.iso8601_parse(value)
      if /(-{0,1})P(-{0,1}\d+Y){0,1}(-{0,1}\d+M){0,1}(-{0,1}\d+D){0,1}T{0,1}(-{0,1}\d+H){0,1}(-{0,1}\d+M){0,1}((-{0,1}\d+)\.{0,1}(\d*)S){0,1}/ =~ value
        sec = Regexp.last_match[8].to_i
        usec = Regexp.last_match[9].to_s.ljust(6, '0')[0, 6].to_i
        usec *= -1 if sec.negative?

        Interval.new(
          year: Regexp.last_match[2].to_i,
          month: Regexp.last_match[3].to_i,
          day: Regexp.last_match[4].to_i,
          hour: Regexp.last_match[5].to_i,
          min: Regexp.last_match[6].to_i,
          sec: sec,
          usec: usec
        )
      end
    end

    attr_accessor :year, :month, :day, :hour, :min, :sec, :usec

    def initialize(year: 0, month: 0, day: 0, hour: 0, min: 0, sec: 0, usec: 0)
      @year = year
      @month = month
      @day = day
      @hour = hour
      @min = min
      @sec = sec
      @usec = usec
    end

    def ==(other)
      other.is_a?(Interval) &&
        @year == other.year &&
        @month == other.month &&
        @day == other.day &&
        @hour == other.hour &&
        @min == other.min &&
        @sec == other.sec &&
        @usec == other.usec
    end
  end
end
