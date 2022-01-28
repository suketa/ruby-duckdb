module DuckDB
  module Converter
    private

    def iso8601_interval_to_hash(value)
      digit = ''
      time = false
      hash = {}
      hash.default = 0

      value.each_char do |c|
        if '-0123456789.'.include?(c)
          digit += c
        elsif c == 'T'
          time = true
          digit = ''
        elsif c == 'M'
          m_interval_to_hash(hash, digit, time)
          digit = ''
        elsif c == 'S'
          s_interval_to_hash(hash, digit)
          digit = ''
        elsif 'YDH'.include?(c)
          hash[c] = digit.to_i
          digit = ''
        elsif c != 'P'
          raise ArgumentError, "The argument `#{value}` can't be parse."
        end
      end
      hash
    end

    def m_interval_to_hash(hash, digit, time)
      key = time ? 'TM' : 'M'
      hash[key] = digit.to_i
    end

    def s_interval_to_hash(hash, digit)
      sec, msec = digit.split('.')
      hash['S'] = sec.to_i
      hash['MS'] = "#{msec}000000"[0, 6].to_i
      hash['MS'] *= -1 if hash['S'].negative?
    end

    def hash_to__append_interval_args(hash)
      months = hash['Y'] * 12 + hash['M']
      days = hash['D']
      micros = (hash['H'] * 3600 + hash['TM'] * 60 + hash['S']) * 1_000_000 + hash['MS']
      [months, days, micros]
    end
  end
end
