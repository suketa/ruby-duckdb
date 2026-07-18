# frozen_string_literal: true

require 'bigdecimal'

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

      # Creates a new DuckDB::Value with a UINT8 (UTINYINT) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_uint8(255)
      #
      # @param value [Integer] the integer value (0..255)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_uint8(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_UINT8, 'UINT8')

        _create_uint8(value)
      end

      # Creates a new DuckDB::Value with a UINT16 (USMALLINT) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_uint16(65_535)
      #
      # @param value [Integer] the integer value (0..65535)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_uint16(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_UINT16, 'UINT16')

        _create_uint16(value)
      end

      # Creates a new DuckDB::Value with a UINT32 (UINTEGER) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_uint32(4_294_967_295)
      #
      # @param value [Integer] the integer value (0..4294967295)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_uint32(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_UINT32, 'UINT32')

        _create_uint32(value)
      end

      # Creates a new DuckDB::Value with a UINT64 (UBIGINT) value.
      #
      #   require 'duckdb'
      #   value = DuckDB::Value.create_uint64(18_446_744_073_709_551_615)
      #
      # @param value [Integer] the integer value (0..18446744073709551615)
      # @return [DuckDB::Value]
      # @raise [ArgumentError] if value is not an Integer or out of range
      def create_uint64(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_UINT64, 'UINT64')

        _create_uint64(value)
      end

      # Creates a DuckDB::Value of FLOAT type.
      #
      #   value = DuckDB::Value.create_float(1.5)
      #
      # @param value [Numeric] the numeric value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not a Numeric.
      def create_float(value)
        check_type!(value, [Integer, Float])
        _create_float(value)
      end

      # Creates a DuckDB::Value of DOUBLE type.
      #
      #   value = DuckDB::Value.create_double(1.5)
      #
      # @param value [Numeric] the numeric value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not a Numeric.
      def create_double(value)
        check_type!(value, [Integer, Float])
        _create_double(value)
      end

      # Creates a DuckDB::Value of VARCHAR type.
      #
      #   value = DuckDB::Value.create_varchar('hello')
      #
      # @param value [String] the string value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not a String.
      def create_varchar(value)
        check_type!(value, String)
        check_utf8_compatible!(value)
        _create_varchar(value)
      end

      # Creates a DuckDB::Value of BLOB type.
      #
      #   value = DuckDB::Value.create_blob("\x00\x01\x02".b)
      #
      # @param value [String] the binary string value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not a BINARY encoded String.
      def create_blob(value)
        check_type!(value, String)
        check_binary!(value)
        _create_blob(value)
      end

      # Creates a DuckDB::Value of HUGEINT type.
      #
      #   value = DuckDB::Value.create_hugeint(1_234_567_890_123_456_789_012_345)
      #
      # @param value [Integer] the integer value (-(2**127)..(2**127 - 1))
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not an Integer or out of range.
      def create_hugeint(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_HUGEINT, 'HUGEINT')

        lower, upper = integer_to_hugeint(value)
        _create_hugeint(lower, upper)
      end

      # Creates a DuckDB::Value of UHUGEINT type.
      #
      #   value = DuckDB::Value.create_uhugeint(340_282_366_920_938_463_463_374_607_431_768_211_455)
      #
      # @param value [Integer] the integer value (0..(2**128 - 1))
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not an Integer or out of range.
      def create_uhugeint(value)
        check_type!(value, Integer)
        check_range!(value, RANGE_UHUGEINT, 'UHUGEINT')

        lower, upper = integer_to_hugeint(value)
        _create_uhugeint(lower, upper)
      end

      # Creates a DuckDB::Value of DECIMAL type.
      #
      #   value = DuckDB::Value.create_decimal(BigDecimal('12345.678'))
      #
      # @param value [BigDecimal] the decimal value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not a BigDecimal or its width is out of range (1..38).
      def create_decimal(value)
        check_type!(value, BigDecimal)

        width = _decimal_width(value)
        check_range!(width, RANGE_DECIMAL_WIDTH, 'DECIMAL width')

        lower, upper = decimal_to_hugeint(value)
        _create_decimal(lower, upper, width, value.scale)
      end

      # Creates a DuckDB::Value of DATE type.
      # The argument is parsed leniently: a Date, a Time, or a String
      # accepted by Date.parse, matching Appender#append_date.
      #
      #   value = DuckDB::Value.create_date(Date.new(2026, 7, 12))
      #   value = DuckDB::Value.create_date('2026-07-12')
      #
      # @param value [Date, Time, String] the date value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Date.
      def create_date(value)
        date = _parse_date(value)
        _create_date(date.year, date.month, date.day)
      end

      # Creates a DuckDB::Value of TIME type (microsecond precision).
      # The argument is parsed leniently: a Time or a String accepted by
      # Time.parse, matching Appender#append_time.
      #
      #   value = DuckDB::Value.create_time(Time.now)
      #   value = DuckDB::Value.create_time('12:34:56.789')
      #
      # @param value [Time, String] the time value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_time(value)
        time = _parse_time(value)
        _create_time(time.hour, time.min, time.sec, time.usec)
      end

      # Creates a DuckDB::Value of TIME_NS type (nanosecond precision).
      # The argument is parsed leniently: a Time or a String accepted by
      # Time.parse. Nanoseconds are preserved.
      #
      #   value = DuckDB::Value.create_time_ns(Time.now)
      #   value = DuckDB::Value.create_time_ns('12:34:56.123456789')
      #
      # @param value [Time, String] the time value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_time_ns(value)
        time = _parse_time(value)
        _create_time_ns(time.hour, time.min, time.sec, time.nsec)
      end

      # Creates a DuckDB::Value of TIMETZ type (time with UTC offset).
      # The argument is parsed leniently: a Time (the offset is taken from
      # the Time) or a String accepted by Time.parse.
      #
      #   value = DuckDB::Value.create_time_tz(Time.now)
      #   value = DuckDB::Value.create_time_tz('12:34:56.789012+05:30')
      #
      # @param value [Time, String] the time value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_time_tz(value)
        time = _parse_time(value)
        micros = (((((time.hour * 60) + time.min) * 60) + time.sec) * 1_000_000) + time.usec
        _create_time_tz(micros, time.utc_offset)
      end

      # Creates a DuckDB::Value of TIMESTAMP type (microsecond precision).
      # The argument is parsed leniently: a Time, a Date, or a String
      # accepted by Time.parse, matching Appender#append_timestamp.
      #
      #   value = DuckDB::Value.create_timestamp(Time.now)
      #   value = DuckDB::Value.create_timestamp('2026-07-12 12:34:56.789')
      #
      # @param value [Time, Date, String] the timestamp value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_timestamp(value)
        time = to_time(value)
        _create_timestamp(time.year, time.month, time.day, time.hour, time.min, time.sec, time.usec)
      end

      # Creates a DuckDB::Value of TIMESTAMP_S type (second precision).
      # The argument is parsed leniently: a Time, a Date, or a String
      # accepted by Time.parse. Sub-second input is truncated to seconds.
      #
      #   value = DuckDB::Value.create_timestamp_s(Time.now)
      #   value = DuckDB::Value.create_timestamp_s('2026-07-12 12:34:56')
      #
      # @param value [Time, Date, String] the timestamp value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_timestamp_s(value)
        time = to_time(value)
        _create_timestamp_s(time.year, time.month, time.day, time.hour, time.min, time.sec)
      end

      # Creates a DuckDB::Value of TIMESTAMP_MS type (millisecond precision).
      # The argument is parsed leniently: a Time, a Date, or a String
      # accepted by Time.parse. Sub-millisecond input is truncated.
      #
      #   value = DuckDB::Value.create_timestamp_ms(Time.now)
      #   value = DuckDB::Value.create_timestamp_ms('2026-07-12 12:34:56.789')
      #
      # @param value [Time, Date, String] the timestamp value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_timestamp_ms(value)
        time = to_time(value)
        _create_timestamp_ms(time.year, time.month, time.day, time.hour, time.min, time.sec, time.usec)
      end

      # Creates a DuckDB::Value of TIMESTAMP_NS type (nanosecond precision).
      # The argument is parsed leniently: a Time, a Date, or a String
      # accepted by Time.parse. Nanoseconds are preserved.
      #
      #   value = DuckDB::Value.create_timestamp_ns(Time.now)
      #   value = DuckDB::Value.create_timestamp_ns('2026-07-12 12:34:56.123456789')
      #
      # @param value [Time, Date, String] the timestamp value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_timestamp_ns(value)
        time = to_time(value)
        _create_timestamp_ns(time.year, time.month, time.day, time.hour, time.min, time.sec, time.nsec)
      end

      # Creates a DuckDB::Value of TIMESTAMP WITH TIME ZONE type.
      # The argument is parsed leniently: a Time, a Date, or a String
      # accepted by Time.parse. The instant is stored (as microseconds since
      # the Unix epoch), so the input's UTC offset is honored.
      #
      #   value = DuckDB::Value.create_timestamp_tz(Time.now)
      #   value = DuckDB::Value.create_timestamp_tz('2026-07-12 12:34:56.789+05:30')
      #
      # @param value [Time, Date, String] the timestamp value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ cannot be parsed to a Time.
      def create_timestamp_tz(value)
        time = to_time(value)
        _create_timestamp_tz((time.to_i * 1_000_000) + time.usec)
      end

      # Creates a DuckDB::Value of INTERVAL type.
      # Unlike the other temporal creators, the input is strict: it must be
      # a DuckDB::Interval.
      #
      #   interval = DuckDB::Interval.new(interval_months: 14, interval_days: 3, interval_micros: 12_000_000)
      #   value = DuckDB::Value.create_interval(interval)
      #
      # @param value [DuckDB::Interval] the interval value.
      # @return [DuckDB::Value] the created Value object.
      # @raise [ArgumentError] if +value+ is not a DuckDB::Interval.
      def create_interval(value)
        check_type!(value, DuckDB::Interval)

        _create_interval(value.interval_months, value.interval_days, value.interval_micros)
      end

      # Creates a DuckDB::Value of LIST type.
      # The first argument is the element type: a Symbol (e.g. :integer) or a
      # DuckDB::LogicalType.
      # The second argument is an Array of DuckDB::Value elements.
      #
      #   values = [1, 2, 3].map { |i| DuckDB::Value.create_int32(i) }
      #   list = DuckDB::Value.create_list(:integer, values)
      #
      # @param child_type [Symbol, DuckDB::LogicalType] the element logical type.
      # @param values [Array<DuckDB::Value>] the list elements.
      # @return [DuckDB::Value] the created Value object.
      # @raise [DuckDB::Error] if child_type cannot be resolved to a
      #   DuckDB::LogicalType.
      # @raise [ArgumentError] if values is not an Array of DuckDB::Value.
      def create_list(child_type, values)
        check_value_array!(values)

        _create_list(DuckDB::LogicalType.resolve(child_type), values)
      end

      # Creates a DuckDB::Value of ARRAY type. The array size is the number
      # of elements given.
      # The first argument is the element type: a Symbol (e.g. :integer) or a
      # DuckDB::LogicalType.
      # The second argument is an Array of DuckDB::Value elements.
      #
      #   values = [1, 2, 3].map { |i| DuckDB::Value.create_int32(i) }
      #   array = DuckDB::Value.create_array(:integer, values)
      #
      # @param child_type [Symbol, DuckDB::LogicalType] the element logical type.
      # @param values [Array<DuckDB::Value>] the array elements.
      # @return [DuckDB::Value] the created Value object.
      # @raise [DuckDB::Error] if child_type cannot be resolved to a
      #   DuckDB::LogicalType.
      # @raise [ArgumentError] if values is not an Array of DuckDB::Value.
      def create_array(child_type, values)
        check_value_array!(values)

        _create_array(DuckDB::LogicalType.resolve(child_type), values)
      end

      # Creates a DuckDB::Value of STRUCT type.
      # The first argument is the field spec: a Hash of field name to field
      # type (a Symbol or a DuckDB::LogicalType, as accepted by
      # DuckDB::LogicalType.create_struct) or a STRUCT DuckDB::LogicalType.
      # The second argument is an Array of DuckDB::Value field values,
      # positional, matching the struct's field order.
      #
      #   values = [DuckDB::Value.create_int32(1), DuckDB::Value.create_varchar('x')]
      #   struct = DuckDB::Value.create_struct({ a: :integer, b: :varchar }, values)
      #
      #   # equivalent, with an explicit STRUCT logical type:
      #   struct_type = DuckDB::LogicalType.create_struct(a: :integer, b: :varchar)
      #   struct = DuckDB::Value.create_struct(struct_type, values)
      #
      # @param struct_type [Hash, DuckDB::LogicalType] the field spec or STRUCT logical type.
      # @param values [Array<DuckDB::Value>] the field values, in field order.
      # @return [DuckDB::Value] the created Value object.
      # @raise [DuckDB::Error] if a field type in the Hash cannot be resolved
      #   to a DuckDB::LogicalType.
      # @raise [ArgumentError] if struct_type is not a Hash or a STRUCT
      #   DuckDB::LogicalType, values is not an Array of DuckDB::Value, or
      #   values.size does not match the struct's field count.
      def create_struct(struct_type, values)
        struct_type = DuckDB::LogicalType.create_struct(**struct_type) if struct_type.is_a?(Hash)
        check_type!(struct_type, DuckDB::LogicalType)
        raise ArgumentError, "expected STRUCT LogicalType, got #{struct_type.type}" unless struct_type.type == :struct

        check_value_array!(values)
        # The C API's duckdb_create_struct_value reads exactly child_count
        # values from the buffer with no count argument, so a size mismatch
        # here would cause an out-of-bounds read in C.
        n = struct_type.child_count
        raise ArgumentError, "expected #{n} values for STRUCT, got #{values.size}" unless values.size == n

        _create_struct(struct_type, values)
      end

      # Creates a DuckDB::Value of MAP type.
      # The first argument is the type spec: a one-pair Hash of key type to
      # value type (each a Symbol or a DuckDB::LogicalType, as accepted by
      # DuckDB::LogicalType.create_map) or a MAP DuckDB::LogicalType.
      # The second argument is a Hash of DuckDB::Value keys to DuckDB::Value
      # values.
      #
      #   entries = {
      #     DuckDB::Value.create_varchar('a') => DuckDB::Value.create_int32(1),
      #     DuckDB::Value.create_varchar('b') => DuckDB::Value.create_int32(2)
      #   }
      #   map = DuckDB::Value.create_map({ varchar: :integer }, entries)
      #
      #   # equivalent, with an explicit MAP logical type:
      #   map_type = DuckDB::LogicalType.create_map(:varchar, :integer)
      #   map = DuckDB::Value.create_map(map_type, entries)
      #
      # @param map_type [Hash, DuckDB::LogicalType] the type spec or MAP logical type.
      # @param entries [Hash{DuckDB::Value => DuckDB::Value}] the map entries.
      # @return [DuckDB::Value] the created Value object.
      # @raise [DuckDB::Error] if a type in the Hash spec cannot be resolved
      #   to a DuckDB::LogicalType.
      # @raise [ArgumentError] if map_type is not a one-pair Hash or a MAP
      #   DuckDB::LogicalType, or entries is not a Hash of DuckDB::Value to
      #   DuckDB::Value.
      def create_map(map_type, entries)
        map_type = resolve_map_type(map_type)
        check_type!(entries, Hash)
        check_value_array!(entries.keys)
        check_value_array!(entries.values)

        _create_map(map_type, entries.keys, entries.values)
      end

      private

      def to_time(value)
        value.is_a?(Date) ? value.to_time : _parse_time(value)
      end

      def resolve_map_type(map_type)
        if map_type.is_a?(Hash)
          unless map_type.size == 1
            raise ArgumentError, "expected exactly one key type => value type pair, got #{map_type.size}"
          end

          map_type = DuckDB::LogicalType.create_map(*map_type.first)
        end
        check_type!(map_type, DuckDB::LogicalType)
        raise ArgumentError, "expected MAP LogicalType, got #{map_type.type}" unless map_type.type == :map

        map_type
      end

      def check_value_array!(values)
        check_type!(values, Array)
        values.each { |value| check_type!(value, DuckDB::Value) }
      end

      def check_range!(value, range, type_name)
        raise ArgumentError, "value out of range for #{type_name} (#{range})" unless range.cover?(value)
      end

      def check_type!(value, expected)
        types = Array(expected)
        return if types.any? { |type| value.is_a?(type) }

        raise ArgumentError, "expected #{types.map(&:name).join(' or ')}, got #{value.class.name}"
      end

      def check_binary!(value)
        return if value.encoding == Encoding::BINARY

        raise ArgumentError, "expected BINARY encoding, got #{value.encoding}"
      end

      def check_utf8_compatible!(value)
        return if [Encoding::UTF_8, Encoding::US_ASCII].include?(value.encoding) && value.valid_encoding?

        raise ArgumentError, "expected valid UTF-8 or US-ASCII string, got #{value.encoding}"
      end
    end
  end
end
