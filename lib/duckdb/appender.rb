require 'date'

module DuckDB
  if defined?(DuckDB::Appender)
    class Appender
      RANGE_INT16 = -32768..32767
      RANGE_INT32 = -2147483648..2147483647
      RANGE_INT64 = -9223372036854775808..9223372036854775807

      def append_hugeint(value)
        case value
        when Integer
          append_varchar(value.to_s)
        else
          rb_raise(ArgumentError, "2nd argument `#{value}` must be Integer.")
        end
      end

      def append(value)
        case value
        when NilClass
          append_null
        when Float
          append_double(value)
        when Integer
          case value
          when RANGE_INT16
            append_int16(value)
          when RANGE_INT32
            append_int32(value)
          when RANGE_INT64
            append_int64(value)
          else
            append_hugeint(value)
          end
        when String
          if defined?(DuckDB::Blob)
            blob?(value) ? append_blob(value) : append_varchar(value)
          else
            append_varchar(value)
          end
        when TrueClass, FalseClass
          append_bool(value)
        when Time
          append_varchar(value.strftime('%Y-%m-%d %H:%M:%S.%N'))
        when Date
          append_varchar(value.strftime('%Y-%m-%d'))
        else
          rb_raise(DuckDB::Error, "not supported type #{value} (value.class)")
        end
      end

      private

      def blob?(value)
        value.instance_of?(DuckDB::Blob) || value.encoding == Encoding::BINARY
      end
    end
  end
end
