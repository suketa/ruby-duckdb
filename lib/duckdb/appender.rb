require 'date'

module DuckDB
  if defined?(DuckDB::Appender)
    class Appender
      def append(value)
        case value
        when NilClass
          append_null
        when Float
          append_double(value)
        when Integer
          append_int64(value)
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
