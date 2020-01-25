require 'date'

module DuckDB
  class PreparedStatement
    def bind(i, value)
      case value
      when NilClass
        respond_to?(:bind_null) ? bind_null(i) : rb_raise(DuckDB::Error, 'This bind method does not support nil value. Re-compile ruby-duckdb with DuckDB version >= 0.1.1')
      when Float
        bind_double(i, value)
      when Integer
        bind_int64(i, value)
      when String
        bind_varchar(i, value)
      when TrueClass, FalseClass
        bind_boolean(i, value)
      when Time
        bind_varchar(i, value.strftime('%Y-%m-%d %H:%M:%S.%N'))
      when Date
        bind_varchar(i, value.strftime('%Y-%m-%d'))
      else
        rb_raise(DuckDB::Error, "not supported type #{value} (value.class)")
      end
    end
  end
end

