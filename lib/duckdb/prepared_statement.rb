require 'date'

module DuckDB
  # The DuckDB::PreparedStatement encapsulates connection with DuckDB prepared
  # statement.
  #
  #   require 'duckdb'
  #   db = DuckDB::Database.open('duckdb_database')
  #   con = db.connect
  #   sql ='SELECT name, email FROM users WHERE email = ?'
  #   stmt = PreparedStatement.new(con, sql)
  #   stmt.bind(1, 'email@example.com')
  #   stmt.execute
  class PreparedStatement

    RANGE_INT16 = -32768..32767
    RANGE_INT32 = -2147483648..2147483647
    RANGE_INT64 = -9223372036854775808..9223372036854775807

    def bind_hugeint(i, value)
      case value
      when Integer
        bind_varchar(i, value.to_s)
      else
        raise(ArgumentError, "2nd argument `#{value}` must be Integer.")
      end
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter. The index of first parameter is
    # 1 not 0.
    # The second argument value is the value of prepared statement parameter.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name, email FROM users WHERE email = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind(1, 'email@example.com')
    def bind(i, value)
      case value
      when NilClass
        respond_to?(:bind_null) ? bind_null(i) : raise(DuckDB::Error, 'This bind method does not support nil value. Re-compile ruby-duckdb with DuckDB version >= 0.1.1')
      when Float
        bind_double(i, value)
      when Integer
        case value
        when RANGE_INT64
          bind_int64(i, value)
        else
          bind_varchar(i, value.to_s)
        end
      when String
        if defined?(DuckDB::Blob)
          blob?(value) ? bind_blob(i, value) : bind_varchar(i, value)
        else
          bind_varchar(i, value)
        end
      when TrueClass, FalseClass
        bind_bool(i, value)
      when Time
        bind_varchar(i, value.strftime('%Y-%m-%d %H:%M:%S.%N'))
      when Date
        bind_varchar(i, value.strftime('%Y-%m-%d'))
      else
        raise(DuckDB::Error, "not supported type `#{value}` (#{value.class})")
      end
    end

    private

    def blob?(value)
      value.instance_of?(DuckDB::Blob) || value.encoding == Encoding::BINARY
    end
  end
end
