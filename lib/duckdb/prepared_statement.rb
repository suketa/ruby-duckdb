require 'date'
require_relative './converter'

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
    include DuckDB::Converter

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

    def bind_date(i, value)
      case value
      when Date, Time
        date = value
      else
        begin
          date = Date.parse(value)
        rescue => e
          raise(ArgumentError, "Cannot parse argument value to date. #{e.message}")
        end
      end

      _bind_date(i, date.year, date.month, date.day)
    end

    def bind_time(i, value)
      case value
      when Time
        time = value
      else
        begin
          time = Time.parse(value)
        rescue => e
          raise(ArgumentError, "Cannot parse argument value to time. #{e.message}")
        end
      end
      usec = time.strftime('%N')[0, 6].to_i
      _bind_time(i, time.hour, time.min, time.sec, usec)
    end

    def bind_interval(i, value)
      raise(DuckDB::Error, 'bind_interval is not available with your duckdb version. please install duckdb latest version at first') unless respond_to?(:_bind_interval, true)
      raise ArgumentError, "Argument `#{value}` must be a string." unless value.is_a?(String)

      hash = iso8601_interval_to_hash(value)

      months, days, micros = hash_to__append_interval_args(hash)

      _bind_interval(i, months, days, micros)
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
        bind_null(i)
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
        blob?(value) ? bind_blob(i, value) : bind_varchar(i, value)
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
