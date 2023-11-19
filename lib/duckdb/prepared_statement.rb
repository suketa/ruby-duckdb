require 'date'
require 'bigdecimal'
require_relative 'converter'

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

    RANGE_INT16 = -32_768..32_767
    RANGE_INT32 = -2_147_483_648..2_147_483_647
    RANGE_INT64 = -9_223_372_036_854_775_808..9_223_372_036_854_775_807

    def pending_prepared
      PendingResult.new(self)
    end

    def pending_prepared_stream
      raise DuckDB::Error, 'DuckDB::Result.use_chunk_each must be true.' unless DuckDB::Result.use_chunk_each?

      PendingResult.new(self, true)
    end

    # binds all parameters with SQL prepared statement.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name FROM users WHERE id = ?'
    #   # or
    #   # sql ='SELECT name FROM users WHERE id = $id'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind_args([1])
    #   # or
    #   # stmt.bind_args(id: 1)
    def bind_args(*args, **kwargs)
      args.each.with_index(1) do |arg, i|
        bind(i, arg)
      end
      kwargs.each do |key, value|
        bind(key, value)
      end
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value is to expected Integer value.
    # This method uses bind_varchar internally.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name FROM users WHERE bigint_col = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind_hugeint(1, 1_234_567_890_123_456_789_012_345)
    def bind_hugeint(index, value)
      case value
      when Integer
        bind_varchar(index, value.to_s)
      else
        raise(ArgumentError, "2nd argument `#{value}` must be Integer.")
      end
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value must be Integer value.
    # This method uses duckdb_bind_hugeint internally.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name FROM users WHERE bigint_col = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind_hugeint_internal(1, 1_234_567_890_123_456_789_012_345)
    def bind_hugeint_internal(index, value)
      lower, upper = integer_to_hugeint(value)
      _bind_hugeint(index, lower, upper)
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value is to expected date.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name FROM users WHERE birth_day = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind(1, Date.today)
    #   #  or you can specify date string.
    #   # stmt.bind(1, '2021-02-23')
    def bind_date(index, value)
      date = _parse_date(value)

      _bind_date(index, date.year, date.month, date.day)
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value is to expected time value.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name FROM users WHERE birth_time = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind(1, Time.now)
    #   #  or you can specify time string.
    #   # stmt.bind(1, '07:39:45')
    def bind_time(index, value)
      time = _parse_time(value)

      _bind_time(index, time.hour, time.min, time.sec, time.usec)
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value is to expected time value.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name FROM users WHERE created_at = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind(1, Time.now)
    #   #  or you can specify timestamp string.
    #   # stmt.bind(1, '2022-02-23 07:39:45')
    def bind_timestamp(index, value)
      time = _parse_time(value)

      _bind_timestamp(index, time.year, time.month, time.day, time.hour, time.min, time.sec, time.usec)
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value is to expected ISO8601 time interval string.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT value FROM intervals WHERE interval = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind(1, 'P1Y2D')
    def bind_interval(index, value)
      value = Interval.to_interval(value)
      _bind_interval(index, value.interval_months, value.interval_days, value.interval_micros)
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value is the value of prepared statement parameter.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name, email FROM users WHERE email = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind(1, 'email@example.com')
    def bind(index, value)
      case index
      when Integer
        bind_with_index(index, value)
      when String
        bind_with_name(index, value)
      when Symbol
        bind_with_name(index.to_s, value)
      else
        raise(ArgumentError, "1st argument `#{index}` must be Integer or String or Symbol.")
      end
    end

    private

    def bind_with_index(index, value)
      case value
      when NilClass
        bind_null(index)
      when Float
        bind_double(index, value)
      when Integer
        case value
        when RANGE_INT64
          bind_int64(index, value)
        else
          bind_varchar(index, value.to_s)
        end
      when String
        blob?(value) ? bind_blob(index, value) : bind_varchar(index, value)
      when TrueClass, FalseClass
        bind_bool(index, value)
      when Time
        bind_varchar(index, value.strftime('%Y-%m-%d %H:%M:%S.%N'))
      when Date
        bind_varchar(index, value.strftime('%Y-%m-%d'))
      when BigDecimal
        bind_varchar(index, value.to_s('F'))
      else
        raise(DuckDB::Error, "not supported type `#{value}` (#{value.class})")
      end
    end

    def bind_with_name(name, value)
      raise DuckDB::Error, 'not supported binding with name' unless respond_to?(:bind_parameter_index)

      i = bind_parameter_index(name)
      bind_with_index(i, value)
    end

    def blob?(value)
      value.instance_of?(DuckDB::Blob) || value.encoding == Encoding::BINARY
    end
  end
end
