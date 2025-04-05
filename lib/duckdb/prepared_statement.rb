# frozen_string_literal: true

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

    class << self
      # return DuckDB::PreparedStatement object.
      # The first argument is DuckDB::Connection object.
      # The second argument is SQL string.
      # If block is given, the block is executed and the statement is destroyed.
      #
      #  require 'duckdb'
      #  db = DuckDB::Database.open('duckdb_database')
      #  con = db.connection
      #  DuckDB::PreparedStatement.prepare(con, 'SELECT * FROM users WHERE id = ?') do |stmt|
      #    stmt.bind(1, 1)
      #    stmt.execute
      #  end
      def prepare(con, sql)
        stmt = new(con, sql)
        return stmt unless block_given?

        begin
          yield stmt
        ensure
          stmt.destroy
        end
      end
    end

    def pending_prepared
      PendingResult.new(self)
    end

    def pending_prepared_stream
      PendingResult.new(self, true)
    end

    # returns statement type. The return value is one of the following symbols:
    #  :invalid, :select, :insert, :update, :explain, :delete, :prepare, :create,
    #  :execute, :alter, :transaction, :copy, :analyze, :variable_set, :create_func,
    #  :drop, :export, :pragma, :vacuum, :call, :set, :load, :relation, :extension,
    #  :logical_plan, :attach, :detach, :multi
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   stmt = con.prepared_statement('SELECT * FROM users')
    #   stmt.statement_type # => :select
    def statement_type
      i = _statement_type
      Converter::IntToSym.statement_type_to_sym(i)
    end

    # returns parameter type. The argument must be index of parameter.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.execute('CREATE TABLE users (id INTEGER, name VARCHAR(255))')
    #   stmt = con.prepared_statement('SELECT * FROM users WHERE id = ?')
    #   stmt.param_type(1) # => :integer
    def param_type(index)
      i = _param_type(index)
      Converter::IntToSym.type_to_sym(i)
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
    # The second argument value is to expected Integer value between 0 to 255.
    def bind_uint8(index, val)
      return _bind_uint8(index, val) if val.between?(0, 255)

      raise DuckDB::Error, "can't bind uint8(bind_uint8) to `#{val}`. The `#{val}` is out of range 0..255."
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value is to expected Integer value between 0 to 65535.
    def bind_uint16(index, val)
      return _bind_uint16(index, val) if val.between?(0, 65_535)

      raise DuckDB::Error, "can't bind uint16(bind_uint16) to `#{val}`. The `#{val}` is out of range 0..65535."
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
    #   sql ='SELECT name FROM users WHERE hugeint_col = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind_hugeint_internal(1, 1_234_567_890_123_456_789_012_345)
    def bind_hugeint_internal(index, value)
      lower, upper = integer_to_hugeint(value)
      _bind_hugeint(index, lower, upper)
    end

    # binds i-th parameter with SQL prepared statement.
    # The first argument is index of parameter.
    # The index of first parameter is 1 not 0.
    # The second argument value must be Integer value.
    # This method uses duckdb_bind_uhugeint internally.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT name FROM users WHERE uhugeint_col = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind_uhugeint(1, (2**128) - 1)
    def bind_uhugeint(index, value)
      lower, upper = integer_to_hugeint(value)
      _bind_uhugeint(index, lower, upper)
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
    # The second argument value is to expected BigDecimal value or any value
    # that can be parsed into a BigDecimal.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open('duckdb_database')
    #   con = db.connect
    #   sql ='SELECT value FROM decimals WHERE decimal = ?'
    #   stmt = PreparedStatement.new(con, sql)
    #   stmt.bind_decimal(1, BigDecimal('987654.321'))
    def bind_decimal(index, value)
      decimal = _parse_deciaml(value)
      lower, upper = decimal_to_hugeint(decimal)
      width = decimal.to_s('F').gsub(/[^0-9]/, '').length
      _bind_decimal(index, lower, upper, width, decimal.scale)
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
        bind_decimal(index, value)
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
