# frozen_string_literal: true

require 'date'
require 'time'
require_relative 'converter'

module DuckDB
  # The DuckDB::Appender encapsulates DuckDB Appender.
  #
  #   require 'duckdb'
  #   db = DuckDB::Database.open
  #   con = db.connect
  #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
  #   appender = con.appender('users')
  #   appender.append_row(1, 'Alice')
  class Appender
    include DuckDB::Converter

    # :stopdoc:
    RANGE_INT16 = -32_768..32_767
    RANGE_INT32 = -2_147_483_648..2_147_483_647
    RANGE_INT64 = -9_223_372_036_854_775_808..9_223_372_036_854_775_807
    private_constant :RANGE_INT16, :RANGE_INT32, :RANGE_INT64
    # :startdoc:

    # :call-seq:
    #   appender.begin_row -> self
    # A nop method, provided for backwards compatibility reasons.
    # Does nothing. Only `end_row` is required.
    def begin_row
      self
    end

    # call-seq:
    #   appender.end_row -> self
    #
    # Finish the current row of appends. After end_row is called, the next row can be appended.
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_varchar('Alice')
    #     .end_row
    def end_row
      return self if _end_row

      raise_appender_error('failed to end_row')
    end

    # :call-seq:
    #   appender.flush -> self
    #
    # Flushes the appender to the table, forcing the cache of the appender to be cleared.
    # If flushing the data triggers a constraint violation or any other error, then all
    # data is invalidated, and this method raises DuckDB::Error.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_varchar('Alice')
    #     .end_row
    #     .flush
    def flush
      return self if _flush

      raise_appender_error('failed to flush')
    end

    # :call-seq:
    #   appender.close -> self
    #
    # Closes the appender by flushing all intermediate states and closing it for further appends.
    # If flushing the data triggers a constraint violation or any other error, then all data is
    # invalidated, and this method raises DuckDB::Error.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_varchar('Alice')
    #     .end_row
    #     .close
    def close
      return self if _close

      raise_appender_error('failed to close')
    end

    # call-seq:
    #   appender.append_bool(val) -> self
    #
    # Appends a boolean value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, active BOOLEAN)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_bool(true)
    #     .end_row
    #     .flush
    def append_bool(value)
      return self if _append_bool(value)

      raise_appender_error('failed to append_bool')
    end

    # call-seq:
    #   appender.append_int8(val) -> self
    #
    # Appends an int8(TINYINT) value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age TINYINT)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_int8(20)
    #     .end_row
    #     .flush
    #
    def append_int8(value)
      return self if _append_int8(value)

      raise_appender_error('failed to append_int8')
    end

    # call-seq:
    #   appender.append_int16(val) -> self
    #
    # Appends an int16(SMALLINT) value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age SMALLINT)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_int16(20)
    #     .end_row
    #     .flush
    def append_int16(value)
      return self if _append_int16(value)

      raise_appender_error('failed to append_int16')
    end

    # call-seq:
    #   appender.append_int32(val) -> self
    #
    # Appends an int32(INTEGER) value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age INTEGER)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_int32(20)
    #     .end_row
    #     .flush
    def append_int32(value)
      return self if _append_int32(value)

      raise_appender_error('failed to append_int32')
    end

    # call-seq:
    #   appender.append_int64(val) -> self
    #
    # Appends an int64(BIGINT) value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age BIGINT)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_int64(20)
    #     .end_row
    #     .flush
    def append_int64(value)
      return self if _append_int64(value)

      raise_appender_error('failed to append_int64')
    end

    # call-seq:
    #   appender.append_uint8(val) -> self
    #
    # Appends an uint8 value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age UTINYINT)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_uint8(20)
    #     .end_row
    #     .flush
    def append_uint8(value)
      return self if _append_uint8(value)

      raise_appender_error('failed to append_uint8')
    end

    # call-seq:
    #   appender.append_uint16(val) -> self
    #
    # Appends an uint16 value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age USMALLINT)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_uint16(20)
    #     .end_row
    #     .flush
    def append_uint16(value)
      return self if _append_uint16(value)

      raise_appender_error('failed to append_uint16')
    end

    # call-seq:
    #   appender.append_uint32(val) -> self
    #
    # Appends an uint32 value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age UINTEGER)')
    #   appender = con.appender('users')
    #   appender
    #     .append_int32(1)
    #     .append_uint32(20)
    #     .end_row
    #     .flush
    def append_uint32(value)
      return self if _append_uint32(value)

      raise_appender_error('failed to append_uint32')
    end

    # call-seq:
    #   appender.append_uint64(val) -> self
    #
    # Appends an uint64 value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, age UBIGINT)')
    #   appender = con.appender('users')
    #   Appender
    #     .append_int32(1)
    #     .append_uint64(20)
    #     .end_row
    #     .flush
    def append_uint64(value)
      return self if _append_uint64(value)

      raise_appender_error('failed to append_uint64')
    end

    # call-seq:
    #   appender.append_float(val) -> self
    #
    # Appends a float value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE numbers (num FLOAT)')
    #   appender = con.appender('numbers')
    #   appender
    #     .append_float(1.23)
    #     .end_row
    #     .flush
    def append_float(value)
      return self if _append_float(value)

      raise_appender_error('failed to append_float')
    end

    # call-seq:
    #   appender.append_double(val) -> self
    #
    # Appends a double value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE numbers (num DOUBLE)')
    #   appender = con.appender('numbers')
    #   appender
    #     .append_double(1.23)
    #     .end_row
    #     .flush
    def append_double(value)
      return self if _append_double(value)

      raise_appender_error('failed to append_double')
    end

    # call-seq:
    #   appender.append_varchar(val) -> self
    #
    # Appends a varchar value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE names (name VARCHAR)')
    #   appender = con.appender('names')
    #   appender
    #     .append_varchar('Alice')
    #     .end_row
    #     .flush
    def append_varchar(value)
      return self if _append_varchar(value)

      raise_appender_error('failed to append_varchar')
    end

    # call-seq:
    #   appender.append_varchar_length(val, len) -> self
    #
    # Appends a varchar value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE names (name VARCHAR)')
    #   appender = con.appender('names')
    #   appender
    #     .append_varchar_length('Alice', 5)
    #     .end_row
    #     .flush
    def append_varchar_length(value, length)
      return self if _append_varchar_length(value, length)

      raise_appender_error('failed to append_varchar_length')
    end

    # call-seq:
    #   appender.append_blob(val) -> self
    #
    # Appends a varchar value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE values (value BLOB)')
    #   appender = con.appender('values')
    #   appender
    #     .append('\0\1\2\3\4\5'.encode(Encoding::BINARY))
    #     .end_row
    #     .flush
    def append_blob(value)
      return self if _append_blob(value)

      raise_appender_error('failed to append_blob')
    end

    # call-seq:
    #   appender.append_null -> self
    #
    # Appends a NULL value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE values (value INTEGER)')
    #   appender = con.appender('values')
    #   appender
    #     .append_null
    #     .end_row
    #     .flush
    def append_null
      return self if _append_null

      raise_appender_error('failed to append_null')
    end

    # call-seq:
    #   appender.append_default -> self
    #
    # Appends a default value to the current row in the appender.
    # If the column does not have a default value, this method
    # appends a NULL value.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE values (value INTEGER DEFAULT 1)')
    #   appender = con.appender('values')
    #   appender
    #     .append_default
    #     .end_row
    #     .flush
    def append_default
      return self if _append_default

      raise_appender_error('failed to append_default')
    end

    # call-seq:
    #   appender.append_hugeint(val) -> self
    #
    # Appends a huge int value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE numbers (num HUGEINT)')
    #   appender = con.appender('numbers')
    #   appender
    #     .append_hugeint(-170_141_183_460_469_231_731_687_303_715_884_105_727)
    #     .end_row
    #     .flush
    def append_hugeint(value)
      lower, upper = integer_to_hugeint(value)

      return self if _append_hugeint(lower, upper)

      raise_appender_error('failed to append_hugeint')
    end

    # call-seq:
    #   appender.append_uhugeint(val) -> self
    #
    # Appends an unsigned huge int value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE numbers (num UHUGEINT)')
    #   appender = con.appender('numbers')
    #   appender
    #     .append_hugeint(340_282_366_920_938_463_463_374_607_431_768_211_455)
    #     .end_row
    #     .flush
    def append_uhugeint(value)
      lower, upper = integer_to_hugeint(value)

      return self if _append_uhugeint(lower, upper)

      raise_appender_error('failed to append_uhugeint')
    end

    # call-seq:
    #   appender.append_date(val) -> self
    #
    # Appends a date value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE dates (date_value DATE)')
    #   appender = con.appender('dates')
    #   appender.append_date(Date.today)
    #   # or
    #   # appender.append_date(Time.now)
    #   # appender.append_date('2021-10-10')
    #   appender.end_row
    #   appender.flush
    def append_date(value)
      date = _parse_date(value)

      return self if _append_date(date.year, date.month, date.day)

      raise_appender_error('failed to append_date')
    end

    # call-seq:
    #   appender.append_time(val) -> self
    #
    # Appends a time value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE times (time_value TIME)')
    #   appender = con.appender('times')
    #   appender.append_time(Time.now)
    #   # or
    #   # appender.append_time('01:01:01')
    #   appender.end_row
    #   appender.flush
    def append_time(value)
      time = _parse_time(value)

      return self if _append_time(time.hour, time.min, time.sec, time.usec)

      raise_appender_error('failed to append_time')
    end

    # call-seq:
    #   appender.append_timestamp(val) -> self
    #
    # Appends a timestamp value to the current row in the appender.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE timestamps (timestamp_value TIMESTAMP)')
    #   appender = con.appender('timestamps')
    #   appender.append_time(Time.now)
    #   # or
    #   # appender.append_time(Date.today)
    #   # appender.append_time('2021-08-01 01:01:01')
    #   appender.end_row
    #   appender.flush
    def append_timestamp(value)
      time = to_time(value)

      return self if _append_timestamp(time.year, time.month, time.day, time.hour, time.min, time.sec, time.nsec / 1000)

      raise_appender_error('failed to append_timestamp')
    end

    # call-seq:
    #   appender.append_interval(val) -> self
    #
    # Appends an interval value to the current row in the appender.
    # The argument must be ISO8601 duration format.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE intervals (interval_value INTERVAL)')
    #   appender = con.appender('intervals')
    #   appender
    #     .append_interval('P1Y2D') # => append 1 year 2 days interval.
    #     .end_row
    #     .flush
    def append_interval(value)
      value = Interval.to_interval(value)

      return self if _append_interval(value.interval_months, value.interval_days, value.interval_micros)

      raise_appender_error('failed to append_interval')
    end

    # appends value.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
    #   appender = con.appender('users')
    #   appender.append(1)
    #   appender.append('Alice')
    #   appender.end_row
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
        blob?(value) ? append_blob(value) : append_varchar(value)
      when TrueClass, FalseClass
        append_bool(value)
      when Time
        append_timestamp(value)
      when Date
        append_date(value)
      when DuckDB::Interval
        append_interval(value)
      else
        raise(DuckDB::Error, "not supported type #{value} (#{value.class})")
      end
    end

    # append a row.
    #
    #   appender.append_row(1, 'Alice')
    #
    # is same as:
    #
    #   appender.append(2)
    #   appender.append('Alice')
    #   appender.end_row
    def append_row(*args)
      args.each do |arg|
        append(arg)
      end
      end_row
    end

    private

    def raise_appender_error(default_message) # :nodoc:
      message = error_message
      raise DuckDB::Error, message || default_message
    end

    def blob?(value) # :nodoc:
      value.instance_of?(DuckDB::Blob) || value.encoding == Encoding::BINARY
    end

    def to_time(value) # :nodoc:
      case value
      when Date
        value.to_time
      else
        _parse_time(value)
      end
    end
  end
end
