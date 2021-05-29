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
        respond_to?(:bind_null) ? bind_null(i) : rb_raise(DuckDB::Error, 'This bind method does not support nil value. Re-compile ruby-duckdb with DuckDB version >= 0.1.1')
      when Float
        bind_double(i, value)
      when Integer
        bind_int64(i, value)
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
        rb_raise(DuckDB::Error, "not supported type #{value} (value.class)")
      end
    end

    private

    def blob?(value)
      value.instance_of?(DuckDB::Blob) || value.encoding == Encoding::BINARY
    end
  end
end
