# frozen_string_literal: true

require 'bigdecimal'

module DuckDB
  # The Result class encapsulates a execute result of DuckDB database.
  #
  # The usage is as follows:
  #
  #   require 'duckdb'
  #
  #   db = DuckDB::Database.open # database in memory
  #   con = db.connect
  #
  #   con.execute('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
  #
  #   con.execute("INSERT into users VALUES(1, 'Alice')")
  #   con.execute("INSERT into users VALUES(2, 'Bob')")
  #   con.execute("INSERT into users VALUES(3, 'Cathy')")
  #
  #   result = con.execute('SELECT * from users')
  #   result.each do |row|
  #     p row
  #   end
  class Result
    include Enumerable

    # typedef enum DUCKDB_TYPE {
    #   DUCKDB_TYPE_INVALID = 0,
    #   DUCKDB_TYPE_BOOLEAN = 1,
    #   DUCKDB_TYPE_TINYINT = 2,
    #   DUCKDB_TYPE_SMALLINT = 3,
    #   DUCKDB_TYPE_INTEGER = 4,
    #   DUCKDB_TYPE_BIGINT = 5,
    #   DUCKDB_TYPE_UTINYINT = 6,
    #   DUCKDB_TYPE_USMALLINT = 7,
    #   DUCKDB_TYPE_UINTEGER = 8,
    #   DUCKDB_TYPE_UBIGINT = 9,
    #   DUCKDB_TYPE_FLOAT = 10,
    #   DUCKDB_TYPE_DOUBLE = 11,
    #   DUCKDB_TYPE_TIMESTAMP = 12,
    #   DUCKDB_TYPE_DATE = 13,
    #   DUCKDB_TYPE_TIME = 14,
    #   DUCKDB_TYPE_INTERVAL = 15,
    #   DUCKDB_TYPE_HUGEINT = 16,
    #   DUCKDB_TYPE_VARCHAR = 17,
    #   DUCKDB_TYPE_BLOB = 18,
    #   DUCKDB_TYPE_DECIMAL = 19,
    #   DUCKDB_TYPE_TIMESTAMP_SEC = 20,
    #   DUCKDB_TYPE_TIMESTAMP_MS = 21,
    #   DUCKDB_TYPE_TIMESTAMP_NS = 22,
    #   DUCKDB_TYPE_ENUM = 23,
    #   DUCKDB_TYPE_LIST = 24,
    #   DUCKDB_TYPE_STRUCT = 25,
    #   DUCKDB_TYPE_MAP = 26,
    #   DUCKDB_TYPE_UUID = 27,
    #   DUCKDB_TYPE_UNION = 28,
    #   DUCKDB_TYPE_BIT = 29,
  end
end
