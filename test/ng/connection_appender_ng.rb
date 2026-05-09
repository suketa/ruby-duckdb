# frozen_string_literal: true

require 'duckdb'

db = DuckDB::Database.new
con = db.connect
con.execute("create table 'a.b' (i integer)")
appender = DuckDB::Appender.new(con, nil, 'a.b')
appender.append_row(1)
appender.flush
result = con.execute("select * from 'a.b'").to_a
p result

db = DuckDB::Database.new
con = db.connect
con.execute("create table 'a.b' (i integer)")
appender = con.appender('a.b') # => bug. DuckDB::Error
appender.append_row(1)
appender.flush
result = con.execute("select * from 'a.b'").to_a
p result
