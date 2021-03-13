# ruby-duckdb

[![Build Status](https://travis-ci.com/suketa/ruby-duckdb.svg?branch=master)](https://travis-ci.com/suketa/ruby-duckdb)
[![](https://github.com/suketa/ruby-duckdb/workflows/Ubuntu/badge.svg)](https://github.com/suketa/ruby-duckdb/actions?query=workflow%3AUbuntu)
[![](https://github.com/suketa/ruby-duckdb/workflows/MacOS/badge.svg)](https://github.com/suketa/ruby-duckdb/actions?query=workflow%3AMacOS)

## Description

ruby-duckdb is Ruby binding for [DuckDB](http://www.duckdb.org) database engine

## Requirement

You must have [DuckDB](http://www.duckdb.org) engine installed in order to build/use this module.

## How to Install

```
gem install duckdb
```

or you must specify the location of the include and lib files:

```
gem install duckdb -- --with-duckdb-include=/duckdb_include_directory --with-duckdb-lib=/duckdb_library_directory
```

## Usage

```
require 'duckdb'

db = DuckDB::Database.open # database in memory
con = db.connect

con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')

con.query("INSERT into users VALUES(1, 'Alice')")
con.query("INSERT into users VALUES(2, 'Bob')")
con.query("INSERT into users VALUES(3, 'Cathy')")

result = con.query('SELECT * from users')
result.each do |row|
  p row
end
```

Or, you can use block.

```
require 'duckdb'

DuckDB::Database.open do |db|
  db.connect do |con|
    con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')

    con.query("INSERT into users VALUES(1, 'Alice')")
    con.query("INSERT into users VALUES(2, 'Bob')")
    con.query("INSERT into users VALUES(3, 'Cathy')")

    result = con.query('SELECT * from users')
    result.each do |row|
      p row
    end
  end
end
```

### using BLOB column

BLOB is available with DuckDB v0.2.5 or later.
Use `DuckDB::Blob.new` or use sting#force_encoding(Encoding::ASCII_8BIT)

```
require 'duckdb'

DuckDB::Database.open do |db|
  db.connect do |con|
    con.query('CREATE TABLE blob_table (binary_data BLOB)')
    stmt = DuckDB::PreparedStatement.new(con, 'INSERT INTO blob_table VALUES ($1)')

    stmt.bind(1, DuckDB::Blob.new("\0\1\2\3\4\5"))
    # stmt.bind(1, "\0\1\2\3\4\5".force_encoding(Encoding::BINARY))
    stmt.execute

    result = con.query('SELECT binary_data FROM blob_table')
    p result.first.first
  end
end
```
