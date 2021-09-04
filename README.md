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
Use `DuckDB::Blob.new` or use sting#force_encoding(Encoding::BINARY)

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

### Appender

Appender class provides Ruby interface of [DuckDB Appender](https://duckdb.org/docs/data/appender)

```
require 'duckdb'
require 'benchmark'

def insert
  DuckDB::Database.open do |db|
    db.connect do |con|
      con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
      10000.times do
        con.query("INSERT into users VALUES(1, 'Alice')")
      end
    end
  end
end

def prepare
  DuckDB::Database.open do |db|
    db.connect do |con|
      con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
      stmt = con.prepared_statement('INSERT INTO users VALUES($1, $2)')
      10000.times do
        stmt.bind(1, 1)
        stmt.bind(2, 'Alice')
        stmt.execute
      end
    end
  end
end

def append
  DuckDB::Database.open do |db|
    db.connect do |con|
      con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
      appender = con.appender('users')
      10000.times do
        appender.begin_row
        appender.append(1)
        appender.append('Alice')
        appender.end_row
      end
      appender.flush
    end
  end
end

Benchmark.bm(8) do |x|
  x.report('insert') { insert }
  x.report('prepare') { prepare }
  x.report('append') { append }
end

# =>
#                user     system      total        real
# insert     0.637439   0.000000   0.637439 (  0.637486 )
# prepare    0.230457   0.000000   0.230457 (  0.230460 )
# append     0.012666   0.000000   0.012666 (  0.012670 )
```

### Configuration

Config class provides Ruby interface of [DuckDB configuration](https://duckdb.org/docs/api/c/config).

```
require 'duckdb'
config = DuckDB::Config.new
config['default_order'] = 'DESC'
db = DuckDB::Database.open(nil, config)
con = db.connect
con.query('CREATE TABLE numbers (number INTEGER)')
con.query('INSERT INTO numbers VALUES (2), (1), (4), (3)')

# number is ordered by descending.
r = con.query('SELECT number FROM numbers ORDER BY number')
r.first.first # => 4
```
