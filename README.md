# ruby-duckdb

[![Ubuntu](https://github.com/suketa/ruby-duckdb/workflows/Ubuntu/badge.svg)](https://github.com/suketa/ruby-duckdb/actions?query=workflow%3AUbuntu)
[![MacOS](https://github.com/suketa/ruby-duckdb/workflows/MacOS/badge.svg)](https://github.com/suketa/ruby-duckdb/actions?query=workflow%3AMacOS)
[![Windows](https://github.com/suketa/ruby-duckdb/workflows/Windows/badge.svg)](https://github.com/suketa/ruby-duckdb/actions?query=workflow%3AWindows)
[![Gem Version](https://badge.fury.io/rb/duckdb.svg)](https://badge.fury.io/rb/duckdb)

## Description

ruby-duckdb is Ruby binding for [DuckDB](http://www.duckdb.org) database engine

## Requirement

You must have [DuckDB](http://www.duckdb.org) engine installed in order to build/use this module.

## Pre-requisite setup (Linux):
1. Head over to the [DuckDB](https://duckdb.org/) webpage

2. Download the latest C++ package release for DuckDB

3. Move the files to their respective location:
    - Extract the `duckdb.h` and `duckdb.hpp` file to `/usr/local/include`
    - Extract the `libduckdb.so` file to `/usr/local/lib`
    
    ```sh
    unzip libduckdb-linux-amd64.zip -d libduckdb
    sudo mv libduckdb/duckdb.* /usr/local/include/
    sudo mv libduckdb/libduckdb.so /usr/local/lib
    ```
4. To create the necessary link, run `ldconfig` as root:
  
    ```sh
    sudo ldconfig /usr/local/lib # adding a --verbose flag is optional - but this will let you know if the libduckdb.so library has been linked
    ```
## Pre-requisite setup (MacOS):

Using `brew install` is recommended.

```sh
brew install duckdb
```

## How to Install

```
gem install duckdb
```
> this will work fine with the above pre-requisite setup.

or you must specify the location of the C header and library files:

```
gem install duckdb -- --with-duckdb-include=/duckdb_header_directory --with-duckdb-lib=/duckdb_library_directory
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
