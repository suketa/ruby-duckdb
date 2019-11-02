# ruby-duckdb


## Description

ruby-duckdb is Ruby binding for [DuckDB](http://www.duckdb.org)

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
