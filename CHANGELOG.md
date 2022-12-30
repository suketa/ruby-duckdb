# ChangeLog

# 0.6.1
- bump Ruby to 3.2.0 on CI
- fix deprected warning (double_heap is deprecated in GC.verify_compaction_references) with Ruby 3.2.0 on CI
- bump duckdb to 0.6.1 on CI
- add DuckDB::PreparedStatement#bind_hugeint_internal
- fix gem install error on M1 MacOS
- implement DuckDB.library_version
- use duckdb_value_string instead of duckdb_value_varchar if duckdb_value_string is available.
- bump Ruby to 3.2.0rc1
- bump duckdb to 0.6.0

## Breaking Change
- drop duckdb <= 0.4.x. ruby-duckdb supports duckdb >= 0.5.0

# 0.5.1.1
- bug fix: reading the boolean column

# 0.5.1
- bump duckdb to 0.5.1

# 0.5.0
- update bundle version of Gemfile.lock
- add ruby ucrt test on Windows
- use TypedData_Wrap_Struct, TypedData_Get_Struct
- bump duckdb to 0.5.0
- fix utf-8 encoding.
- add DuckDB::Result#enum_dictionary_values
- add DuckDB::Result#row_count, DuckDB::Result#row_size(alias of row_count).
- add DuckDB::Result#column_count, DuckDB::Result#column_size(alias of column_count).

## Breaking Change
- bind_varchar does not raised DuckDB::Error when the binding column is date or datetime.

# 0.3.4.0
- bump duckdb to 0.3.4

# 0.3.3.0
- DuckDB::Column#type supports :decimal.
- bump duckdb to 0.3.3.
- bump Ruby to 2.6.10, 2.7.6, 3.0.4, 3.1.2.

# 0.3.2.0

- bind_time, bind_timestamp, bind_date, bind_timeinterval to DuckDB::PreparedStatement
- bump duckdb 0.3.2
- bump Ruby to 3.1.1, add Ruby mingw in CI.
- bump Ruby to 2.6.9, 2.7.5, 3.0.3 in CI.

## BREAKING CHANGE
- drop duckdb <= 0.2.8

# 0.3.1.0

- bump duckdb to 0.3.1 in CI.
- bump duckdb to 0.3.0 in CI.
- append_time, append_date, append_timestamp of DuckDB::Appender accept argument
  having to_str to convert time string.

# 0.2.9.0

- add DuckDB::Appender#append
  - breaking change.
    - append_timestamp is called when the argument is Time object.
    - append_date is called when the argument is Date object.
- add DuckDB::Appender#append_timestamp.
- add DuckDB::Appender#append_interval. append_interval is expremental.
- add DuckDB::Result#rows_changed
- refactoring DuckDB::Append#append_hugeint with duckdb v0.2.9
- test 2 versions of duckdb on github actions macos CI.
- fix windows CI failes
- update github actions CI on ubuntu
- fix to build with duckdb v0.2.9
- use duckdb_prepare_error when get error message of prepared statement.
- add DuckDB::Appender#append_date
- add DuckDB::Appender#append_time

# 0.2.8.0

- DuckDB::Database.open accepts 2-nd argument as DuckDB::Config object.
- add DuckDB::Config
- bump duckdb to 0.2.8 in CI
- bump Ruby to 2.6.8, 2.7.4, 3.0.2 in CI

# 0.2.7.0

- call duckdb_free after calling duckdb_value_blob, duckdb_value_varchar.
- bump DuckDB to v0.2.7 in CI
- rake build on Windows in github actions.
  - There is a issue (LoadError) when running rake test on Windows (in GitHub actions).

# 0.2.6.1

- add DuckDB::PreparedStatement#bind_int8
- DuckDB::Connection#appender accepts block.
- add DuckDB::Appender#append_row.
- support HUGEINT type.
- add DuckDB::Appender#append.
- rename PreparedStatement#bind_boolean to PreparedStatement#bind_bool.
- add DuckDB::Connection#appender.

# 0.2.6.0

- change version policy
  - ruby-duckdb W.X.Y.Z supports duckdb W.X.Y
- add DuckDB::Appender
- bump DuckDB to v0.2.6 in CI.
- remove unnecessary duckdb header file from MacOS CI.
- add DuckDB::Connection#prepared_statement.

## 0.0.12

- bump DuckDB to v0.2.5
- support BLOB type (with DuckDB version 0.2.5 or later)

## 0.0.11

- fix failure in test_close in test/duckdb_test/database_test.rb because DuckDB error message was changed.
- bump DuckDb to v0.2.4
- add test CI with Ruby 3.0.0
- add test CI on MacOS.
- bunp DuckDB to v0.2.3

## 0.0.10

- bump DuckDB to v0.2.2
- fix to build failure on MacOS.

## 0.0.9

- bump DuckDB to v0.2.1
- bump Ruby to v2.7.2
- bunmp DuckDB to v0.2.0

## 0.0.8.1

- update Gemfile.lock
- unsupport Ruby 2.4

## 0.0.8

- remove test with Ruby 2.4.10
- bump DuckDB to v0.1.8
- bump DuckDB to v0.1.8
- bump DuckDB to v0.1.7
  - current ruby-duckdb supports DuckDB version 0.1.5 and 0.1.6
- support Ruby 2.7.1
- bump DuckDB to v0.1.6
  - current ruby-duckdb supports DuckDB version 0.1.5 and 0.1.6
- DuckDB::Connection#connect accepts block
- add DuckDB::Connection#connect
- DuckDB::Database#connect accepts block
- DuckDB::Database.open accepts block
- update duckdb.gemspec, required ruby version >= 2.4.0

## 0.0.7

- bump DuckDB to v0.1.5
  - DuckDB version must be 0.1.5 or later.
- add DuckDB::Connection#connect, alias method open
- add DuckDB::Connection#disconnect, alias method close
- add DuckDB::Database#close

## 0.0.6

- add alias `execute` of `DuckDB::Connection#query`
- support `duckdb version 0.1.3`
- add `DuckDB:PreparedStatement`
- create CI (GitHub Actions / Travis-CI)
- create database only once in result_test.rb

## 0.0.5

- add `DuckDB::Error`
- `DuckDB::Result#each` convert DuckDB number value to Ruby's number
- `DuckDB::Result#each` convert DuckDB NULL value to nil
- `DuckDB::Result#each` returns Enumerator object when block is not given
- `DuckDB::Result` include `Enumerable`
- add test for `DuckDB::Result`
- add test for `DuckDB::Connection`
- fix description in duckdb.gemspec

## 0.0.4

- add test for `DuckDB::Database`
- rename module name to `DuckDB` from `Duckdb`

## 0.0.3

- rename native extension name to duckdb_native

## 0.0.2

- fix gem install error

## 0.0.1

- first release
