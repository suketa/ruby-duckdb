# ChangeLog

- bump Ruby to 3.3.0 on CI.

## Breaking changes
- drop Ruby 2.7.

# 0.9.2.1
- support Time column in `DuckDB#Result#chunk_each`.
- add `DuckDB::Interval#eql?`.

# 0.9.2
- add `DuckDB::Connection#async_query_stream`.
- `DuckDB::PendingResult` accepts second argument. If the second argument is
  true, `PendingResult#execute_pending` returns streaming `DuckDB::Result` object.
- add `DuckDB::PreparedStatement#pending_prepared_stream`
- add `DuckDB::Result#streaming?`.

# 0.9.1.2
- add `DuckDB::Connection#interrupt`, `DuckDB::Connection#query_progress`.
- add `DuckDB::Connection#async_query`, alias method `async_execute`.

# 0.9.1.1
- change default branch to main from master.
- add `DuckDB::PendingResult` class.
- add `DuckDB::PendingResult#state`.
- add `DuckDB::PendingResult#execute_task`.
- add `DuckDB::PendingResult#execute_pending`.
- add `DuckDB::PreparedStatement#pending_prepared`.

## Breaking Changes
- drop duckdb v0.7.x.

# 0.9.1
- add `DuckDB::PreparedStatement#parameter_name`.
- bump duckdb to 0.9.1.

# 0.9.0.1
- add `DuckDB::PreparedStatement#bind_parameter_index`.
- `DuckDB::Connection#query` accepts SQL with named bind parameters.

# 0.9.0
- bump duckdb to 0.9.0.

## Breaking Changes
- deprecation warning when `DuckDB::Result.each` calling with `DuckDB::Result.use_chunk_each` is false.
  The `each` behavior will be same as `DuckDB::Result.chunk_each` in the future.
  set `DuckDB::Result.use_chunk_each = true` to suppress the warning.
- `DuckDB::Result#chunk_each` returns `DuckDB::Interval` class when the column type is INTERVAL.

# 0.8.1.3
- Fix BigDecimal conversion.

# 0.8.1.2
- Fix BigDecimal conversion when the value is 0.
  Thanks to shreeve.

# 0.8.1.1
- DuckDB::Result#chunk_each supports:
  - UTINYINT
  - USMALLINT
  - UINTEGER
  - UBIGINT
- fix memory leak of:
  - `DuckDB::Result#_enum_dictionary_value`
  - `DuckDB::Result#_enum_dictionary_size`
  - `DuckDB::Result#_enum_internal_type`

# 0.8.1
- bump duckdb to 0.8.1.
- add `DuckDB::Result#chunk_each`, `DuckDB::Result.use_chunk_each=`, `DuckDB::Result#use_chunk_each?`
  The current behavior of `DuckDB::Result#each` is same as older version.
  But `DuckDB::Result#each` behavior will be changed like as `DuckDB::Result#chunk_each` in near future release.
  And there are some breaking changes.
  Write `DuckdDB::Result.use_chunk_each = true` if you want to try new behavior.
    ```ruby
    DuckDB::Result.use_chunk_each = true

    result = con.query('SELECT ....')
    result.each do |record| # <= each method behavior is same as DuckDB::Result#chunk_each
      ...
    end
    ```
  Thanks to @stephenprater.
- support enum type in DuckDB::Result#chunk_each.
- support uuid type in DuckDB::Result#chunk_each.

## Breaking Changes

- DuckDB::Config.set_config does not raise exception when invalid key specified.
  Instead, DuckDB::Database.open raises DuckDB::Error with invalid key configuration.

# 0.8.0
- bump duckdb to 0.8.0
- add DuckDB::Result#_to_decimal_internal
- add DuckDB::Result#_to_hugeint_internal

## Breaking Changes
- DuckDB::Result returns BigDecimal object instead of String object if the column type is DECIMAL.

# 0.7.1
- bump duckdb to 0.7.1
- fix docker build error on M1 Mac

# 0.7.0
- bump duckdb to 0.7.0
- fix have_func argument order
- remove unused variable in test
- add DuckDB::LIBRARY_VERSION
- add DuckDB::Result#_to_string_internal
- add DuckDB::Result#__to_hugeint_internal
- add DuckDB::Result#__to_decimal_internal
- add Ruby 3.2.1 on CI test
- add Ruby mswin on CI test
## Breaking Changes
- drop Ruby 2.6

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

## Breaking Changes
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

## Breaking Changes
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
