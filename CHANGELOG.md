# ChangeLog

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
