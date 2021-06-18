require 'mkmf'

dir_config('duckdb')
if have_library('duckdb')
  have_func('duckdb_value_blob', 'duckdb.h')
  have_func('duckdb_bind_blob', 'duckdb.h')
  have_func('duckdb_appender_create', 'duckdb.h')
  have_func('duckdb_free', 'duckdb.h')
  create_makefile('duckdb/duckdb_native')
end
