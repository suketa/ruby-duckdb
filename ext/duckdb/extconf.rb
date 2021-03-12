require 'mkmf'

dir_config('duckdb')
if have_library('duckdb')
  have_func('duckdb_value_blob', 'duckdb.h')
  create_makefile('duckdb/duckdb_native')
end
