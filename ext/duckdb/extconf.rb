require 'mkmf'

dir_config('duckdb')
if have_library('duckdb')
  create_makefile('duckdb/duckdb_native')
end
