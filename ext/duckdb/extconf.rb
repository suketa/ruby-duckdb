require 'mkmf'

dir_config('duckdb')
have_library('duckdb')
create_makefile('duckdb/duckdb_native')
