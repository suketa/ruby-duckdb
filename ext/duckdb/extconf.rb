require 'mkmf'

dir_config('duckdb')

raise 'duckdb library is not found. Install duckdb library file and header file.' unless have_library('duckdb')

raise 'duckdb >= 0.2.9 is required. Install duckdb >= 0.2.9' unless have_func('duckdb_value_is_null', 'duckdb.h')

# check duckdb >= 0.3.3
# ducdb >= 0.3.3 if duckdb_append_data_chunk() is defined.
have_func('duckdb_append_data_chunk', 'duckdb.h')

# check duckdb >= 0.6.0
have_func('duckdb_value_string', 'duckdb.h')

have_func('duckdb_free', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
