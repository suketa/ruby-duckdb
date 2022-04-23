require 'mkmf'

dir_config('duckdb')

raise 'duckdb library is not found. Install duckdb library file and header file.' unless have_library('duckdb')

raise 'duckdb >= 0.2.9 is required. Install duckdb >= 0.2.9' unless have_func('duckdb_value_is_null', 'duckdb.h')

# ducdb >= 0.3.3 if duckdb_append_data_chunk() is defined.
have_func('duckdb_append_data_chunk', 'duckdb.h')

have_func('duckdb_free', 'duckdb.h')

have_func('duckdb_create_config', 'duckdb.h')
have_func('duckdb_open_ext', 'duckdb.h')
have_func('duckdb_prepare_error', 'duckdb.h')

have_func('duckdb_append_date', 'duckdb.h')
have_func('duckdb_append_interval', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
