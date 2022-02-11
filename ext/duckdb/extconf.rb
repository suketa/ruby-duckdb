require 'mkmf'

dir_config('duckdb')

raise 'duckdb library is not found. Install duckdb library file and header file.' unless have_library('duckdb')

raise 'duckdb >= 0.2.9 is required. Install duckdb >= 0.2.9' unless have_func('duckdb_value_is_null', 'duckdb.h')

if have_func('duckdb_nparams(NULL)', 'duckdb.h')
  $defs << '-DHAVE_DUCKDB_NPARAMS_029'
elsif have_func('duckdb_nparams(NULL, NULL)', 'duckdb.h')
  $defs << '-DHAVE_DUCKDB_NPARAMS_028'
end

have_func('duckdb_appender_create', 'duckdb.h')
have_func('duckdb_free', 'duckdb.h')

have_func('duckdb_create_config', 'duckdb.h')
have_func('duckdb_open_ext', 'duckdb.h')
have_func('duckdb_prepare_error', 'duckdb.h')

have_func('duckdb_append_date', 'duckdb.h')
have_func('duckdb_append_interval', 'duckdb.h')
have_func('duckdb_append_time', 'duckdb.h')
have_func('duckdb_append_timestamp', 'duckdb.h')
have_func('duckdb_append_hugeint', 'duckdb.h')

have_func('duckdb_bind_date', 'duckdb.h')
have_func('duckdb_bind_time', 'duckdb.h')
have_func('duckdb_bind_timestamp', 'duckdb.h')
have_func('duckdb_bind_interval', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
