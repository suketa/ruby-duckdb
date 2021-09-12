require 'mkmf'

dir_config('duckdb')
if have_library('duckdb')
  if have_func('duckdb_nparams(NULL)', 'duckdb.h')
    $defs << '-DHAVE_DUCKDB_NPARAMS_029'
  elsif have_func('duckdb_nparams(NULL, NULL)', 'duckdb.h')
    $defs << '-DHAVE_DUCKDB_NPARAMS_028'
  end

  have_func('duckdb_value_blob', 'duckdb.h')
  have_func('duckdb_bind_blob', 'duckdb.h')
  have_func('duckdb_appender_create', 'duckdb.h')
  have_func('duckdb_free', 'duckdb.h')
  have_func('duckdb_create_config', 'duckdb.h')
  have_func('duckdb_open_ext', 'duckdb.h')
  have_func('duckdb_prepare_error', 'duckdb.h')
  have_func('duckdb_append_date', 'duckdb.h')
  create_makefile('duckdb/duckdb_native')
end
