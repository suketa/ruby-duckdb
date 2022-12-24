require 'mkmf'

def have_duckdb_library(func)
  header = find_header('duckdb.h') || find_header('duckdb.h', '/opt/homebrew/include')
  library = have_func('duckdb', func) || find_library('duckdb', func, '/opt/homebrew/opt/duckdb/lib')
  header && library
end

dir_config('duckdb')

unless have_duckdb_library('duckdb_value_is_null')
  msg = 'duckdb >= 0.2.9 is not found. Install duckdb >= 0.2.9 library file and header file.'
  puts ''
  puts msg
  puts ''
  raise msg
end

# check duckdb >= 0.3.3
# ducdb >= 0.3.3 if duckdb_append_data_chunk() is defined.
have_func('duckdb_append_data_chunk', 'duckdb.h')

# check duckdb >= 0.6.0
have_func('duckdb_value_string', 'duckdb.h')

have_func('duckdb_free', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
