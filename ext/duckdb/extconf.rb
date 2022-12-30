require 'mkmf'

def duckdb_library_available?(func)
  header = find_header('duckdb.h') || find_header('duckdb.h', '/opt/homebrew/include')
  library = have_func('duckdb', func) || find_library('duckdb', func, '/opt/homebrew/opt/duckdb/lib')
  header && library
end

def check_duckdb_library(func, version)
  return if duckdb_library_available?(func)

  msg = "duckdb >= #{version} is not found. Install duckdb >= #{version} library and header file."
  puts ''
  puts '*' * 80
  puts msg
  puts '*' * 80
  puts ''
  raise msg
end

dir_config('duckdb')

check_duckdb_library('duckdb_pending_prepared', '0.5.0')

# check duckdb >= 0.3.3
# ducdb >= 0.3.3 if duckdb_append_data_chunk() is defined.
have_func('duckdb_append_data_chunk', 'duckdb.h')

# check duckdb >= 0.6.0
have_func('duckdb_value_string', 'duckdb.h')

have_func('duckdb_free', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
