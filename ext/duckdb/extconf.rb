require 'mkmf'

def duckdb_library_available?(func)
  header = find_header('duckdb.h') || find_header('duckdb.h', '/opt/homebrew/include')
  library = have_func(func, 'duckdb.h') || find_library('duckdb', func, '/opt/homebrew/opt/duckdb/lib')
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

CONFIG['optflags'] = "-O0"
CONFIG['debugflags'] = "-ggdb3"

dir_config('duckdb')

check_duckdb_library('duckdb_pending_prepared', '0.5.0')

# check duckdb >= 0.6.0
have_func('duckdb_value_string', 'duckdb.h')

# check duckdb >= 0.7.0
have_func('duckdb_extract_statements', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
