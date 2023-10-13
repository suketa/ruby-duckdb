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

dir_config('duckdb')

check_duckdb_library('duckdb_extract_statements', '0.7.0')

# check duckdb >= 0.7.0
have_func('duckdb_extract_statements', 'duckdb.h')

# check duckdb >= 0.8.0
have_func('duckdb_string_is_inlined', 'duckdb.h')

# check duckdb >= 0.9.0
have_func('duckdb_bind_parameter_index', 'duckdb.h')

have_func('duckdb_parameter_name', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
