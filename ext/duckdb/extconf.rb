# frozen_string_literal: true

require 'mkmf'

DUCKDB_REQUIRED_VERSION = '0.8.0'

def check_duckdb_header(header, version)
  found = find_header(
    header,
    '/opt/homebrew/include',
    '/opt/homebrew/opt/duckdb/include',
    '/opt/local/include'
  )
  return if found

  msg = "#{header} is not found. Install #{header} of duckdb >= #{version}."
  print_message(msg)
  raise msg
end

def check_duckdb_library(library, func, version)
  found = find_library(
    library,
    func,
    '/opt/homebrew/lib',
    '/opt/homebrew/opt/duckdb/lib',
    '/opt/local/lib'
  )
  return if found

  library_name = duckdb_library_name(library)
  msg = "#{library_name} is not found. Install #{library_name} of duckdb >= #{version}."
  print_message(msg)
  raise msg
end

def duckdb_library_name(library)
  "lib#{library}.(so|dylib|dll)"
end

def print_message(msg)
  print <<~END_OF_MESSAGE

    #{'*' * 80}
    #{msg}
    #{'*' * 80}

  END_OF_MESSAGE
end

dir_config('duckdb')

check_duckdb_header('duckdb.h', DUCKDB_REQUIRED_VERSION)

# check duckdb >= 0.8.0
check_duckdb_library('duckdb', 'duckdb_string_is_inlined', DUCKDB_REQUIRED_VERSION)

# check duckdb >= 0.9.0
have_func('duckdb_bind_parameter_index', 'duckdb.h')

# duckdb_parameter_name is not found on Windows.
have_func('duckdb_parameter_name', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
