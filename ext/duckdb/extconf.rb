# frozen_string_literal: true

require 'mkmf'

DUCKDB_REQUIRED_VERSION = '0.10.0'

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
  have_func(func, 'duckdb.h')
  return if found

  raise_not_found_library(library, version)
end

def raise_not_found_library(library, version)
  library_name = duckdb_library_name(library)
  msg = "#{library_name} is not found. Install #{library_name} of duckdb >= #{version}."
  print_message(msg)
  raise msg
end

def duckdb_library_name(library)
  "lib#{library}.#{RbConfig::CONFIG['DLEXT']}"
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
check_duckdb_library('duckdb', 'duckdb_appender_column_count', DUCKDB_REQUIRED_VERSION)

# check duckdb >= 0.10.0
have_func('duckdb_appender_column_count', 'duckdb.h')

# check duckdb >= 1.0.0
have_func('duckdb_fetch_chunk', 'duckdb.h')

# check duckdb >= 1.1.0
have_func('duckdb_result_error_type', 'duckdb.h')

$CFLAGS << ' -DDUCKDB_API_NO_DEPRECATED' if ENV['DUCKDB_API_NO_DEPRECATED']

create_makefile('duckdb/duckdb_native')
