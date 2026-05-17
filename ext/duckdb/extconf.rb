# frozen_string_literal: true

require 'mkmf'

DUCKDB_REQUIRED_VERSION = '1.4.0'

def brew_prefix(formula = nil)
  cmd = formula ? "brew --prefix #{formula} 2>/dev/null" : 'brew --prefix 2>/dev/null'
  prefix = `#{cmd}`.chomp
  prefix.empty? ? nil : prefix
end

FALLBACK_PREFIXES = %w[/opt/homebrew /opt/homebrew/opt/duckdb /opt/local].freeze

def brew_dirs(subdir)
  dirs = []
  dirs << "#{brew_prefix('duckdb')}/#{subdir}" if brew_prefix('duckdb')
  if (prefix = brew_prefix)
    dirs << "#{prefix}/#{subdir}"
    dirs << "#{prefix}/opt/duckdb/#{subdir}"
  end
  dirs
end

def homebrew_include_dirs
  (brew_dirs('include') + FALLBACK_PREFIXES.map { |p| "#{p}/include" }).uniq
end

def homebrew_lib_dirs
  (brew_dirs('lib') + FALLBACK_PREFIXES.map { |p| "#{p}/lib" }).uniq
end

def check_duckdb_header(header, version)
  found = find_header(header, *homebrew_include_dirs)
  return if found

  msg = "#{header} is not found. Install #{header} of duckdb >= #{version}."
  print_message(msg)
  raise msg
end

def check_duckdb_library(library, func, version)
  found = find_library(library, func, *homebrew_lib_dirs)
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
check_duckdb_library('duckdb', 'duckdb_appender_create_query', DUCKDB_REQUIRED_VERSION)

# check duckdb >= 1.5.0
have_func('duckdb_unsafe_vector_assign_string_element_len', 'duckdb.h')

# check duckdb >= 1.5.2
have_func('duckdb_geometry_type_get_crs', 'duckdb.h')

create_makefile('duckdb/duckdb_native')
