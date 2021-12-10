#ifndef RUBY_DUCKDB_UTIL_H
#define RUBY_DUCKDB_UTIL_H

#ifdef HAVE_DUCKDB_APPEND_DATE

duckdb_date to_duckdb_date_from_value(VALUE year, VALUE month, VALUE day);

#endif

#endif
