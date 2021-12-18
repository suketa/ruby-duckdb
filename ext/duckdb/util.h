#ifndef RUBY_DUCKDB_UTIL_H
#define RUBY_DUCKDB_UTIL_H

#ifdef HAVE_DUCKDB_APPEND_DATE

duckdb_date to_duckdb_date_from_value(VALUE year, VALUE month, VALUE day);
duckdb_time to_duckdb_time_from_value(VALUE hour, VALUE min, VALUE sec, VALUE micros);
duckdb_timestamp to_duckdb_timestamp_from_value(VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros);

#endif

#endif
