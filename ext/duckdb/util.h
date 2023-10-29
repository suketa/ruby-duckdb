#ifndef RUBY_DUCKDB_UTIL_H
#define RUBY_DUCKDB_UTIL_H

duckdb_date rbduckdb_to_duckdb_date_from_value(VALUE year, VALUE month, VALUE day);
duckdb_time rbduckdb_to_duckdb_time_from_value(VALUE hour, VALUE min, VALUE sec, VALUE micros);
duckdb_timestamp rbduckdb_to_duckdb_timestamp_from_value(VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros);
void rbduckdb_to_duckdb_interval_from_value(duckdb_interval* interval, VALUE months, VALUE days, VALUE micros);

#endif
