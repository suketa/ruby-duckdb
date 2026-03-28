#ifndef RUBY_DUCKDB_CONVERTER_H
#define RUBY_DUCKDB_CONVERTER_H

extern ID id__to_date;
extern ID id__to_time;
extern ID id__to_time_from_duckdb_time;
extern ID id__to_interval_from_vector;
extern ID id__to_hugeint_from_vector;
extern ID id__to_decimal_from_hugeint;
extern ID id__to_uuid_from_vector;
extern ID id__to_time_from_duckdb_timestamp_s;
extern ID id__to_time_from_duckdb_timestamp_ms;
extern ID id__to_time_from_duckdb_timestamp_ns;
extern ID id__to_time_from_duckdb_time_tz;
extern ID id__to_time_from_duckdb_timestamp_tz;
extern ID id__to_infinity;

VALUE infinite_date_value(duckdb_date date);
VALUE infinite_timestamp_value(duckdb_timestamp timestamp);
VALUE infinite_timestamp_s_value(duckdb_timestamp_s timestamp_s);
VALUE infinite_timestamp_ms_value(duckdb_timestamp_ms timestamp_ms);
VALUE infinite_timestamp_ns_value(duckdb_timestamp_ns timestamp_ns);

void rbduckdb_init_duckdb_converter(void);

#endif
