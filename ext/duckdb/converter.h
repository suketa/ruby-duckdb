#ifndef RUBY_DUCKDB_CONVERTER_H
#define RUBY_DUCKDB_CONVERTER_H

extern ID id__to_date;
extern ID id__to_time;
extern ID id__to_time_from_duckdb_time;
extern ID id__to_interval_from_vector;
extern ID id__to_hugeint_from_vector;
extern ID id__to_decimal_from_hugeint;
extern ID id__to_uuid_from_vector;
extern ID id__to_uuid_from_uhugeint;
extern ID id__uuid_string_to_hugeint;
extern ID id__to_time_from_duckdb_timestamp_s;
extern ID id__to_time_from_duckdb_timestamp_ms;
extern ID id__to_time_from_duckdb_timestamp_ns;
extern ID id__to_time_from_duckdb_time_ns;
extern ID id__to_time_from_duckdb_time_tz;
extern ID id__to_time_from_duckdb_timestamp_tz;
extern ID id__to_infinity;

VALUE rbduckdb_uuid_to_ruby(duckdb_hugeint h);
VALUE rbduckdb_uuid_uhugeint_to_ruby(duckdb_uhugeint h);
VALUE rbduckdb_interval_to_ruby(duckdb_interval i);
VALUE rbduckdb_hugeint_to_ruby(duckdb_hugeint h);
VALUE rbduckdb_uhugeint_to_ruby(duckdb_uhugeint h);
VALUE rbduckdb_timestamp_s_to_ruby(duckdb_timestamp_s ts);
VALUE rbduckdb_timestamp_ms_to_ruby(duckdb_timestamp_ms ts);
VALUE rbduckdb_timestamp_ns_to_ruby(duckdb_timestamp_ns ts);
VALUE rbduckdb_time_ns_to_ruby(duckdb_time_ns ts);
VALUE rbduckdb_time_tz_to_ruby(duckdb_time_tz tz);
VALUE rbduckdb_timestamp_tz_to_ruby(duckdb_timestamp ts);
VALUE rbduckdb_time_to_ruby(duckdb_time t);
VALUE rbduckdb_date_to_ruby(duckdb_date date);
VALUE rbduckdb_timestamp_to_ruby(duckdb_timestamp ts);

VALUE infinite_date_value(duckdb_date date);
VALUE infinite_timestamp_value(duckdb_timestamp timestamp);
VALUE infinite_timestamp_s_value(duckdb_timestamp_s timestamp_s);
VALUE infinite_timestamp_ms_value(duckdb_timestamp_ms timestamp_ms);
VALUE infinite_timestamp_ns_value(duckdb_timestamp_ns timestamp_ns);

void rbduckdb_init_duckdb_converter(void);

#endif
