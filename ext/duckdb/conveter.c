#include "ruby-duckdb.h"

VALUE mDuckDBConverter;

ID id__to_date;
ID id__to_time;
ID id__to_time_from_duckdb_time;
ID id__to_interval_from_vector;
ID id__to_hugeint_from_vector;
ID id__to_decimal_from_hugeint;
ID id__to_uuid_from_vector;
ID id__to_time_from_duckdb_timestamp_s;
ID id__to_time_from_duckdb_timestamp_ms;
ID id__to_time_from_duckdb_timestamp_ns;
ID id__to_time_from_duckdb_time_tz;
ID id__to_time_from_duckdb_timestamp_tz;
ID id__to_infinity;

VALUE infinite_date_value(duckdb_date date) {
    if (duckdb_is_finite_date(date) == false) {
        return rb_funcall(mDuckDBConverter, id__to_infinity, 1,
                          INT2NUM(date.days)
                         );
    }
    return Qnil;
}

VALUE infinite_timestamp_value(duckdb_timestamp timestamp) {
    if (duckdb_is_finite_timestamp(timestamp) == false) {
        return rb_funcall(mDuckDBConverter, id__to_infinity, 1,
                          LL2NUM(timestamp.micros)
                         );
    }
    return Qnil;
}

VALUE infinite_timestamp_s_value(duckdb_timestamp_s timestamp_s) {
    if (duckdb_is_finite_timestamp_s(timestamp_s) == false) {
        return rb_funcall(mDuckDBConverter, id__to_infinity, 1,
                          LL2NUM(timestamp_s.seconds)
                         );
    }
    return Qnil;
}

VALUE infinite_timestamp_ms_value(duckdb_timestamp_ms timestamp_ms) {
    if (duckdb_is_finite_timestamp_ms(timestamp_ms) == false) {
        return rb_funcall(mDuckDBConverter, id__to_infinity, 1,
                          LL2NUM(timestamp_ms.millis)
                         );
    }
    return Qnil;
}

VALUE infinite_timestamp_ns_value(duckdb_timestamp_ns timestamp_ns) {
    if (duckdb_is_finite_timestamp_ns(timestamp_ns) == false) {
        return rb_funcall(mDuckDBConverter, id__to_infinity, 1,
                          LL2NUM(timestamp_ns.nanos)
                         );
    }
    return Qnil;
}

void rbduckdb_init_duckdb_converter(void) {
    mDuckDBConverter = rb_define_module_under(mDuckDB, "Converter");

    id__to_date = rb_intern("_to_date");
    id__to_time = rb_intern("_to_time");
    id__to_time_from_duckdb_time = rb_intern("_to_time_from_duckdb_time");
    id__to_interval_from_vector = rb_intern("_to_interval_from_vector");
    id__to_hugeint_from_vector = rb_intern("_to_hugeint_from_vector");
    id__to_decimal_from_hugeint = rb_intern("_to_decimal_from_hugeint");
    id__to_uuid_from_vector = rb_intern("_to_uuid_from_vector");
    id__to_time_from_duckdb_timestamp_s = rb_intern("_to_time_from_duckdb_timestamp_s");
    id__to_time_from_duckdb_timestamp_ms = rb_intern("_to_time_from_duckdb_timestamp_ms");
    id__to_time_from_duckdb_timestamp_ns = rb_intern("_to_time_from_duckdb_timestamp_ns");
    id__to_time_from_duckdb_time_tz = rb_intern("_to_time_from_duckdb_time_tz");
    id__to_time_from_duckdb_timestamp_tz = rb_intern("_to_time_from_duckdb_timestamp_tz");
    id__to_infinity = rb_intern("_to_infinity");
}
