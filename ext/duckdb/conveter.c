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

VALUE rbduckdb_timestamp_s_to_ruby(duckdb_timestamp_s ts) {
    VALUE obj = infinite_timestamp_s_value(ts);
    if (obj != Qnil) {
        return obj;
    }
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_s, 1,
                      LL2NUM(ts.seconds)
                      );
}

VALUE rbduckdb_timestamp_ms_to_ruby(duckdb_timestamp_ms ts) {
    VALUE obj = infinite_timestamp_ms_value(ts);
    if (obj != Qnil) {
        return obj;
    }
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_ms, 1,
                      LL2NUM(ts.millis)
                      );
}

VALUE rbduckdb_timestamp_ns_to_ruby(duckdb_timestamp_ns ts) {
    VALUE obj = infinite_timestamp_ns_value(ts);
    if (obj != Qnil) {
        return obj;
    }
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_ns, 1,
                      LL2NUM(ts.nanos)
                      );
}

VALUE rbduckdb_time_to_ruby(duckdb_time t) {
    duckdb_time_struct data = duckdb_from_time(t);
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_time, 4,
                      INT2FIX(data.hour),
                      INT2FIX(data.min),
                      INT2FIX(data.sec),
                      INT2NUM(data.micros)
                      );
}

VALUE rbduckdb_date_to_ruby(duckdb_date date) {
    VALUE obj = infinite_date_value(date);

    if (obj == Qnil) {
        duckdb_date_struct date_st = duckdb_from_date(date);
        obj = rb_funcall(mDuckDBConverter, id__to_date, 3,
                         INT2FIX(date_st.year),
                         INT2FIX(date_st.month),
                         INT2FIX(date_st.day)
                         );
    }
    return obj;
}

VALUE rbduckdb_timestamp_to_ruby(duckdb_timestamp ts) {
    VALUE obj = infinite_timestamp_value(ts);

    if (obj == Qnil) {
        duckdb_timestamp_struct data_st = duckdb_from_timestamp(ts);
        obj = rb_funcall(mDuckDBConverter, id__to_time, 7,
                         INT2FIX(data_st.date.year),
                         INT2FIX(data_st.date.month),
                         INT2FIX(data_st.date.day),
                         INT2FIX(data_st.time.hour),
                         INT2FIX(data_st.time.min),
                         INT2FIX(data_st.time.sec),
                         INT2NUM(data_st.time.micros)
                         );
    }
    return obj;
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
