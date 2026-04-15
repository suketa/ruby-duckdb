#include "ruby-duckdb.h"

VALUE mDuckDBConverter;

ID id__to_date;
ID id__to_time;
ID id__to_time_from_duckdb_time;
ID id__to_interval_from_vector;
ID id__to_hugeint_from_vector;
ID id__to_decimal_from_hugeint;
ID id__uuid_string_to_hugeint;
ID id__to_time_from_duckdb_timestamp_s;
ID id__to_time_from_duckdb_timestamp_ms;
ID id__to_time_from_duckdb_timestamp_ns;
ID id__to_time_from_duckdb_time_ns;
ID id__to_time_from_duckdb_time_tz;
ID id__to_time_from_duckdb_timestamp_tz;
ID id__to_infinity;
ID id__decimal_to_unscaled;

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

#define DUCKDB_UUID_SIGN_BIT 0x8000000000000000ULL

/*
 * Write a 128-bit UUID (hi/lo) into buf[36] as "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
 *
 * The 128-bit value is split into two uint64_t halves. Each half is consumed
 * from its most-significant nibble downward: for each output character we shift
 * the relevant half right so the target nibble lands in the lowest 4 bits, mask
 * with 0xf, and use the result as an index into a 16-character lookup table.
 * This avoids sprintf's format-string overhead and any intermediate heap
 * allocation — the result is written directly into a 36-byte stack buffer and
 * wrapped in a single rb_utf8_str_new call.
 *
 * Dash positions (8, 13, 18, 23) follow the standard UUID layout:
 *   xxxxxxxx - xxxx - xxxx - xxxx - xxxxxxxxxxxx
 *   hi[63:32]  hi[31:16] hi[15:0]  lo[63:48]  lo[47:0]
 */
static void uuid_to_str(uint64_t hi, uint64_t lo, char *buf)
{
    static const char hex[] = "0123456789abcdef";

    /* xxxxxxxx- (bytes 0-3 of hi) */
    buf[ 0] = hex[(hi >> 60) & 0xf];
    buf[ 1] = hex[(hi >> 56) & 0xf];
    buf[ 2] = hex[(hi >> 52) & 0xf];
    buf[ 3] = hex[(hi >> 48) & 0xf];
    buf[ 4] = hex[(hi >> 44) & 0xf];
    buf[ 5] = hex[(hi >> 40) & 0xf];
    buf[ 6] = hex[(hi >> 36) & 0xf];
    buf[ 7] = hex[(hi >> 32) & 0xf];
    buf[ 8] = '-';
    /* xxxx- (bytes 4-5 of hi) */
    buf[ 9] = hex[(hi >> 28) & 0xf];
    buf[10] = hex[(hi >> 24) & 0xf];
    buf[11] = hex[(hi >> 20) & 0xf];
    buf[12] = hex[(hi >> 16) & 0xf];
    buf[13] = '-';
    /* xxxx- (bytes 6-7 of hi) */
    buf[14] = hex[(hi >> 12) & 0xf];
    buf[15] = hex[(hi >>  8) & 0xf];
    buf[16] = hex[(hi >>  4) & 0xf];
    buf[17] = hex[ hi        & 0xf];
    buf[18] = '-';
    /* xxxx- (bytes 0-1 of lo) */
    buf[19] = hex[(lo >> 60) & 0xf];
    buf[20] = hex[(lo >> 56) & 0xf];
    buf[21] = hex[(lo >> 52) & 0xf];
    buf[22] = hex[(lo >> 48) & 0xf];
    buf[23] = '-';
    /* xxxxxxxxxxxx (bytes 2-7 of lo) */
    buf[24] = hex[(lo >> 44) & 0xf];
    buf[25] = hex[(lo >> 40) & 0xf];
    buf[26] = hex[(lo >> 36) & 0xf];
    buf[27] = hex[(lo >> 32) & 0xf];
    buf[28] = hex[(lo >> 28) & 0xf];
    buf[29] = hex[(lo >> 24) & 0xf];
    buf[30] = hex[(lo >> 20) & 0xf];
    buf[31] = hex[(lo >> 16) & 0xf];
    buf[32] = hex[(lo >> 12) & 0xf];
    buf[33] = hex[(lo >>  8) & 0xf];
    buf[34] = hex[(lo >>  4) & 0xf];
    buf[35] = hex[ lo        & 0xf];
}

VALUE rbduckdb_uuid_to_ruby(duckdb_hugeint h) {
    char buf[36];
    // DuckDB stores UUIDs with the sign bit of upper flipped for unsigned sort order.
    // Cast to uint64_t first so all bit manipulation stays unsigned.
    uint64_t hi = (uint64_t)h.upper ^ DUCKDB_UUID_SIGN_BIT;
    uint64_t lo = h.lower;
    uuid_to_str(hi, lo, buf);
    return rb_utf8_str_new(buf, 36);
}

VALUE rbduckdb_uuid_uhugeint_to_ruby(duckdb_uhugeint h) {
    char buf[36];
    uuid_to_str(h.upper, h.lower, buf);
    return rb_utf8_str_new(buf, 36);
}

VALUE rbduckdb_interval_to_ruby(duckdb_interval i) {
    return rb_funcall(mDuckDBConverter, id__to_interval_from_vector, 3,
                      INT2NUM(i.months),
                      INT2NUM(i.days),
                      LL2NUM(i.micros)
                      );
}

VALUE rbduckdb_hugeint_to_ruby(duckdb_hugeint h) {
    return rb_funcall(mDuckDBConverter, id__to_hugeint_from_vector, 2,
                      ULL2NUM(h.lower),
                      LL2NUM(h.upper)
                      );
}

VALUE rbduckdb_uhugeint_to_ruby(duckdb_uhugeint h) {
    return rb_funcall(mDuckDBConverter, id__to_hugeint_from_vector, 2,
                      ULL2NUM(h.lower),
                      ULL2NUM(h.upper)
                      );
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

VALUE rbduckdb_time_ns_to_ruby(duckdb_time_ns ts) {
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_time_ns, 1,
                      LL2NUM(ts.nanos)
                      );
}

VALUE rbduckdb_time_tz_to_ruby(duckdb_time_tz tz) {
    duckdb_time_tz_struct data = duckdb_from_time_tz(tz);
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_time_tz, 5,
                      INT2FIX(data.time.hour),
                      INT2FIX(data.time.min),
                      INT2FIX(data.time.sec),
                      INT2NUM(data.time.micros),
                      INT2NUM(data.offset)
                      );
}

VALUE rbduckdb_timestamp_tz_to_ruby(duckdb_timestamp ts) {
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_tz, 1,
                      LL2NUM(ts.micros)
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
    id__uuid_string_to_hugeint = rb_intern("_uuid_string_to_hugeint");
    id__to_time_from_duckdb_timestamp_s = rb_intern("_to_time_from_duckdb_timestamp_s");
    id__to_time_from_duckdb_timestamp_ms = rb_intern("_to_time_from_duckdb_timestamp_ms");
    id__to_time_from_duckdb_timestamp_ns = rb_intern("_to_time_from_duckdb_timestamp_ns");
    id__to_time_from_duckdb_time_ns = rb_intern("_to_time_from_duckdb_time_ns");
    id__to_time_from_duckdb_time_tz = rb_intern("_to_time_from_duckdb_time_tz");
    id__to_time_from_duckdb_timestamp_tz = rb_intern("_to_time_from_duckdb_timestamp_tz");
    id__to_infinity = rb_intern("_to_infinity");
    id__decimal_to_unscaled = rb_intern("_decimal_to_unscaled");
}
