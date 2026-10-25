#include "ruby-duckdb.h"

#define MICROS_PER_DAY 86400000000LL

/*
 * Mirrors DuckDB's Date::IsValid. duckdb_to_date throws a C++ exception
 * for dates it rejects, which would abort the process.
 */
static int is_valid_date(int32_t year, int32_t month, int32_t day) {
    static const int32_t days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int32_t max_day;

    if (month < 1 || month > 12 || day < 1) {
        return 0;
    }
    if (year < -5877641 || (year == -5877641 && (month < 6 || (month == 6 && day < 25)))) {
        return 0;
    }
    if (year > 5881580 || (year == 5881580 && (month > 7 || (month == 7 && day > 10)))) {
        return 0;
    }
    max_day = days_in_month[month];
    if (month == 2 && ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)) {
        max_day = 29;
    }
    return day <= max_day;
}

static duckdb_date to_duckdb_date(duckdb_date_struct dt_struct) {
    if (!is_valid_date(dt_struct.year, dt_struct.month, dt_struct.day)) {
        rb_raise(rb_eRangeError, "date out of range: %d-%02d-%02d", dt_struct.year, dt_struct.month, dt_struct.day);
    }
    return duckdb_to_date(dt_struct);
}

duckdb_date rbduckdb_to_duckdb_date_from_value(VALUE year, VALUE month, VALUE day) {
    duckdb_date_struct dt_struct;

    dt_struct.year = NUM2INT(year);
    dt_struct.month = NUM2INT(month);
    dt_struct.day = NUM2INT(day);

    return to_duckdb_date(dt_struct);
}

duckdb_time rbduckdb_to_duckdb_time_from_value(VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_time_struct time_st;

    time_st.hour = NUM2INT(hour);
    time_st.min = NUM2INT(min);
    time_st.sec = NUM2INT(sec);
    time_st.micros = NUM2INT(micros);

    return duckdb_to_time(time_st);
}

duckdb_time_ns rbduckdb_to_duckdb_time_ns_from_value(VALUE hour, VALUE min, VALUE sec, VALUE nanos) {
    duckdb_time_ns time_ns;

    time_ns.nanos = ((NUM2LL(hour) * 60 + NUM2LL(min)) * 60 + NUM2LL(sec)) * 1000000000LL + NUM2LL(nanos);

    return time_ns;
}

duckdb_timestamp rbduckdb_to_duckdb_timestamp_from_time_value(VALUE time_obj) {
    if (!rb_obj_is_kind_of(time_obj, rb_cTime)) {
        rb_raise(rb_eTypeError, "Expected Time object");
    }

    return rbduckdb_to_duckdb_timestamp_from_value(
        rb_funcall(time_obj, rb_intern("year"), 0),
        rb_funcall(time_obj, rb_intern("month"), 0),
        rb_funcall(time_obj, rb_intern("day"), 0),
        rb_funcall(time_obj, rb_intern("hour"), 0),
        rb_funcall(time_obj, rb_intern("min"), 0),
        rb_funcall(time_obj, rb_intern("sec"), 0),
        rb_funcall(time_obj, rb_intern("usec"), 0)
    );
}

duckdb_timestamp rbduckdb_to_duckdb_timestamp_from_value(VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_timestamp_struct timestamp_st;
    duckdb_timestamp result;
    int64_t days;
    int64_t time_micros;

    timestamp_st.date.year = NUM2INT(year);
    timestamp_st.date.month = NUM2INT(month);
    timestamp_st.date.day = NUM2INT(day);
    timestamp_st.time.hour = NUM2INT(hour);
    timestamp_st.time.min = NUM2INT(min);
    timestamp_st.time.sec = NUM2INT(sec);
    timestamp_st.time.micros = NUM2INT(micros);

    /* Mirrors DuckDB's Timestamp::TryFromDatetime; duckdb_to_timestamp throws on failure. */
    days = to_duckdb_date(timestamp_st.date).days;
    time_micros = duckdb_to_time(timestamp_st.time).micros;
    if (days > INT64_MAX / MICROS_PER_DAY || days < INT64_MIN / MICROS_PER_DAY) {
        rb_raise(rb_eRangeError, "timestamp out of range");
    }
    result.micros = days * MICROS_PER_DAY;
    if ((time_micros > 0 && result.micros > INT64_MAX - time_micros) ||
        (time_micros < 0 && result.micros < INT64_MIN - time_micros)) {
        rb_raise(rb_eRangeError, "timestamp out of range");
    }
    result.micros += time_micros;
    if (!duckdb_is_finite_timestamp(result)) {
        rb_raise(rb_eRangeError, "timestamp out of range");
    }
    return result;
}

void rbduckdb_to_duckdb_interval_from_value(duckdb_interval* interval, VALUE months, VALUE days, VALUE micros) {
    interval->months = NUM2INT(months);
    interval->days = NUM2INT(days);
    interval->micros = NUM2LL(micros);
}
