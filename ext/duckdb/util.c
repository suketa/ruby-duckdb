#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_APPEND_DATE

duckdb_date to_duckdb_date_from_value(VALUE year, VALUE month, VALUE day) {
    duckdb_date_struct dt_struct;

    dt_struct.year = NUM2INT(year);
    dt_struct.month = NUM2INT(month);
    dt_struct.day = NUM2INT(day);

    return duckdb_to_date(dt_struct);
}

duckdb_time to_duckdb_time_from_value(VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_time_struct time_st;

    time_st.hour = NUM2INT(hour);
    time_st.min = NUM2INT(min);
    time_st.sec = NUM2INT(sec);
    time_st.micros = NUM2INT(micros);

    return duckdb_to_time(time_st);
}

#endif
