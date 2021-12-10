#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_APPEND_DATE

duckdb_date to_duckdb_date_from_value(VALUE year, VALUE month, VALUE day) {
    duckdb_date_struct dt_struct;

    dt_struct.year = NUM2INT(year);
    dt_struct.month = NUM2INT(month);
    dt_struct.day = NUM2INT(day);

    return duckdb_to_date(dt_struct);
}

#endif
