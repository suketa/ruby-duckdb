#include "ruby-duckdb.h"

static VALUE cDuckDBAppender;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static VALUE appender_initialize(VALUE klass, VALUE con, VALUE schema, VALUE table);
static VALUE appender_begin_row(VALUE self);
static VALUE appender_end_row(VALUE self);
static VALUE appender_append_bool(VALUE self, VALUE val);
static VALUE appender_append_int8(VALUE self, VALUE val);
static VALUE appender_append_int16(VALUE self, VALUE val);
static VALUE appender_append_int32(VALUE self, VALUE val);
static VALUE appender_append_int64(VALUE self, VALUE val);
static VALUE appender_append_uint8(VALUE self, VALUE val);
static VALUE appender_append_uint16(VALUE self, VALUE val);
static VALUE appender_append_uint32(VALUE self, VALUE val);
static VALUE appender_append_uint64(VALUE self, VALUE val);
static VALUE appender_append_float(VALUE self, VALUE val);
static VALUE appender_append_double(VALUE self, VALUE val);
static VALUE appender_append_varchar(VALUE self, VALUE val);
static VALUE appender_append_varchar_length(VALUE self, VALUE val, VALUE len);
static VALUE appender_append_blob(VALUE self, VALUE val);
static VALUE appender_append_null(VALUE self);

#ifdef HAVE_DUCKDB_APPEND_DATE
static VALUE appender__append_date(VALUE self, VALUE yearval, VALUE monthval, VALUE dayval);
#endif

#ifdef HAVE_DUCKDB_APPEND_INTERVAL
static VALUE appender__append_interval(VALUE self, VALUE months, VALUE days, VALUE micros);
#endif

static VALUE appender__append_time(VALUE self, VALUE hour, VALUE min, VALUE sec, VALUE micros);
static VALUE appender__append_timestamp(VALUE self, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros);
static VALUE appender__append_hugeint(VALUE self, VALUE lower, VALUE upper);

static VALUE appender_flush(VALUE self);
static VALUE appender_close(VALUE self);

static void deallocate(void * ctx) {
    rubyDuckDBAppender *p = (rubyDuckDBAppender *)ctx;

    duckdb_appender_destroy(&(p->appender));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBAppender *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBAppender));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

static VALUE appender_initialize(VALUE self, VALUE con, VALUE schema, VALUE table) {

    rubyDuckDBConnection *ctxcon;
    rubyDuckDBAppender *ctx;
    char *pschema = 0;

    if (!rb_obj_is_kind_of(con, cDuckDBConnection)) {
        rb_raise(rb_eTypeError, "1st argument should be instance of DackDB::Connection");
    }

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);
    Data_Get_Struct(con, rubyDuckDBConnection, ctxcon);

    if (schema != Qnil) {
        pschema = StringValuePtr(schema);
    }

    if (duckdb_appender_create(ctxcon->con, pschema, StringValuePtr(table), &(ctx->appender)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to create appender");
    }
    return self;
}

static VALUE appender_begin_row(VALUE self) {
    rubyDuckDBAppender *ctx;
    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_appender_begin_row(ctx->appender) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to flush");
    }
    return self;
}

static VALUE appender_end_row(VALUE self) {
    rubyDuckDBAppender *ctx;
    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_appender_end_row(ctx->appender) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to flush");
    }
    return self;
}

static VALUE appender_append_bool(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (val != Qtrue && val != Qfalse) {
        rb_raise(rb_eArgError, "argument must be boolean");
    }

    if (duckdb_append_bool(ctx->appender, (val == Qtrue)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append boolean");
    }
    return self;
}

static VALUE appender_append_int8(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int8_t i8val = (int8_t)NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_int8(ctx->appender, i8val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_int16(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int16_t i16val = (int16_t)NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_int16(ctx->appender, i16val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_int32(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int32_t i32val = (int32_t)NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_int32(ctx->appender, i32val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_int64(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int64_t i64val = (int64_t)NUM2LL(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_int64(ctx->appender, i64val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_uint8(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int8_t ui8val = (uint8_t)NUM2UINT(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_uint8(ctx->appender, ui8val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_uint16(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    uint16_t ui16val = (uint16_t)NUM2UINT(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_uint16(ctx->appender, ui16val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_uint32(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    uint32_t ui32val = (uint32_t)NUM2UINT(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_uint32(ctx->appender, ui32val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_uint64(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    uint64_t ui64val = (uint64_t)NUM2ULL(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_uint64(ctx->appender, ui64val) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_float(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    float fval = (float)NUM2DBL(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_float(ctx->appender, fval) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_double(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    double dval = NUM2DBL(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_double(ctx->appender, dval) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_varchar(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    char *pval = StringValuePtr(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_varchar(ctx->appender, pval) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_varchar_length(VALUE self, VALUE val, VALUE len) {
    rubyDuckDBAppender *ctx;

    char *pval = StringValuePtr(val);
    idx_t length = (idx_t)NUM2ULL(len);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_varchar_length(ctx->appender, pval, length) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_blob(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;

    char *pval = StringValuePtr(val);
    idx_t length = (idx_t)RSTRING_LEN(val);

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_blob(ctx->appender, (void *)pval, length) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

static VALUE appender_append_null(VALUE self) {
    rubyDuckDBAppender *ctx;
    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_append_null(ctx->appender) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append");
    }
    return self;
}

#ifdef HAVE_DUCKDB_APPEND_DATE
static VALUE appender__append_date(VALUE self, VALUE year, VALUE month, VALUE day) {
    duckdb_date dt;
    rubyDuckDBAppender *ctx;

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);
    dt = to_duckdb_date_from_value(year, month, day);

    if (duckdb_append_date(ctx->appender, dt) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append date");
    }
    return self;
}
#endif

#ifdef HAVE_DUCKDB_APPEND_INTERVAL
static VALUE appender__append_interval(VALUE self, VALUE months, VALUE days, VALUE micros) {
    duckdb_interval interval;
    rubyDuckDBAppender *ctx;

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);
    to_duckdb_interval_from_value(&interval, months, days, micros);

    if (duckdb_append_interval(ctx->appender, interval) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append interval");
    }
    return self;
}
#endif

static VALUE appender__append_time(VALUE self, VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_time time;
    rubyDuckDBAppender *ctx;

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);
    time = to_duckdb_time_from_value(hour, min, sec, micros);

    if (duckdb_append_time(ctx->appender, time) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append time");
    }
    return self;
}

static VALUE appender__append_timestamp(VALUE self, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_timestamp timestamp;

    rubyDuckDBAppender *ctx;

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    timestamp = to_duckdb_timestamp_from_value(year, month, day, hour, min, sec, micros);

    if (duckdb_append_timestamp(ctx->appender, timestamp) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append timestamp");
    }
    return self;
}

static VALUE appender__append_hugeint(VALUE self, VALUE lower, VALUE upper) {
    duckdb_hugeint hugeint;

    hugeint.lower = NUM2ULL(lower);
    hugeint.upper = NUM2LL(upper);

    rubyDuckDBAppender *ctx;

    Data_Get_Struct(self, rubyDuckDBAppender, ctx);
    if (duckdb_append_hugeint(ctx->appender, hugeint) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append hugeint");
    }
    return self;
}

static VALUE appender_flush(VALUE self) {
    rubyDuckDBAppender *ctx;
    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_appender_flush(ctx->appender) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to flush");
    }
    return self;
}

static VALUE appender_close(VALUE self) {
    rubyDuckDBAppender *ctx;
    Data_Get_Struct(self, rubyDuckDBAppender, ctx);

    if (duckdb_appender_close(ctx->appender) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to flush");
    }
    return self;
}

void init_duckdb_appender(void) {
    cDuckDBAppender = rb_define_class_under(mDuckDB, "Appender", rb_cObject);
    rb_define_alloc_func(cDuckDBAppender, allocate);
    rb_define_method(cDuckDBAppender, "initialize", appender_initialize, 3);
    rb_define_method(cDuckDBAppender, "begin_row", appender_begin_row, 0);
    rb_define_method(cDuckDBAppender, "end_row", appender_end_row, 0);
    rb_define_method(cDuckDBAppender, "append_bool", appender_append_bool, 1);
    rb_define_method(cDuckDBAppender, "append_int8", appender_append_int8, 1);
    rb_define_method(cDuckDBAppender, "append_int16", appender_append_int16, 1);
    rb_define_method(cDuckDBAppender, "append_int32", appender_append_int32, 1);
    rb_define_method(cDuckDBAppender, "append_int64", appender_append_int64, 1);
    rb_define_method(cDuckDBAppender, "append_uint8", appender_append_uint8, 1);
    rb_define_method(cDuckDBAppender, "append_uint16", appender_append_uint16, 1);
    rb_define_method(cDuckDBAppender, "append_uint32", appender_append_uint32, 1);
    rb_define_method(cDuckDBAppender, "append_uint64", appender_append_uint64, 1);
    rb_define_method(cDuckDBAppender, "append_float", appender_append_float, 1);
    rb_define_method(cDuckDBAppender, "append_double", appender_append_double, 1);
    rb_define_method(cDuckDBAppender, "append_varchar", appender_append_varchar, 1);
    rb_define_method(cDuckDBAppender, "append_varchar_length", appender_append_varchar_length, 2);
    rb_define_method(cDuckDBAppender, "append_blob", appender_append_blob, 1);
    rb_define_method(cDuckDBAppender, "append_null", appender_append_null, 0);
#ifdef HAVE_DUCKDB_APPEND_DATE
    rb_define_private_method(cDuckDBAppender, "_append_date", appender__append_date, 3);
#endif
#ifdef HAVE_DUCKDB_APPEND_INTERVAL
    rb_define_private_method(cDuckDBAppender, "_append_interval", appender__append_interval, 3);
#endif
    rb_define_private_method(cDuckDBAppender, "_append_time", appender__append_time, 4);
    rb_define_private_method(cDuckDBAppender, "_append_timestamp", appender__append_timestamp, 7);
    rb_define_private_method(cDuckDBAppender, "_append_hugeint", appender__append_hugeint, 2);
    rb_define_method(cDuckDBAppender, "flush", appender_flush, 0);
    rb_define_method(cDuckDBAppender, "close", appender_close, 0);
}
