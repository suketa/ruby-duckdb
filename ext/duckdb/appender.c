#include "ruby-duckdb.h"

static VALUE cDuckDBAppender;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE appender_initialize(VALUE klass, VALUE con, VALUE schema, VALUE table);
static VALUE appender_error_message(VALUE self);
static VALUE appender__append_bool(VALUE self, VALUE val);
static VALUE appender__append_int8(VALUE self, VALUE val);
static VALUE appender__append_int16(VALUE self, VALUE val);
static VALUE appender__append_int32(VALUE self, VALUE val);
static VALUE appender__append_int64(VALUE self, VALUE val);
static VALUE appender__append_uint8(VALUE self, VALUE val);
static VALUE appender__append_uint16(VALUE self, VALUE val);
static VALUE appender__append_uint32(VALUE self, VALUE val);
static VALUE appender__append_uint64(VALUE self, VALUE val);
static VALUE appender__append_float(VALUE self, VALUE val);
static VALUE appender__append_double(VALUE self, VALUE val);
static VALUE appender__append_varchar(VALUE self, VALUE val);
static VALUE appender__append_varchar_length(VALUE self, VALUE val, VALUE len);
static VALUE appender_append_blob(VALUE self, VALUE val);
static VALUE appender_append_null(VALUE self);

#ifdef HAVE_DUCKDB_H_GE_V1_1_0
static VALUE appender_append_default(VALUE self);
#endif

static VALUE appender__end_row(VALUE self);
static VALUE appender__append_date(VALUE self, VALUE yearval, VALUE monthval, VALUE dayval);
static VALUE appender__append_interval(VALUE self, VALUE months, VALUE days, VALUE micros);
static VALUE appender__append_time(VALUE self, VALUE hour, VALUE min, VALUE sec, VALUE micros);
static VALUE appender__append_timestamp(VALUE self, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros);
static VALUE appender__append_hugeint(VALUE self, VALUE lower, VALUE upper);
static VALUE appender__append_uhugeint(VALUE self, VALUE lower, VALUE upper);
static VALUE appender__flush(VALUE self);
static VALUE appender__close(VALUE self);
static VALUE duckdb_state_to_bool_value(duckdb_state state);

static const rb_data_type_t appender_data_type = {
    "DuckDB/Appender",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBAppender *p = (rubyDuckDBAppender *)ctx;

    duckdb_appender_destroy(&(p->appender));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBAppender *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBAppender));
    return TypedData_Wrap_Struct(klass, &appender_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBAppender);
}

static VALUE appender_initialize(VALUE self, VALUE con, VALUE schema, VALUE table) {

    rubyDuckDBConnection *ctxcon;
    rubyDuckDBAppender *ctx;
    char *pschema = 0;

    if (!rb_obj_is_kind_of(con, cDuckDBConnection)) {
        rb_raise(rb_eTypeError, "1st argument should be instance of DackDB::Connection");
    }

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);
    ctxcon = get_struct_connection(con);

    if (schema != Qnil) {
        pschema = StringValuePtr(schema);
    }

    if (duckdb_appender_create(ctxcon->con, pschema, StringValuePtr(table), &(ctx->appender)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to create appender");
    }
    return self;
}

/* call-seq:
 *   appender.error_message -> String
 *
 * Returns the error message of the appender. If there is no error, then it returns nil.
 *
 *   require 'duckdb'
 *   db = DuckDB::Database.open
 *   con = db.connect
 *   con.query('CREATE TABLE users (id INTEGER, name VARCHAR)')
 *   appender = con.appender('users')
 *   appender.error_message # => nil
 */
static VALUE appender_error_message(VALUE self) {
    rubyDuckDBAppender *ctx;
    const char *msg;
    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    msg = duckdb_appender_error(ctx->appender);
    if (msg == NULL) {
        return Qnil;
    }
    return rb_str_new2(msg);
}

/* :nodoc: */
static VALUE appender__end_row(VALUE self) {
    rubyDuckDBAppender *ctx;
    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_appender_end_row(ctx->appender));
}

/* :nodoc: */
static VALUE appender__append_bool(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    if (val != Qtrue && val != Qfalse) {
        rb_raise(rb_eArgError, "argument must be boolean");
    }

    return duckdb_state_to_bool_value(duckdb_append_bool(ctx->appender, (val == Qtrue)));
}

/* :nodoc: */
static VALUE appender__append_int8(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int8_t i8val = (int8_t)NUM2INT(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_int8(ctx->appender, i8val));
}

/* :nodoc: */
static VALUE appender__append_int16(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int16_t i16val = (int16_t)NUM2INT(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_int16(ctx->appender, i16val));
}

/* :nodoc: */
static VALUE appender__append_int32(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int32_t i32val = (int32_t)NUM2INT(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_int32(ctx->appender, i32val));
}

/* :nodoc: */
static VALUE appender__append_int64(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    int64_t i64val = (int64_t)NUM2LL(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_int64(ctx->appender, i64val));
}

/* :nodoc: */
static VALUE appender__append_uint8(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    uint8_t ui8val = (uint8_t)NUM2UINT(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_uint8(ctx->appender, ui8val));
}

/* :nodoc: */
static VALUE appender__append_uint16(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    uint16_t ui16val = (uint16_t)NUM2UINT(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return  duckdb_state_to_bool_value(duckdb_append_uint16(ctx->appender, ui16val));
}

/* :nodoc: */
static VALUE appender__append_uint32(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    uint32_t ui32val = (uint32_t)NUM2UINT(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return  duckdb_state_to_bool_value(duckdb_append_uint32(ctx->appender, ui32val));
}

/* :nodoc: */
static VALUE appender__append_uint64(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    uint64_t ui64val = (uint64_t)NUM2ULL(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_uint64(ctx->appender, ui64val));
}

/* :nodoc: */
static VALUE appender__append_float(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    float fval = (float)NUM2DBL(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_float(ctx->appender, fval));
}

/* :nodoc: */
static VALUE appender__append_double(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    double dval = NUM2DBL(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_double(ctx->appender, dval));
}

/* :nodoc: */
static VALUE appender__append_varchar(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;
    char *pval = StringValuePtr(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_varchar(ctx->appender, pval));
}

/* :nodoc: */
static VALUE appender__append_varchar_length(VALUE self, VALUE val, VALUE len) {
    rubyDuckDBAppender *ctx;

    char *pval = StringValuePtr(val);
    idx_t length = (idx_t)NUM2ULL(len);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_append_varchar_length(ctx->appender, pval, length));
}

static VALUE appender_append_blob(VALUE self, VALUE val) {
    rubyDuckDBAppender *ctx;

    char *pval = StringValuePtr(val);
    idx_t length = (idx_t)RSTRING_LEN(val);

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    if (duckdb_append_blob(ctx->appender, (void *)pval, length) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append blob");
    }
    return self;
}

static VALUE appender_append_null(VALUE self) {
    rubyDuckDBAppender *ctx;
    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    if (duckdb_append_null(ctx->appender) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append null");
    }
    return self;
}

#ifdef HAVE_DUCKDB_H_GE_V1_1_0
static VALUE appender_append_default(VALUE self) {
    rubyDuckDBAppender *ctx;
    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    if (duckdb_append_default(ctx->appender) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append default");
    }
    return self;
}
#endif

/* :nodoc: */
static VALUE appender__append_date(VALUE self, VALUE year, VALUE month, VALUE day) {
    duckdb_date dt;
    rubyDuckDBAppender *ctx;

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);
    dt = rbduckdb_to_duckdb_date_from_value(year, month, day);

    if (duckdb_append_date(ctx->appender, dt) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append date");
    }
    return self;
}

/* :nodoc: */
static VALUE appender__append_interval(VALUE self, VALUE months, VALUE days, VALUE micros) {
    duckdb_interval interval;
    rubyDuckDBAppender *ctx;

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);
    rbduckdb_to_duckdb_interval_from_value(&interval, months, days, micros);

    if (duckdb_append_interval(ctx->appender, interval) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append interval");
    }
    return self;
}

/* :nodoc: */
static VALUE appender__append_time(VALUE self, VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_time time;
    rubyDuckDBAppender *ctx;

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);
    time = rbduckdb_to_duckdb_time_from_value(hour, min, sec, micros);

    if (duckdb_append_time(ctx->appender, time) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append time");
    }
    return self;
}

/* :nodoc: */
static VALUE appender__append_timestamp(VALUE self, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_timestamp timestamp;

    rubyDuckDBAppender *ctx;

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    timestamp = rbduckdb_to_duckdb_timestamp_from_value(year, month, day, hour, min, sec, micros);

    if (duckdb_append_timestamp(ctx->appender, timestamp) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append timestamp");
    }
    return self;
}

/* :nodoc: */
static VALUE appender__append_hugeint(VALUE self, VALUE lower, VALUE upper) {
    duckdb_hugeint hugeint;

    hugeint.lower = NUM2ULL(lower);
    hugeint.upper = NUM2LL(upper);

    rubyDuckDBAppender *ctx;

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);
    if (duckdb_append_hugeint(ctx->appender, hugeint) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append hugeint");
    }
    return self;
}

/* :nodoc: */
static VALUE appender__append_uhugeint(VALUE self, VALUE lower, VALUE upper) {
    duckdb_uhugeint uhugeint;

    uhugeint.lower = NUM2ULL(lower);
    uhugeint.upper = NUM2ULL(upper);

    rubyDuckDBAppender *ctx;

    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);
    if (duckdb_append_uhugeint(ctx->appender, uhugeint) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to append uhugeint");
    }
    return self;
}

/* :nodoc: */
static VALUE appender__flush(VALUE self) {
    rubyDuckDBAppender *ctx;
    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_appender_flush(ctx->appender));
}

/* :nodoc: */
static VALUE appender__close(VALUE self) {
    rubyDuckDBAppender *ctx;
    TypedData_Get_Struct(self, rubyDuckDBAppender, &appender_data_type, ctx);

    return duckdb_state_to_bool_value(duckdb_appender_close(ctx->appender));
}

static VALUE duckdb_state_to_bool_value(duckdb_state state) {
    if (state == DuckDBSuccess) {
        return Qtrue;
    }
    return Qfalse;
}

void rbduckdb_init_duckdb_appender(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBAppender = rb_define_class_under(mDuckDB, "Appender", rb_cObject);
    rb_define_alloc_func(cDuckDBAppender, allocate);
    rb_define_method(cDuckDBAppender, "initialize", appender_initialize, 3);
    rb_define_method(cDuckDBAppender, "error_message", appender_error_message, 0);
    rb_define_method(cDuckDBAppender, "append_blob", appender_append_blob, 1);
    rb_define_method(cDuckDBAppender, "append_null", appender_append_null, 0);

#ifdef HAVE_DUCKDB_H_GE_V1_1_0
    rb_define_method(cDuckDBAppender, "append_default", appender_append_default, 0);
#endif

    rb_define_private_method(cDuckDBAppender, "_end_row", appender__end_row, 0);
    rb_define_private_method(cDuckDBAppender, "_flush", appender__flush, 0);
    rb_define_private_method(cDuckDBAppender, "_close", appender__close, 0);
    rb_define_private_method(cDuckDBAppender, "_append_bool", appender__append_bool, 1);
    rb_define_private_method(cDuckDBAppender, "_append_int8", appender__append_int8, 1);
    rb_define_private_method(cDuckDBAppender, "_append_int16", appender__append_int16, 1);
    rb_define_private_method(cDuckDBAppender, "_append_int32", appender__append_int32, 1);
    rb_define_private_method(cDuckDBAppender, "_append_int64", appender__append_int64, 1);
    rb_define_private_method(cDuckDBAppender, "_append_uint8", appender__append_uint8, 1);
    rb_define_private_method(cDuckDBAppender, "_append_uint16", appender__append_uint16, 1);
    rb_define_private_method(cDuckDBAppender, "_append_uint32", appender__append_uint32, 1);
    rb_define_private_method(cDuckDBAppender, "_append_uint64", appender__append_uint64, 1);
    rb_define_private_method(cDuckDBAppender, "_append_float", appender__append_float, 1);
    rb_define_private_method(cDuckDBAppender, "_append_double", appender__append_double, 1);
    rb_define_private_method(cDuckDBAppender, "_append_varchar", appender__append_varchar, 1);
    rb_define_private_method(cDuckDBAppender, "_append_varchar_length", appender__append_varchar_length, 2);
    rb_define_private_method(cDuckDBAppender, "_append_date", appender__append_date, 3);
    rb_define_private_method(cDuckDBAppender, "_append_interval", appender__append_interval, 3);
    rb_define_private_method(cDuckDBAppender, "_append_time", appender__append_time, 4);
    rb_define_private_method(cDuckDBAppender, "_append_timestamp", appender__append_timestamp, 7);
    rb_define_private_method(cDuckDBAppender, "_append_hugeint", appender__append_hugeint, 2);
    rb_define_private_method(cDuckDBAppender, "_append_uhugeint", appender__append_uhugeint, 2);
}
