#include "ruby-duckdb.h"

static VALUE cDuckDBPreparedStatement;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static VALUE duckdb_prepared_statement_initialize(VALUE self, VALUE con, VALUE query);
static VALUE duckdb_prepared_statement_nparams(VALUE self);
static VALUE duckdb_prepared_statement_execute(VALUE self);
static idx_t check_index(VALUE vidx);
static VALUE duckdb_prepared_statement_bind_bool(VALUE self, VALUE vidx, VALUE val);
static VALUE duckdb_prepared_statement_bind_int8(VALUE self, VALUE vidx, VALUE val);
static VALUE duckdb_prepared_statement_bind_int16(VALUE self, VALUE vidx, VALUE val);
static VALUE duckdb_prepared_statement_bind_int32(VALUE self, VALUE vidx, VALUE val);
static VALUE duckdb_prepared_statement_bind_int64(VALUE self, VALUE vidx, VALUE val);
static VALUE duckdb_prepared_statement_bind_float(VALUE self, VALUE vidx, VALUE val);
static VALUE duckdb_prepared_statement_bind_double(VALUE self, VALUE vidx, VALUE val);
static VALUE duckdb_prepared_statement_bind_varchar(VALUE self, VALUE vidx, VALUE str);
static VALUE duckdb_prepared_statement_bind_blob(VALUE self, VALUE vidx, VALUE blob);
static VALUE duckdb_prepared_statement_bind_null(VALUE self, VALUE vidx);

#ifdef HAVE_DUCKDB_BIND_DATE
static VALUE duckdb_prepared_statement__bind_date(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day);
#endif

#ifdef HAVE_DUCKDB_BIND_TIME
static VALUE duckdb_prepared_statement__bind_time(VALUE self, VALUE vidx, VALUE hour, VALUE min, VALUE sec, VALUE micros);
#endif

static VALUE duckdb_prepared_statement__bind_timestamp(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros);
static VALUE duckdb_prepared_statement__bind_interval(VALUE self, VALUE vidx, VALUE months, VALUE days, VALUE micros);

static void deallocate(void *ctx) {
    rubyDuckDBPreparedStatement *p = (rubyDuckDBPreparedStatement *)ctx;

    duckdb_destroy_prepare(&(p->prepared_statement));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBPreparedStatement *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBPreparedStatement));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

static VALUE duckdb_prepared_statement_initialize(VALUE self, VALUE con, VALUE query) {
    rubyDuckDBConnection *ctxcon;
    rubyDuckDBPreparedStatement *ctx;

    if (!rb_obj_is_kind_of(con, cDuckDBConnection)) {
        rb_raise(rb_eTypeError, "1st argument should be instance of DackDB::Connection");
    }

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    Data_Get_Struct(con, rubyDuckDBConnection, ctxcon);

    if (duckdb_prepare(ctxcon->con, StringValuePtr(query), &(ctx->prepared_statement)) == DuckDBError) {
#ifdef HAVE_DUCKDB_PREPARE_ERROR
        const char *error = duckdb_prepare_error(ctx->prepared_statement);
        rb_raise(eDuckDBError, "%s", error);
#else
        /* TODO: include query parameter information in error message. */
        rb_raise(eDuckDBError, "failed to prepare statement");
#endif
    }
    return self;
}

static VALUE duckdb_prepared_statement_nparams(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    return rb_int2big(duckdb_nparams(ctx->prepared_statement));
}


static VALUE duckdb_prepared_statement_execute(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    rubyDuckDBResult *ctxr;
    VALUE result = create_result();

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    Data_Get_Struct(result, rubyDuckDBResult, ctxr);
    if (duckdb_execute_prepared(ctx->prepared_statement, &(ctxr->result)) == DuckDBError) {
        rb_raise(eDuckDBError, "%s", duckdb_result_error(&(ctxr->result)));
    }
    return result;
}

static idx_t check_index(VALUE vidx) {
    idx_t idx = FIX2INT(vidx);
    if (idx <= 0) {
        rb_raise(rb_eArgError, "index of parameter must be greater than 0");
    }
    return idx;
}

static VALUE duckdb_prepared_statement_bind_bool(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (val != Qtrue && val != Qfalse) {
        rb_raise(rb_eArgError, "binding value must be boolean");
    }

    if (duckdb_bind_boolean(ctx->prepared_statement, idx, (val == Qtrue)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int8(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int8_t i8val = (int8_t)NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_int8(ctx->prepared_statement, idx, i8val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int16(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int16_t i16val = NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_int16(ctx->prepared_statement, idx, i16val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int32(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int32_t i32val = NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_int32(ctx->prepared_statement, idx, i32val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int64(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int64_t i64val = NUM2LL(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_int64(ctx->prepared_statement, idx, i64val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_float(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    double dbl = NUM2DBL(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_float(ctx->prepared_statement, idx, (float)dbl) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_double(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    double dbl = NUM2DBL(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_double(ctx->prepared_statement, idx, dbl) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_varchar(VALUE self, VALUE vidx, VALUE str) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_varchar(ctx->prepared_statement, idx, StringValuePtr(str)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_blob(VALUE self, VALUE vidx, VALUE blob) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_blob(ctx->prepared_statement, idx, (const void *)StringValuePtr(blob), (idx_t)RSTRING_LEN(blob)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_null(VALUE self, VALUE vidx) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_null(ctx->prepared_statement, idx) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

#ifdef HAVE_DUCKDB_BIND_DATE
static VALUE duckdb_prepared_statement__bind_date(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day) {
    rubyDuckDBPreparedStatement *ctx;
    duckdb_date dt;
    idx_t idx = check_index(vidx);

    dt = to_duckdb_date_from_value(year, month, day);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_date(ctx->prepared_statement, idx, dt) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}
#endif

#ifdef HAVE_DUCKDB_BIND_TIME
static VALUE duckdb_prepared_statement__bind_time(VALUE self, VALUE vidx, VALUE hour, VALUE min, VALUE sec, VALUE micros){
    rubyDuckDBPreparedStatement *ctx;
    duckdb_time time;

    idx_t idx = check_index(vidx);

    time = to_duckdb_time_from_value(hour, min, sec, micros);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_time(ctx->prepared_statement, idx, time) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}
#endif

static VALUE duckdb_prepared_statement__bind_timestamp(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_timestamp timestamp;
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    timestamp = to_duckdb_timestamp_from_value(year, month, day, hour, min, sec, micros);
    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_timestamp(ctx->prepared_statement, idx, timestamp) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement__bind_interval(VALUE self, VALUE vidx, VALUE months, VALUE days, VALUE micros) {
    duckdb_interval interval;
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    to_duckdb_interval_from_value(&interval, months, days, micros);

    if (duckdb_bind_interval(ctx->prepared_statement, idx, interval) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

void init_duckdb_prepared_statement(void) {
    cDuckDBPreparedStatement = rb_define_class_under(mDuckDB, "PreparedStatement", rb_cObject);

    rb_define_alloc_func(cDuckDBPreparedStatement, allocate);

    rb_define_method(cDuckDBPreparedStatement, "initialize", duckdb_prepared_statement_initialize, 2);
    rb_define_method(cDuckDBPreparedStatement, "execute", duckdb_prepared_statement_execute, 0);
    rb_define_method(cDuckDBPreparedStatement, "nparams", duckdb_prepared_statement_nparams, 0);
    rb_define_method(cDuckDBPreparedStatement, "bind_bool", duckdb_prepared_statement_bind_bool, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_int8", duckdb_prepared_statement_bind_int8, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_int16", duckdb_prepared_statement_bind_int16, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_int32", duckdb_prepared_statement_bind_int32, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_int64", duckdb_prepared_statement_bind_int64, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_float", duckdb_prepared_statement_bind_float, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_double", duckdb_prepared_statement_bind_double, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_varchar", duckdb_prepared_statement_bind_varchar, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_blob", duckdb_prepared_statement_bind_blob, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_null", duckdb_prepared_statement_bind_null, 1);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_date", duckdb_prepared_statement__bind_date, 4);
#ifdef HAVE_DUCKDB_BIND_TIME
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_time", duckdb_prepared_statement__bind_time, 5);
#endif
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_timestamp", duckdb_prepared_statement__bind_timestamp, 8);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_interval", duckdb_prepared_statement__bind_interval, 4);
}
