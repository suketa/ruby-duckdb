#include "ruby-duckdb.h"

VALUE cDuckDBPreparedStatement;

static void destroy_prepared_statement(rubyDuckDBPreparedStatement *p);
static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_prepared_statement_initialize(VALUE self, VALUE con, VALUE query);
static VALUE duckdb_prepared_statement_nparams(VALUE self);
static VALUE duckdb_prepared_statement_execute(VALUE self);
static VALUE duckdb_prepared_statement_destroy(VALUE self);
static idx_t check_index(VALUE vidx);

static VALUE duckdb_prepared_statement_bind_parameter_index(VALUE self, VALUE name);
static VALUE duckdb_prepared_statement_parameter_name(VALUE self, VALUE vidx);
static VALUE duckdb_prepared_statement_clear_bindings(VALUE self);
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
static VALUE duckdb_prepared_statement__statement_type(VALUE self);
static VALUE duckdb_prepared_statement__param_type(VALUE self, VALUE vidx);
static VALUE duckdb_prepared_statement__bind_date(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day);
static VALUE duckdb_prepared_statement__bind_time(VALUE self, VALUE vidx, VALUE hour, VALUE min, VALUE sec, VALUE micros);
static VALUE duckdb_prepared_statement__bind_timestamp(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros);
static VALUE duckdb_prepared_statement__bind_interval(VALUE self, VALUE vidx, VALUE months, VALUE days, VALUE micros);
static VALUE duckdb_prepared_statement__bind_hugeint(VALUE self, VALUE vidx, VALUE lower, VALUE upper);
static VALUE duckdb_prepared_statement__bind_decimal(VALUE self, VALUE vidx, VALUE lower, VALUE upper, VALUE width, VALUE scale);

static const rb_data_type_t prepared_statement_data_type = {
    "DuckDB/PreparedStatement",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void destroy_prepared_statement(rubyDuckDBPreparedStatement *p) {
    if (p->prepared_statement) {
        duckdb_destroy_prepare(&(p->prepared_statement));
    }
}

static void deallocate(void *ctx) {
    rubyDuckDBPreparedStatement *p = (rubyDuckDBPreparedStatement *)ctx;

    destroy_prepared_statement(p);
    // duckdb_destroy_prepare(&(p->prepared_statement));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBPreparedStatement *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBPreparedStatement));
    return TypedData_Wrap_Struct(klass, &prepared_statement_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBPreparedStatement);
}

VALUE rbduckdb_prepared_statement_new(duckdb_connection con, duckdb_extracted_statements extracted_statements, idx_t index) {
    VALUE obj;
    rubyDuckDBPreparedStatement *ctx;

    obj = allocate(cDuckDBPreparedStatement);

    TypedData_Get_Struct(obj, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_prepare_extracted_statement(con, extracted_statements, index, &(ctx->prepared_statement)) == DuckDBError) {
        const char *error = duckdb_prepare_error(ctx->prepared_statement);
        rb_raise(eDuckDBError, "%s", error ? error : "Failed to create DuckDB::PreparedStatement object.");
    }
    return obj;
}

static VALUE duckdb_prepared_statement_initialize(VALUE self, VALUE con, VALUE query) {
    rubyDuckDBConnection *ctxcon;
    rubyDuckDBPreparedStatement *ctx;

    duckdb_prepared_statement prepared_statement;

    if (!rb_obj_is_kind_of(con, cDuckDBConnection)) {
        rb_raise(rb_eTypeError, "1st argument should be instance of DackDB::Connection");
    }

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    ctxcon = get_struct_connection(con);

    if (duckdb_prepare(ctxcon->con, StringValuePtr(query), &(prepared_statement)) == DuckDBError) {
        const char *error = duckdb_prepare_error(prepared_statement);
        duckdb_destroy_prepare(&prepared_statement);
        rb_raise(eDuckDBError, "%s", error ? error : "Failed to prepare statement(Database connection closed?).");
    }
    ctx->prepared_statement = prepared_statement;
    return self;
}

static VALUE duckdb_prepared_statement_nparams(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    return ULL2NUM(duckdb_nparams(ctx->prepared_statement));
}

static VALUE duckdb_prepared_statement_execute(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    rubyDuckDBResult *ctxr;
    VALUE result = rbduckdb_create_result();
    const char *p = NULL;

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    ctxr = get_struct_result(result);
    if (duckdb_execute_prepared(ctx->prepared_statement, &(ctxr->result)) == DuckDBError) {
        p = duckdb_result_error(&(ctxr->result));
        if (p == NULL) {
            p = duckdb_prepare_error(ctx->prepared_statement);
        }
        rb_raise(eDuckDBError, "%s", p ? p : "Failed to execute prepared statement.");
    }
    return result;
}

/*
 * :nodoc:
 */
static VALUE duckdb_prepared_statement_destroy(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    destroy_prepared_statement(ctx);
    /*
    ctx->prepared_statement = NULL;
    */
    return Qnil;
}

static idx_t check_index(VALUE vidx) {
    idx_t idx = NUM2ULL(vidx);
    if (idx <= 0) {
        rb_raise(rb_eArgError, "index of parameter must be greater than 0");
    }
    return idx;
}

static VALUE duckdb_prepared_statement_bind_parameter_index(VALUE self, VALUE name) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx;

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_parameter_index(ctx->prepared_statement, &idx, StringValuePtr(name)) == DuckDBError) {;
        rb_raise(rb_eArgError, "parameter '%s' not found", StringValuePtr(name));
    }
    return ULL2NUM(idx);
}

static VALUE duckdb_prepared_statement_parameter_name(VALUE self, VALUE vidx) {
    rubyDuckDBPreparedStatement *ctx;
    VALUE vname;
    const char *name;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    name = duckdb_parameter_name(ctx->prepared_statement, idx);
    if (name == NULL) {
        rb_raise(eDuckDBError, "fail to get name of %llu parameter", (unsigned long long)idx);
    }
    vname = rb_str_new2(name);
    duckdb_free((void *)name);
    return vname;
}

/*
 *  call-seq:
 *    prepared_statement.clear_bindings -> DuckDB::PreparedStatement
 *
 *  clear all bindings of prepared statement.
 */
static VALUE duckdb_prepared_statement_clear_bindings(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_clear_bindings(ctx->prepared_statement) == DuckDBError) {
        const char *error = duckdb_prepare_error(ctx->prepared_statement);
        rb_raise(eDuckDBError, "fail to clear bindings. %s", error);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_bool(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
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

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_int8(ctx->prepared_statement, idx, i8val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int16(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int16_t i16val = NUM2INT(val);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_int16(ctx->prepared_statement, idx, i16val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int32(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int32_t i32val = NUM2INT(val);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_int32(ctx->prepared_statement, idx, i32val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int64(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int64_t i64val = NUM2LL(val);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_int64(ctx->prepared_statement, idx, i64val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_float(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    double dbl = NUM2DBL(val);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_float(ctx->prepared_statement, idx, (float)dbl) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_double(VALUE self, VALUE vidx, VALUE val) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    double dbl = NUM2DBL(val);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_double(ctx->prepared_statement, idx, dbl) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_varchar(VALUE self, VALUE vidx, VALUE str) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_varchar(ctx->prepared_statement, idx, StringValuePtr(str)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_blob(VALUE self, VALUE vidx, VALUE blob) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_blob(ctx->prepared_statement, idx, (const void *)StringValuePtr(blob), (idx_t)RSTRING_LEN(blob)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_null(VALUE self, VALUE vidx) {
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_null(ctx->prepared_statement, idx) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__statement_type(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    return INT2FIX(duckdb_prepared_statement_type(ctx->prepared_statement));
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__param_type(VALUE self, VALUE vidx) {
    rubyDuckDBPreparedStatement *ctx;
    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    return INT2FIX(duckdb_param_type(ctx->prepared_statement, NUM2ULL(vidx)));
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__bind_date(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day) {
    rubyDuckDBPreparedStatement *ctx;
    duckdb_date dt;
    idx_t idx = check_index(vidx);

    dt = rbduckdb_to_duckdb_date_from_value(year, month, day);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_date(ctx->prepared_statement, idx, dt) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }

    return self;
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__bind_time(VALUE self, VALUE vidx, VALUE hour, VALUE min, VALUE sec, VALUE micros){
    rubyDuckDBPreparedStatement *ctx;
    duckdb_time time;

    idx_t idx = check_index(vidx);

    time = rbduckdb_to_duckdb_time_from_value(hour, min, sec, micros);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_time(ctx->prepared_statement, idx, time) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }

    return self;
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__bind_timestamp(VALUE self, VALUE vidx, VALUE year, VALUE month, VALUE day, VALUE hour, VALUE min, VALUE sec, VALUE micros) {
    duckdb_timestamp timestamp;
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    timestamp = rbduckdb_to_duckdb_timestamp_from_value(year, month, day, hour, min, sec, micros);
    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    if (duckdb_bind_timestamp(ctx->prepared_statement, idx, timestamp) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__bind_interval(VALUE self, VALUE vidx, VALUE months, VALUE days, VALUE micros) {
    duckdb_interval interval;
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);

    rbduckdb_to_duckdb_interval_from_value(&interval, months, days, micros);

    if (duckdb_bind_interval(ctx->prepared_statement, idx, interval) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__bind_hugeint(VALUE self, VALUE vidx, VALUE lower, VALUE upper) {
    duckdb_hugeint hugeint;
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    hugeint.lower = NUM2ULL(lower);
    hugeint.upper = NUM2LL(upper);

    if (duckdb_bind_hugeint(ctx->prepared_statement, idx, hugeint) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }

    return self;
}

/* :nodoc: */
static VALUE duckdb_prepared_statement__bind_decimal(VALUE self, VALUE vidx, VALUE lower, VALUE upper, VALUE width, VALUE scale) {
    duckdb_hugeint hugeint;
    duckdb_decimal decimal;
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    hugeint.lower = NUM2ULL(lower);
    hugeint.upper = NUM2LL(upper);
    decimal.value = hugeint;
    decimal.width = (uint8_t)NUM2UINT(width);
    decimal.scale = (uint8_t)NUM2UINT(scale);

    if (duckdb_bind_decimal(ctx->prepared_statement, idx, decimal) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }

    return self;
}

rubyDuckDBPreparedStatement *get_struct_prepared_statement(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    TypedData_Get_Struct(self, rubyDuckDBPreparedStatement, &prepared_statement_data_type, ctx);
    return ctx;
}

void rbduckdb_init_duckdb_prepared_statement(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBPreparedStatement = rb_define_class_under(mDuckDB, "PreparedStatement", rb_cObject);

    rb_define_alloc_func(cDuckDBPreparedStatement, allocate);

    rb_define_method(cDuckDBPreparedStatement, "initialize", duckdb_prepared_statement_initialize, 2);
    rb_define_method(cDuckDBPreparedStatement, "execute", duckdb_prepared_statement_execute, 0);
    rb_define_method(cDuckDBPreparedStatement, "destroy", duckdb_prepared_statement_destroy, 0);
    rb_define_method(cDuckDBPreparedStatement, "nparams", duckdb_prepared_statement_nparams, 0);
    rb_define_method(cDuckDBPreparedStatement, "bind_parameter_index", duckdb_prepared_statement_bind_parameter_index, 1);
    rb_define_method(cDuckDBPreparedStatement, "parameter_name", duckdb_prepared_statement_parameter_name, 1);
    rb_define_method(cDuckDBPreparedStatement, "clear_bindings", duckdb_prepared_statement_clear_bindings, 0);
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
    rb_define_private_method(cDuckDBPreparedStatement, "_statement_type", duckdb_prepared_statement__statement_type, 0);
    rb_define_private_method(cDuckDBPreparedStatement, "_param_type", duckdb_prepared_statement__param_type, 1);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_date", duckdb_prepared_statement__bind_date, 4);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_time", duckdb_prepared_statement__bind_time, 5);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_timestamp", duckdb_prepared_statement__bind_timestamp, 8);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_interval", duckdb_prepared_statement__bind_interval, 4);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_hugeint", duckdb_prepared_statement__bind_hugeint, 3);
    rb_define_private_method(cDuckDBPreparedStatement, "_bind_decimal", duckdb_prepared_statement__bind_decimal, 5);
}
