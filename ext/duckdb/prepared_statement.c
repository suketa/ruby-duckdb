#include "ruby-duckdb.h"

static VALUE cDuckDBPreparedStatement;

static void deallocate(void *ctx)
{
    rubyDuckDBPreparedStatement *p = (rubyDuckDBPreparedStatement *)ctx;

    duckdb_destroy_prepare(&(p->prepared_statement));
    xfree(p);
}

static VALUE allocate(VALUE klass)
{
    rubyDuckDBPreparedStatement *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBPreparedStatement));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

static VALUE duckdb_prepared_statement_initialize(VALUE self, VALUE con, VALUE query)
{
    rubyDuckDBConnection *ctxcon;
    rubyDuckDBPreparedStatement *ctx;

    if (!rb_obj_is_kind_of(con, cDuckDBConnection)) {
        rb_raise(rb_eTypeError, "1st argument should be instance of DackDB::Connection");
    }

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    Data_Get_Struct(con, rubyDuckDBConnection, ctxcon);

    if (duckdb_prepare(ctxcon->con, StringValuePtr(query), &(ctx->prepared_statement)) == DuckDBError) {
        /* TODO: include query parameter information in error message. */
        rb_raise(eDuckDBError, "failed to prepare statement");
    }
    return self;
}

static VALUE duckdb_prepared_statement_nparams(VALUE self)
{
    rubyDuckDBPreparedStatement *ctx;
    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_nparams(ctx->prepared_statement, &(ctx->nparams)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to get number of parameters");
    }
    return rb_int2big(ctx->nparams);
}


static VALUE duckdb_prepared_statement_execute(VALUE self)
{
    rubyDuckDBPreparedStatement *ctx;
    rubyDuckDBResult *ctxr;
    VALUE result = create_result();

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    Data_Get_Struct(result, rubyDuckDBResult, ctxr);
    if (duckdb_execute_prepared(ctx->prepared_statement, &(ctxr->result)) == DuckDBError) {
        rb_raise(eDuckDBError, "%s", ctxr->result.error_message);
    }
    return result;
}

static idx_t check_index(VALUE vidx)
{
    idx_t idx = FIX2INT(vidx);
    if (idx <= 0) {
        rb_raise(rb_eArgError, "index of parameter must be greater than 0");
    }
    return idx;
}

static VALUE duckdb_prepared_statement_bind_boolean(VALUE self, VALUE vidx, VALUE val)
{
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

static VALUE duckdb_prepared_statement_bind_int16(VALUE self, VALUE vidx, VALUE val)
{
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int16_t i16val = NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_int16(ctx->prepared_statement, idx, i16val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int32(VALUE self, VALUE vidx, VALUE val)
{
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int32_t i32val = NUM2INT(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_int32(ctx->prepared_statement, idx, i32val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_int64(VALUE self, VALUE vidx, VALUE val)
{
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    int64_t i64val = NUM2LL(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_int64(ctx->prepared_statement, idx, i64val) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_float(VALUE self, VALUE vidx, VALUE val)
{
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    double dbl = NUM2DBL(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_float(ctx->prepared_statement, idx, (float)dbl) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_double(VALUE self, VALUE vidx, VALUE val)
{
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);
    double dbl = NUM2DBL(val);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_bind_double(ctx->prepared_statement, idx, dbl) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_varchar(VALUE self, VALUE vidx, VALUE str)
{
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_varchar(ctx->prepared_statement, idx, StringValuePtr(str)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_null(VALUE self, VALUE vidx)
{
    rubyDuckDBPreparedStatement *ctx;
    idx_t idx = check_index(vidx);

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_null(ctx->prepared_statement, idx) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %llu parameter", (unsigned long long)idx);
    }
    return self;
}

void init_duckdb_prepared_statement(void)
{
    cDuckDBPreparedStatement = rb_define_class_under(mDuckDB, "PreparedStatement", rb_cObject);

    rb_define_alloc_func(cDuckDBPreparedStatement, allocate);

    rb_define_method(cDuckDBPreparedStatement, "initialize", duckdb_prepared_statement_initialize, 2);
    rb_define_method(cDuckDBPreparedStatement, "execute", duckdb_prepared_statement_execute, 0);
    rb_define_method(cDuckDBPreparedStatement, "nparams", duckdb_prepared_statement_nparams, 0);
    rb_define_method(cDuckDBPreparedStatement, "bind_boolean", duckdb_prepared_statement_bind_boolean, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_int16", duckdb_prepared_statement_bind_int16, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_int32", duckdb_prepared_statement_bind_int32, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_int64", duckdb_prepared_statement_bind_int64, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_float", duckdb_prepared_statement_bind_float, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_double", duckdb_prepared_statement_bind_double, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_varchar", duckdb_prepared_statement_bind_varchar, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_null", duckdb_prepared_statement_bind_null, 1);
}
