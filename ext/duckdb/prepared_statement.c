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

static VALUE duckdb_prepared_statement_initialize(VALUE self, VALUE con, VALUE query) {

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

static VALUE duckdb_prepared_statement_nparams(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);

    if (duckdb_nparams(ctx->prepared_statement, &(ctx->nparams)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to get number of parameters");
    }
    return rb_int2big(ctx->nparams);
}


static VALUE duckdb_prepared_statement_execute(VALUE self) {
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

static VALUE duckdb_prepared_statement_bind_boolean(VALUE self, VALUE vidx, VALUE bval) {
    rubyDuckDBPreparedStatement *ctx;
    index_t idx = FIX2INT(vidx);
    if (idx <= 0) {
        rb_raise(rb_eArgError, "index of parameter must be greater than 0");
    }
    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (bval != Qtrue && bval != Qfalse) {
        rb_raise(rb_eArgError, "binding value must be boolean");
    }

    if (duckdb_bind_boolean(ctx->prepared_statement, idx, (bval == Qtrue)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %ld parameter", idx);
    }
    return self;
}

static VALUE duckdb_prepared_statement_bind_varchar(VALUE self, VALUE vidx, VALUE str) {
    rubyDuckDBPreparedStatement *ctx;
    index_t idx = FIX2INT(vidx);
    if (idx <= 0) {
        rb_raise(rb_eArgError, "index of parameter must be greater than 0");
    }
    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    if (duckdb_bind_varchar(ctx->prepared_statement, idx, StringValuePtr(str)) == DuckDBError) {
        rb_raise(eDuckDBError, "fail to bind %ld parameter", idx);
    }
    return self;
}

void init_duckdb_prepared_statement(void) {
    cDuckDBPreparedStatement = rb_define_class_under(mDuckDB, "PreparedStatement", rb_cObject);
    rb_define_alloc_func(cDuckDBPreparedStatement, allocate);
    rb_define_method(cDuckDBPreparedStatement, "initialize", duckdb_prepared_statement_initialize, 2);
    rb_define_method(cDuckDBPreparedStatement, "execute", duckdb_prepared_statement_execute, 0);
    rb_define_method(cDuckDBPreparedStatement, "nparams", duckdb_prepared_statement_nparams, 0);
    rb_define_method(cDuckDBPreparedStatement, "bind_boolean", duckdb_prepared_statement_bind_boolean, 2);
    rb_define_method(cDuckDBPreparedStatement, "bind_varchar", duckdb_prepared_statement_bind_varchar, 2);
}
