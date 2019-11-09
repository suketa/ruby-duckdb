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

static VALUE duckdb_prepared_statement_execute(VALUE self) {
    rubyDuckDBPreparedStatement *ctx;
    rubyDuckDBResult *ctxr;
    VALUE result = create_result();

    Data_Get_Struct(self, rubyDuckDBPreparedStatement, ctx);
    Data_Get_Struct(result, rubyDuckDBResult, ctxr);
    if (duckdb_execute_prepared(ctx->prepared_statement, &(ctxr->result)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to execute statement");
    }
    return result;
}

void init_duckdb_prepared_statement(void) {
    cDuckDBPreparedStatement = rb_define_class_under(mDuckDB, "PreparedStatement", rb_cObject);
    rb_define_alloc_func(cDuckDBPreparedStatement, allocate);
    rb_define_method(cDuckDBPreparedStatement, "initialize", duckdb_prepared_statement_initialize, 2);
    rb_define_method(cDuckDBPreparedStatement, "execute", duckdb_prepared_statement_execute, 0);
}
