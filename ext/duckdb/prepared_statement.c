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

void init_duckdb_prepared_statement(void) {
    cDuckDBPreparedStatement = rb_define_class_under(mDuckDB, "PreparedStatement", rb_cObject);
    rb_define_alloc_func(cDuckDBPreparedStatement, allocate);
}
