#include "ruby-duckdb.h"

static void deallocate(void *ctx)
{
    rubyDuckDBConnection *p = (rubyDuckDBConnection *)ctx;

    duckdb_close(&(p->con));
    xfree(p);
}

static VALUE allocate(VALUE klass)
{
    rubyDuckDBConnection *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBConnection));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

void init_duckdb_connection(void)
{
    VALUE cDuckDBConnection = rb_define_class_under(mDuckDB, "Connection", rb_cObject);
    rb_define_alloc_func(cDuckDBConnection, allocate);
}

