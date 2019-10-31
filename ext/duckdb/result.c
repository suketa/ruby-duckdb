#include "ruby-duckdb.h"

static VALUE cDuckDBResult;

static void deallocate(void *ctx)
{
    rubyDuckDBResult *p = (rubyDuckDBResult *)ctx;

    duckdb_destroy_result(&(p->result));
    xfree(p);
}

static VALUE allocate(VALUE klass)
{
    rubyDuckDBResult *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBResult));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

VALUE create_result(void) {
    return allocate(cDuckDBResult);
}

void init_duckdb_result(void)
{
    cDuckDBResult = rb_define_class_under(mDuckDB, "Result", rb_cObject);
    rb_define_alloc_func(cDuckDBResult, allocate);
}

