#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_CREATE_CONFIG

VALUE cDuckDBConfig;

static void deallocate(void *);
static VALUE allocate(VALUE klass);

static void deallocate(void * ctx)
{
    rubyDuckDBConfig *p = (rubyDuckDBConfig *)ctx;

    duckdb_destroy_config(&(p->config));
    xfree(p);
}

static VALUE allocate(VALUE klass)
{
    rubyDuckDBConfig *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBConfig));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

void init_duckdb_config(void) {
    cDuckDBConfig = rb_define_class_under(mDuckDB, "Config", rb_cObject);
    rb_define_alloc_func(cDuckDBConfig, allocate);
}

#endif

