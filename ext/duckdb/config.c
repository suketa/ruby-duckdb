#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_CREATE_CONFIG

VALUE cDuckDBConfig;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static VALUE config_initialize(VALUE self);
static VALUE config_s_size(VALUE klass);

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

static VALUE config_initialize(VALUE self) {
    rubyDuckDBConfig *ctx;

    Data_Get_Struct(self, rubyDuckDBConfig, ctx);

    if (duckdb_create_config(&(ctx->config)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to create config");
    }
    return self;
}

static VALUE config_s_size(VALUE self) {
    return INT2NUM(duckdb_config_count());
}

void init_duckdb_config(void) {
    cDuckDBConfig = rb_define_class_under(mDuckDB, "Config", rb_cObject);
    rb_define_alloc_func(cDuckDBConfig, allocate);
    rb_define_singleton_method(cDuckDBConfig, "size", config_s_size, 0);

    rb_define_method(cDuckDBConfig, "initialize", config_initialize, 0);
}

#endif

