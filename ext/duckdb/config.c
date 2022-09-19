#include "ruby-duckdb.h"

VALUE cDuckDBConfig;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE config_s_size(VALUE klass);
static VALUE config_s_get_config_flag(VALUE self, VALUE value);
static VALUE config_initialize(VALUE self);
static VALUE config_set_config(VALUE self, VALUE key, VALUE value);

static const rb_data_type_t config_data_type = {
    "DuckDB/Config",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBConfig *p = (rubyDuckDBConfig *)ctx;

    duckdb_destroy_config(&(p->config));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBConfig *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBConfig));
    return TypedData_Wrap_Struct(klass, &config_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBConfig);
}

rubyDuckDBConfig *get_struct_config(VALUE obj) {
    rubyDuckDBConfig *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBConfig, &config_data_type, ctx);
    return ctx;
}

static VALUE config_initialize(VALUE self) {
    rubyDuckDBConfig *ctx;

    TypedData_Get_Struct(self, rubyDuckDBConfig, &config_data_type, ctx);

    if (duckdb_create_config(&(ctx->config)) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to create config");
    }
    return self;
}

static VALUE config_s_size(VALUE self) {
    return ULONG2NUM(duckdb_config_count());
}

static VALUE config_s_get_config_flag(VALUE klass, VALUE value) {
    const char *pkey;
    const char *pdesc;

    size_t i = NUM2INT(value);

    if (duckdb_get_config_flag(i, &pkey, &pdesc) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to get config information of index %ld", i);
    }

    return rb_ary_new3(2, rb_utf8_str_new_cstr(pkey), rb_utf8_str_new_cstr(pdesc));
}

static VALUE config_set_config(VALUE self, VALUE key, VALUE value) {
    const char *pkey = StringValuePtr(key);
    const char *pval = StringValuePtr(value);

    rubyDuckDBConfig *ctx;
    TypedData_Get_Struct(self, rubyDuckDBConfig, &config_data_type, ctx);

    if (duckdb_set_config(ctx->config, pkey, pval) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to set config %s => %s", pkey, pval);
    }
    return self;
}

void init_duckdb_config(void) {
    cDuckDBConfig = rb_define_class_under(mDuckDB, "Config", rb_cObject);
    rb_define_alloc_func(cDuckDBConfig, allocate);
    rb_define_singleton_method(cDuckDBConfig, "size", config_s_size, 0);
    rb_define_singleton_method(cDuckDBConfig, "get_config_flag", config_s_get_config_flag, 1);

    rb_define_method(cDuckDBConfig, "initialize", config_initialize, 0);
    rb_define_method(cDuckDBConfig, "set_config", config_set_config, 2);
}
