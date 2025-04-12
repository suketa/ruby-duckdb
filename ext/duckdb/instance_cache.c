#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_H_GE_V1_2_0
VALUE cDuckDBInstanceCache;

static void deallocate(void * ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_instance_cache_initialize(VALUE self);
static VALUE duckdb_instance_cache_destroy(VALUE self);

static const rb_data_type_t instance_cache_data_type = {
    "DuckDB/InstanceCache",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBInstanceCache *p = (rubyDuckDBInstanceCache *)ctx;

    if (p->instance_cache) {
        duckdb_destroy_instance_cache(&(p->instance_cache));
    }
    xfree(p);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBInstanceCache);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBInstanceCache *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBInstanceCache));
    return TypedData_Wrap_Struct(klass, &instance_cache_data_type, ctx);
}

static VALUE duckdb_instance_cache_initialize(VALUE self) {
    rubyDuckDBInstanceCache *ctx;

    TypedData_Get_Struct(self, rubyDuckDBInstanceCache, &instance_cache_data_type, ctx);

    ctx->instance_cache = duckdb_create_instance_cache();
    if (ctx->instance_cache == NULL) {
        rb_raise(eDuckDBError, "Failed to create instance cache");
    }

    return self;
}

static VALUE duckdb_instance_cache_destroy(VALUE self) {
    rubyDuckDBInstanceCache *ctx;
    TypedData_Get_Struct(self, rubyDuckDBInstanceCache, &instance_cache_data_type, ctx);

    if (ctx->instance_cache) {
        duckdb_destroy_instance_cache(&(ctx->instance_cache));
        ctx->instance_cache = NULL;
    }

    return Qnil;
}

void rbduckdb_init_duckdb_instance_cache(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBInstanceCache = rb_define_class_under(mDuckDB, "InstanceCache", rb_cObject);
    rb_define_method(cDuckDBInstanceCache, "initialize", duckdb_instance_cache_initialize, 0);
    rb_define_method(cDuckDBInstanceCache, "destroy", duckdb_instance_cache_destroy, 0);
    rb_define_alloc_func(cDuckDBInstanceCache, allocate);
}
#endif
