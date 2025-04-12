#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_H_GE_V1_2_0
VALUE cDuckDBInstanceCache;

static void deallocate(void * ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_instance_cache_initialize(VALUE self);
static VALUE duckdb_instance_cache_get_or_create(int argc, VALUE *argv, VALUE self);
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

static VALUE duckdb_instance_cache_get_or_create(int argc, VALUE *argv, VALUE self) {
    VALUE vpath = Qnil;
    VALUE vconfig = Qnil;
    const char *path = NULL;
    char *error = NULL;
    duckdb_config config = NULL;
    duckdb_database db;
    rubyDuckDBInstanceCache *ctx;

    rb_scan_args(argc, argv, "02", &vpath, &vconfig);
    if (!NIL_P(vpath)) {
        path = StringValuePtr(vpath);
    }
    if (!NIL_P(vconfig)) {
        if (!rb_obj_is_kind_of(vconfig, cDuckDBConfig)) {
            rb_raise(rb_eTypeError, "The second argument must be DuckDB::Config object.");
        }
        rubyDuckDBConfig *ctx_config = get_struct_config(vconfig);
        config = ctx_config->config;
    }

    TypedData_Get_Struct(self, rubyDuckDBInstanceCache, &instance_cache_data_type, ctx);

    if (duckdb_get_or_create_from_cache(ctx->instance_cache, path, &db, config, &error) == DuckDBError) {
        if (error) {
            VALUE message = rb_str_new_cstr(error);
            duckdb_free(error);
            rb_raise(eDuckDBError, "%s", StringValuePtr(message));
        } else {
            rb_raise(eDuckDBError, "Failed to get or create database from instance cache");
        }
    }
    return rbduckdb_create_database_obj(db);
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
    rb_define_method(cDuckDBInstanceCache, "get_or_create", duckdb_instance_cache_get_or_create, -1);
    rb_define_method(cDuckDBInstanceCache, "destroy", duckdb_instance_cache_destroy, 0);
    rb_define_alloc_func(cDuckDBInstanceCache, allocate);
}
#endif
