#include "ruby-duckdb.h"

VALUE cDuckDBInstanceCache;

static void deallocate(void * ctx);
static void mark(void *ctx);
static VALUE allocate(VALUE klass);
static VALUE memoizable_path(VALUE vpath);
static VALUE find_cached_wrapper(rubyDuckDBInstanceCache *ctx, VALUE vpath);
static size_t memsize(const void *p);
static VALUE instance_cache_initialize(VALUE self);
static VALUE instance_cache_get_or_create(int argc, VALUE *argv, VALUE self);
static VALUE instance_cache_destroy(VALUE self);

static const rb_data_type_t instance_cache_data_type = {
    "DuckDB/InstanceCache",
    {mark, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBInstanceCache *p = (rubyDuckDBInstanceCache *)ctx;

    if (p->instance_cache) {
        duckdb_destroy_instance_cache(&(p->instance_cache));
    }
    xfree(p);
}

static void mark(void *ctx) {
    rubyDuckDBInstanceCache *p = (rubyDuckDBInstanceCache *)ctx;

    rb_gc_mark(p->wrappers);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBInstanceCache);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBInstanceCache *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBInstanceCache));
    ctx->wrappers = Qnil;
    return TypedData_Wrap_Struct(klass, &instance_cache_data_type, ctx);
}

/*
 * A path is memoizable only if two get_or_create calls on it share one DuckDB
 * instance. An absent, empty or ":memory:" path gets a fresh instance every
 * time, so those wrappers must stay distinct. Returns the path or Qnil.
 */
static VALUE memoizable_path(VALUE vpath) {
    if (NIL_P(vpath) || RSTRING_LEN(vpath) == 0) {
        return Qnil;
    }
    if (rb_str_cmp(vpath, rb_str_new_cstr(":memory:")) == 0) {
        return Qnil;
    }
    return vpath;
}

/*
 * The wrapper this cache already holds for vpath, or Qnil. Entries whose
 * wrapper has been collected are gone from the weak set already.
 */
static VALUE find_cached_wrapper(rubyDuckDBInstanceCache *ctx, VALUE vpath) {
    VALUE wrappers = rb_funcall(ctx->wrappers, rb_intern("keys"), 0);
    long i;

    for (i = 0; i < RARRAY_LEN(wrappers); i++) {
        VALUE wrapper = rb_ary_entry(wrappers, i);
        rubyDuckDB *db_ctx = rbduckdb_get_struct_database(wrapper);

        if (!NIL_P(db_ctx->cached_path) && rb_str_equal(db_ctx->cached_path, vpath) == Qtrue) {
            return wrapper;
        }
    }
    return Qnil;
}

static VALUE instance_cache_initialize(VALUE self) {
    rubyDuckDBInstanceCache *ctx;

    TypedData_Get_Struct(self, rubyDuckDBInstanceCache, &instance_cache_data_type, ctx);

    ctx->instance_cache = duckdb_create_instance_cache();
    if (ctx->instance_cache == NULL) {
        rb_raise(eDuckDBError, "Failed to create instance cache");
    }

    ctx->wrappers = rb_funcall(rb_const_get(rb_const_get(rb_cObject, rb_intern("ObjectSpace")),
                                            rb_intern("WeakMap")),
                               rb_intern("new"), 0);

    return self;
}

/* :nodoc: */
static VALUE instance_cache_get_or_create(int argc, VALUE *argv, VALUE self) {
    VALUE vpath = Qnil;
    VALUE vconfig = Qnil;
    VALUE memo_path;
    VALUE obj;
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
    /*
     * duckdb_get_or_create_from_cache hands back a fresh duckdb_database handle
     * over an instance it may already have returned before. Registered functions
     * live in that shared instance's catalog, but each wrapper retains them
     * separately, so a second wrapper's collection frees functions the first
     * one's connections can still resolve. One wrapper per instance is what the
     * catalog's lifetime actually is, so reuse the wrapper we already have.
     */
    memo_path = memoizable_path(vpath);
    if (!NIL_P(memo_path)) {
        VALUE cached = find_cached_wrapper(ctx, memo_path);

        if (!NIL_P(cached)) {
            /* Our existing wrapper keeps the instance alive; this handle is spare. */
            duckdb_close(&db);
            return cached;
        }
    }

    obj = rbduckdb_create_database_obj(db);
    if (!NIL_P(memo_path)) {
        rbduckdb_database_set_cache_entry(obj, memo_path, ctx->wrappers);
        rb_funcall(ctx->wrappers, rb_intern("[]="), 2, obj, Qtrue);
    }
    return obj;
}

static VALUE instance_cache_destroy(VALUE self) {
    rubyDuckDBInstanceCache *ctx;
    TypedData_Get_Struct(self, rubyDuckDBInstanceCache, &instance_cache_data_type, ctx);

    if (ctx->instance_cache) {
        duckdb_destroy_instance_cache(&(ctx->instance_cache));
        ctx->instance_cache = NULL;
    }

    return Qnil;
}

void rbduckdb_init_instance_cache(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBInstanceCache = rb_define_class_under(mDuckDB, "InstanceCache", rb_cObject);
    rb_define_method(cDuckDBInstanceCache, "initialize", instance_cache_initialize, 0);
    rb_define_method(cDuckDBInstanceCache, "get_or_create", instance_cache_get_or_create, -1);
    rb_define_method(cDuckDBInstanceCache, "destroy", instance_cache_destroy, 0);
    rb_define_alloc_func(cDuckDBInstanceCache, allocate);
}
