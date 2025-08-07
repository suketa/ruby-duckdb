#include "ruby-duckdb.h"

VALUE cDuckDBDatabase;

static void close_database(rubyDuckDB *p);
static void deallocate(void * ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static duckdb_config create_config_with_ruby_api(void);
static VALUE duckdb_database_s_open(int argc, VALUE *argv, VALUE cDuckDBDatabase);
static VALUE duckdb_database_s_open_ext(int argc, VALUE *argv, VALUE cDuckDBDatabase);
static VALUE duckdb_database_connect(VALUE self);
static VALUE duckdb_database_close(VALUE self);

static const rb_data_type_t database_data_type = {
    "DuckDB/Database",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void close_database(rubyDuckDB *p) {
    duckdb_close(&(p->db));
}

static void deallocate(void * ctx) {
    rubyDuckDB *p = (rubyDuckDB *)ctx;

    close_database(p);
    xfree(p);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDB);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDB *ctx = xcalloc((size_t)1, sizeof(rubyDuckDB));
    return TypedData_Wrap_Struct(klass, &database_data_type, ctx);
}

rubyDuckDB *rbduckdb_get_struct_database(VALUE obj) {
    rubyDuckDB *ctx;
    TypedData_Get_Struct(obj, rubyDuckDB, &database_data_type, ctx);
    return ctx;
}

static duckdb_config create_config_with_ruby_api(void) {
    duckdb_config config;

    if (duckdb_create_config(&config) == DuckDBError) {
        rb_raise(eDuckDBError, "failed to create config");
    }

    if (duckdb_set_config(config, "duckdb_api", "ruby") == DuckDBError) {
        duckdb_destroy_config(&config);
        rb_raise(eDuckDBError, "failed to set duckdb_api config");
    }

    return config;
}

/* :nodoc: */
static VALUE duckdb_database_s_open(int argc, VALUE *argv, VALUE cDuckDBDatabase) {
    rubyDuckDB *ctx;
    VALUE obj;
    duckdb_config config;
    char *perror = NULL;

    char *pfile = NULL;
    VALUE file = Qnil;

    rb_scan_args(argc, argv, "01", &file);

    if (!NIL_P(file)) {
        pfile = StringValuePtr(file);
    }

    obj = allocate(cDuckDBDatabase);
    TypedData_Get_Struct(obj, rubyDuckDB, &database_data_type, ctx);

    config = create_config_with_ruby_api();

    if (duckdb_open_ext(pfile, &(ctx->db), config, &perror) == DuckDBError) {
        VALUE error_msg = rb_str_new_cstr(perror ? perror : "Unknown error");
        if (perror) {
            duckdb_free(perror);
        }
        duckdb_destroy_config(&config);
        rb_raise(eDuckDBError, "failed to open database: %s", StringValueCStr(error_msg));
    }

    duckdb_destroy_config(&config);
    return obj;
}

/* :nodoc: */
static VALUE duckdb_database_s_open_ext(int argc, VALUE *argv, VALUE cDuckDBDatabase) {
    rubyDuckDB *ctx;
    VALUE obj;
    rubyDuckDBConfig *ctx_config;
    duckdb_config config_to_use;
    char *perror = NULL;
    int need_destroy_config = 0;

    char *pfile = NULL;
    VALUE file = Qnil;
    VALUE config = Qnil;

    rb_scan_args(argc, argv, "02", &file, &config);

    if (!NIL_P(file)) {
        pfile = StringValuePtr(file);
    }

    obj = allocate(cDuckDBDatabase);
    TypedData_Get_Struct(obj, rubyDuckDB, &database_data_type, ctx);

    if (!NIL_P(config)) {
        if (!rb_obj_is_kind_of(config, cDuckDBConfig)) {
            rb_raise(rb_eTypeError, "The second argument must be DuckDB::Config object.");
        }
        ctx_config = get_struct_config(config);
        /* Set duckdb_api to "ruby" for the provided config */
        if (duckdb_set_config(ctx_config->config, "duckdb_api", "ruby") == DuckDBError) {
            rb_raise(eDuckDBError, "failed to set duckdb_api config");
        }
        config_to_use = ctx_config->config;
    } else {
        config_to_use = create_config_with_ruby_api();
        need_destroy_config = 1;
    }

    if (duckdb_open_ext(pfile, &(ctx->db), config_to_use, &perror) == DuckDBError) {
        VALUE error_msg = rb_str_new_cstr(perror ? perror : "Unknown error");
        if (perror) {
            duckdb_free(perror);
        }
        if (need_destroy_config) {
            duckdb_destroy_config(&config_to_use);
        }
        rb_raise(eDuckDBError, "failed to open database: %s", StringValueCStr(error_msg));
    }

    if (need_destroy_config) {
        duckdb_destroy_config(&config_to_use);
    }

    return obj;
}

/* :nodoc: */
static VALUE duckdb_database_connect(VALUE self) {
    return rbduckdb_create_connection(self);
}

/*
 *  call-seq:
 *    duckdb.close -> DuckDB::Database
 *
 *  closes DuckDB database.
 */
static VALUE duckdb_database_close(VALUE self) {
    rubyDuckDB *ctx;
    TypedData_Get_Struct(self, rubyDuckDB, &database_data_type, ctx);
    close_database(ctx);
    return self;
}

VALUE rbduckdb_create_database_obj(duckdb_database db) {
    VALUE obj = allocate(cDuckDBDatabase);
    rubyDuckDB *ctx;
    TypedData_Get_Struct(obj, rubyDuckDB, &database_data_type, ctx);
    ctx->db = db;
    return obj;
}

void rbduckdb_init_duckdb_database(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBDatabase = rb_define_class_under(mDuckDB, "Database", rb_cObject);
    rb_define_alloc_func(cDuckDBDatabase, allocate);
    rb_define_singleton_method(cDuckDBDatabase, "_open", duckdb_database_s_open, -1);
    rb_define_singleton_method(cDuckDBDatabase, "_open_ext", duckdb_database_s_open_ext, -1);
    rb_define_private_method(cDuckDBDatabase, "_connect", duckdb_database_connect, 0);
    rb_define_method(cDuckDBDatabase, "close", duckdb_database_close, 0);
}
