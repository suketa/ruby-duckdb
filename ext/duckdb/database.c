#include "ruby-duckdb.h"

VALUE cDuckDBDatabase;

static void close_database(rubyDuckDB *p);
static void deallocate(void * ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
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

static VALUE duckdb_database_s_open(int argc, VALUE *argv, VALUE cDuckDBDatabase) {
    rubyDuckDB *ctx;
    VALUE obj;

    char *pfile = NULL;
    VALUE file = Qnil;

    rb_scan_args(argc, argv, "01", &file);

    if (!NIL_P(file)) {
        pfile = StringValuePtr(file);
    }

    obj = allocate(cDuckDBDatabase);
    TypedData_Get_Struct(obj, rubyDuckDB, &database_data_type, ctx);
    if (duckdb_open(pfile, &(ctx->db)) == DuckDBError) {
        rb_raise(eDuckDBError, "Failed to open database"); /* FIXME */
    }
    return obj;
}

static VALUE duckdb_database_s_open_ext(int argc, VALUE *argv, VALUE cDuckDBDatabase) {
    rubyDuckDB *ctx;
    VALUE obj;
    rubyDuckDBConfig *ctx_config;
    char *perror;

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
        if (duckdb_open_ext(pfile, &(ctx->db), ctx_config->config, &perror) == DuckDBError) {
            rb_raise(eDuckDBError, "Failed to open database %s", perror);
        }
    } else {
        if (duckdb_open(pfile, &(ctx->db)) == DuckDBError) {
            rb_raise(eDuckDBError, "Failed to open database"); /* FIXME */
        }
    }
    return obj;
}

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

void rbduckdb_init_duckdb_database(void) {
    cDuckDBDatabase = rb_define_class_under(mDuckDB, "Database", rb_cObject);
    rb_define_alloc_func(cDuckDBDatabase, allocate);
    rb_define_singleton_method(cDuckDBDatabase, "_open", duckdb_database_s_open, -1);
    rb_define_singleton_method(cDuckDBDatabase, "_open_ext", duckdb_database_s_open_ext, -1);
    rb_define_private_method(cDuckDBDatabase, "_connect", duckdb_database_connect, 0);
    rb_define_method(cDuckDBDatabase, "close", duckdb_database_close, 0);
}
