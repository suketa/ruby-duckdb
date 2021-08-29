#include "ruby-duckdb.h"

VALUE cDuckDBDatabase;

static void close_database(rubyDuckDB *p)
{
    duckdb_close(&(p->db));
}

static void deallocate(void * ctx)
{
    rubyDuckDB *p = (rubyDuckDB *)ctx;

    close_database(p);
    xfree(p);
}

static VALUE allocate(VALUE klass)
{
    rubyDuckDB *ctx = xcalloc((size_t)1, sizeof(rubyDuckDB));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

static VALUE duckdb_database_s_open(int argc, VALUE *argv, VALUE cDuckDBDatabase)
{
    rubyDuckDB *ctx;
    VALUE obj;
#ifdef HAVE_DUCKDB_OPEN_EXT
    rubyDuckDBConfig *ctx_config;
    char *perror;
#endif

    char *pfile = NULL;
    VALUE file = Qnil;
#ifdef HAVE_DUCKDB_OPEN_EXT
    VALUE config = Qnil;
#endif

#ifdef HAVE_DUCKDB_OPEN_EXT
    rb_scan_args(argc, argv, "02", &file, &config);
#else
    rb_scan_args(argc, argv, "01", &file);
#endif

    if (!NIL_P(file)) {
        pfile = StringValuePtr(file);
    }

    obj = allocate(cDuckDBDatabase);
    Data_Get_Struct(obj, rubyDuckDB, ctx);
#ifdef HAVE_DUCKDB_OPEN_EXT
    if (!NIL_P(config)) {
        if (!rb_obj_is_kind_of(config, cDuckDBConfig)) {
            rb_raise(rb_eTypeError, "The second argument must be DuckDB::Config object.");
        }
        Data_Get_Struct(config, rubyDuckDBConfig, ctx_config);
        if (duckdb_open_ext(pfile, &(ctx->db), ctx_config->config, &perror) == DuckDBError) {
            rb_raise(eDuckDBError, "Failed to open database %s", perror);
        }
    } else {
        if (duckdb_open(pfile, &(ctx->db)) == DuckDBError) {
            rb_raise(eDuckDBError, "Failed to open database"); /* FIXME */
        }
    }
#else
    if (duckdb_open(pfile, &(ctx->db)) == DuckDBError) {
        rb_raise(eDuckDBError, "Failed to open database"); /* FIXME */
    }
#endif /* HAVE_DUCKDB_OPEN_EXT */
    return obj;
}

static VALUE duckdb_database_connect(VALUE self)
{
    return create_connection(self);
}

/*
 *  call-seq:
 *    duckdb.close -> DuckDB::Database
 *
 *  closes DuckDB database.
 */
static VALUE duckdb_database_close(VALUE self)
{
    rubyDuckDB *ctx;
    Data_Get_Struct(self, rubyDuckDB, ctx);
    close_database(ctx);
    return self;
}

void init_duckdb_database(void)
{
    cDuckDBDatabase = rb_define_class_under(mDuckDB, "Database", rb_cObject);
    rb_define_alloc_func(cDuckDBDatabase, allocate);
    rb_define_singleton_method(cDuckDBDatabase, "_open", duckdb_database_s_open, -1);
    rb_define_private_method(cDuckDBDatabase, "_connect", duckdb_database_connect, 0);
    rb_define_method(cDuckDBDatabase, "close", duckdb_database_close, 0);
}
