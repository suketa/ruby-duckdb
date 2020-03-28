#include "ruby-duckdb.h"

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
    char *pfile = NULL;
    VALUE file = Qnil;

    rb_scan_args(argc, argv, "01", &file);

    if (!NIL_P(file)) {
        pfile = StringValuePtr(file);
    }

    obj = allocate(cDuckDBDatabase);
    Data_Get_Struct(obj, rubyDuckDB, ctx);
    if (duckdb_open(pfile, &(ctx->db)) == DuckDBError)
    {
        rb_raise(eDuckDBError, "Failed to open database"); /* FIXME */
    }
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
