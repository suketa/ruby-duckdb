#include "ruby-duckdb.h"

VALUE cDuckDBConnection;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_connection_disconnect(VALUE self);
static VALUE duckdb_connection_interrupt(VALUE self);
static VALUE duckdb_connection_query_progress(VALUE self);
static VALUE duckdb_connection_connect(VALUE self, VALUE oDuckDBDatabase);
static VALUE duckdb_connection_query_sql(VALUE self, VALUE str);

static const rb_data_type_t connection_data_type = {
    "DuckDB/Connection",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBConnection *p = (rubyDuckDBConnection *)ctx;

    duckdb_disconnect(&(p->con));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBConnection *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBConnection));
    return TypedData_Wrap_Struct(klass, &connection_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBConnection);
}

rubyDuckDBConnection *get_struct_connection(VALUE obj) {
    rubyDuckDBConnection *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBConnection, &connection_data_type, ctx);
    return ctx;
}

VALUE rbduckdb_create_connection(VALUE oDuckDBDatabase) {
    rubyDuckDB *ctxdb;
    rubyDuckDBConnection *ctxcon;
    VALUE obj;

    ctxdb = rbduckdb_get_struct_database(oDuckDBDatabase);

    obj = allocate(cDuckDBConnection);
    TypedData_Get_Struct(obj, rubyDuckDBConnection, &connection_data_type, ctxcon);

    if (duckdb_connect(ctxdb->db, &(ctxcon->con)) == DuckDBError) {
        rb_raise(eDuckDBError, "connection error");
    }

    return obj;
}

static VALUE duckdb_connection_disconnect(VALUE self) {
    rubyDuckDBConnection *ctx;

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    duckdb_disconnect(&(ctx->con));

    return self;
}

/*
 * call-seq:
 *   connection.interrupt -> nil
 *
 * Interrupts the currently running query.
 *
 *  db = DuckDB::Database.open
 *  conn = db.connect
 *  con.query('SET ENABLE_PROGRESS_BAR=true')
 *  con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')
 *  pending_result = con.async_query('slow query')
 *
 *  pending_result.execute_task
 *  con.interrupt # => nil
 */
static VALUE duckdb_connection_interrupt(VALUE self) {
    rubyDuckDBConnection *ctx;

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    duckdb_interrupt(ctx->con);

    return Qnil;
}

/*
 * Returns the progress of the currently running query.
 *
 *  require 'duckdb'
 *
 *  db = DuckDB::Database.open
 *  conn = db.connect
 *  con.query('SET ENABLE_PROGRESS_BAR=true')
 *  con.query('SET ENABLE_PROGRESS_BAR_PRINT=false')
 *  con.query_progress # => -1.0
 *  pending_result = con.async_query('slow query')
 *  con.query_progress # => 0.0
 *  pending_result.execute_task
 *  con.query_progress # => Float
 */
static VALUE duckdb_connection_query_progress(VALUE self) {
    rubyDuckDBConnection *ctx;
    duckdb_query_progress_type progress;

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    progress = duckdb_query_progress(ctx->con);

    return rb_funcall(mDuckDBConverter, rb_intern("_to_query_progress"), 3, DBL2NUM(progress.percentage), ULL2NUM(progress.rows_processed), ULL2NUM(progress.total_rows_to_process));
}

static VALUE duckdb_connection_connect(VALUE self, VALUE oDuckDBDatabase) {
    rubyDuckDBConnection *ctx;
    rubyDuckDB *ctxdb;

    if (!rb_obj_is_kind_of(oDuckDBDatabase, cDuckDBDatabase)) {
        rb_raise(rb_eTypeError, "The first argument must be DuckDB::Database object.");
    }
    ctxdb = rbduckdb_get_struct_database(oDuckDBDatabase);
    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);

    if (duckdb_connect(ctxdb->db, &(ctx->con)) == DuckDBError) {
        rb_raise(eDuckDBError, "connection error");
    }

    return self;
}

static VALUE duckdb_connection_query_sql(VALUE self, VALUE str) {
    rubyDuckDBConnection *ctx;
    rubyDuckDBResult *ctxr;

    VALUE result = rbduckdb_create_result();

    TypedData_Get_Struct(self, rubyDuckDBConnection, &connection_data_type, ctx);
    ctxr = get_struct_result(result);

    if (!(ctx->con)) {
        rb_raise(eDuckDBError, "Database connection closed");
    }

    if (duckdb_query(ctx->con, StringValueCStr(str), &(ctxr->result)) == DuckDBError) {
        rb_raise(eDuckDBError, "%s", duckdb_result_error(&(ctxr->result)));
    }
    return result;
}

void rbduckdb_init_duckdb_connection(void) {
    cDuckDBConnection = rb_define_class_under(mDuckDB, "Connection", rb_cObject);
    rb_define_alloc_func(cDuckDBConnection, allocate);

    rb_define_method(cDuckDBConnection, "disconnect", duckdb_connection_disconnect, 0);
    rb_define_method(cDuckDBConnection, "interrupt", duckdb_connection_interrupt, 0);
    rb_define_method(cDuckDBConnection, "query_progress", duckdb_connection_query_progress, 0);
    rb_define_private_method(cDuckDBConnection, "_connect", duckdb_connection_connect, 1);
    rb_define_private_method(cDuckDBConnection, "query_sql", duckdb_connection_query_sql, 1);
}
