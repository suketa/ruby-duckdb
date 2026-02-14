#include "ruby-duckdb.h"

VALUE cDuckDBConnection;

static void deallocate(void *ctx);
static void mark(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_connection_disconnect(VALUE self);
static VALUE duckdb_connection_interrupt(VALUE self);
static VALUE duckdb_connection_query_progress(VALUE self);
static VALUE duckdb_connection_connect(VALUE self, VALUE oDuckDBDatabase);
static VALUE duckdb_connection_query_sql(VALUE self, VALUE str);
static VALUE duckdb_connection_register_scalar_function(VALUE self, VALUE scalar_function);
static VALUE duckdb_connection_register_table_function(VALUE self, VALUE table_function);

static const rb_data_type_t connection_data_type = {
    "DuckDB/Connection",
    {mark, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBConnection *p = (rubyDuckDBConnection *)ctx;

    duckdb_disconnect(&(p->con));
    xfree(p);
}

static void mark(void *ctx) {
    rubyDuckDBConnection *p = (rubyDuckDBConnection *)ctx;
    rb_gc_mark(p->registered_functions);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBConnection *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBConnection));
    VALUE obj = TypedData_Wrap_Struct(klass, &connection_data_type, ctx);
    VALUE registered_functions = rb_ary_new();
    ctx->registered_functions = registered_functions;
    RB_GC_GUARD(registered_functions);
    return obj;
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

    /* Clear registered functions to release memory */
    rb_ary_clear(ctx->registered_functions);

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

/* :nodoc: */
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

/* :nodoc: */
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

/* :nodoc: */
static VALUE duckdb_connection_register_scalar_function(VALUE self, VALUE scalar_function) {
    rubyDuckDBConnection *ctxcon;
    rubyDuckDBScalarFunction *ctxsf;
    duckdb_state state;

    ctxcon = get_struct_connection(self);
    ctxsf = get_struct_scalar_function(scalar_function);

    state = duckdb_register_scalar_function(ctxcon->con, ctxsf->scalar_function);

    if (state == DuckDBError) {
        rb_raise(eDuckDBError, "Failed to register scalar function");
    }

    /* Keep reference to prevent GC while connection is alive */
    rb_ary_push(ctxcon->registered_functions, scalar_function);

    return self;
}

static VALUE duckdb_connection_register_table_function(VALUE self, VALUE table_function) {
    rubyDuckDBConnection *ctxcon;
    rubyDuckDBTableFunction *ctxtf;
    duckdb_state state;

    ctxcon = get_struct_connection(self);
    ctxtf = get_struct_table_function(table_function);

    state = duckdb_register_table_function(ctxcon->con, ctxtf->table_function);

    if (state == DuckDBError) {
        rb_raise(eDuckDBError, "Failed to register table function");
    }

    /* Keep reference to prevent GC while connection is alive */
    rb_ary_push(ctxcon->registered_functions, table_function);

    return self;
}

void rbduckdb_init_duckdb_connection(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBConnection = rb_define_class_under(mDuckDB, "Connection", rb_cObject);
    rb_define_alloc_func(cDuckDBConnection, allocate);

    rb_define_method(cDuckDBConnection, "disconnect", duckdb_connection_disconnect, 0);
    rb_define_method(cDuckDBConnection, "interrupt", duckdb_connection_interrupt, 0);
    rb_define_method(cDuckDBConnection, "query_progress", duckdb_connection_query_progress, 0);
    rb_define_private_method(cDuckDBConnection, "_register_scalar_function", duckdb_connection_register_scalar_function, 1);
    rb_define_private_method(cDuckDBConnection, "_register_table_function", duckdb_connection_register_table_function, 1);
    rb_define_private_method(cDuckDBConnection, "_connect", duckdb_connection_connect, 1);
    /* TODO: query_sql => _query_sql */
    rb_define_private_method(cDuckDBConnection, "query_sql", duckdb_connection_query_sql, 1);
}
