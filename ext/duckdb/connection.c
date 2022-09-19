#include "ruby-duckdb.h"

VALUE cDuckDBConnection;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static VALUE duckdb_connection_disconnect(VALUE self);
static VALUE duckdb_connection_connect(VALUE self, VALUE oDuckDBDatabase);
static VALUE duckdb_connection_query_sql(VALUE self, VALUE str);

static void deallocate(void *ctx) {
    rubyDuckDBConnection *p = (rubyDuckDBConnection *)ctx;

    duckdb_disconnect(&(p->con));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBConnection *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBConnection));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

VALUE create_connection(VALUE oDuckDBDatabase) {
    rubyDuckDB *ctxdb;
    rubyDuckDBConnection *ctxcon;
    VALUE obj;

    ctxdb = get_struct_database(oDuckDBDatabase);

    obj = allocate(cDuckDBConnection);
    Data_Get_Struct(obj, rubyDuckDBConnection, ctxcon);

    if (duckdb_connect(ctxdb->db, &(ctxcon->con)) == DuckDBError) {
        rb_raise(eDuckDBError, "connection error");
    }

    // rb_ivar_set(obj, rb_intern("database"), oDuckDBDatabase);
    return obj;
}

static VALUE duckdb_connection_disconnect(VALUE self) {
    rubyDuckDBConnection *ctx;

    Data_Get_Struct(self, rubyDuckDBConnection, ctx);
    duckdb_disconnect(&(ctx->con));

    return self;
}

static VALUE duckdb_connection_connect(VALUE self, VALUE oDuckDBDatabase) {
    rubyDuckDBConnection *ctx;
    rubyDuckDB *ctxdb;

    if (!rb_obj_is_kind_of(oDuckDBDatabase, cDuckDBDatabase)) {
        rb_raise(rb_eTypeError, "The first argument must be DuckDB::Database object.");
    }
    ctxdb = get_struct_database(oDuckDBDatabase);
    Data_Get_Struct(self, rubyDuckDBConnection, ctx);

    if (duckdb_connect(ctxdb->db, &(ctx->con)) == DuckDBError) {
        rb_raise(eDuckDBError, "connection error");
    }

    return self;
}

static VALUE duckdb_connection_query_sql(VALUE self, VALUE str) {
    rubyDuckDBConnection *ctx;
    rubyDuckDBResult *ctxr;

    VALUE result = create_result();

    Data_Get_Struct(self, rubyDuckDBConnection, ctx);
    Data_Get_Struct(result, rubyDuckDBResult, ctxr);

    if (!(ctx->con)) {
        rb_raise(eDuckDBError, "Database connection closed");
    }

    if (duckdb_query(ctx->con, StringValueCStr(str), &(ctxr->result)) == DuckDBError) {
        rb_raise(eDuckDBError, "%s", duckdb_result_error(&(ctxr->result)));
    }
    return result;
}

void init_duckdb_connection(void) {
    cDuckDBConnection = rb_define_class_under(mDuckDB, "Connection", rb_cObject);
    rb_define_alloc_func(cDuckDBConnection, allocate);

    rb_define_method(cDuckDBConnection, "disconnect", duckdb_connection_disconnect, 0);
    rb_define_private_method(cDuckDBConnection, "_connect", duckdb_connection_connect, 1);
    rb_define_private_method(cDuckDBConnection, "query_sql", duckdb_connection_query_sql, 1);
}
