#include "ruby-duckdb.h"

VALUE cDuckDBClientContext;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_client_context_connection_id(VALUE self);

static const rb_data_type_t client_context_data_type = {
    "DuckDB/ClientContext",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBClientContext *p = (rubyDuckDBClientContext *)ctx;

    if (p->client_context) {
        duckdb_destroy_client_context(&(p->client_context));
    }

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBClientContext *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBClientContext));
    return TypedData_Wrap_Struct(klass, &client_context_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBClientContext);
}

VALUE rbduckdb_client_context_new(duckdb_client_context client_context) {
    rubyDuckDBClientContext *ctx;
    VALUE obj = allocate(cDuckDBClientContext);

    TypedData_Get_Struct(obj, rubyDuckDBClientContext, &client_context_data_type, ctx);
    ctx->client_context = client_context;

    return obj;
}

/*
 * call-seq:
 *   client_context.connection_id -> Integer
 *
 * Returns the connection id of the client context.
 */
static VALUE rbduckdb_client_context_connection_id(VALUE self) {
    rubyDuckDBClientContext *ctx;
    TypedData_Get_Struct(self, rubyDuckDBClientContext, &client_context_data_type, ctx);
    return ULL2NUM(duckdb_client_context_get_connection_id(ctx->client_context));
}

void rbduckdb_init_duckdb_client_context(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBClientContext = rb_define_class_under(mDuckDB, "ClientContext", rb_cObject);
    rb_define_alloc_func(cDuckDBClientContext, allocate);
    rb_define_method(cDuckDBClientContext, "connection_id", rbduckdb_client_context_connection_id, 0);
}
