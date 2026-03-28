#include "ruby-duckdb.h"

VALUE cDuckDBClientContext;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

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

void rbduckdb_init_duckdb_client_context(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBClientContext = rb_define_class_under(mDuckDB, "ClientContext", rb_cObject);
    rb_define_alloc_func(cDuckDBClientContext, allocate);
}
