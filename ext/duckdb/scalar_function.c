#include "ruby-duckdb.h"

VALUE cDuckDBScalarFunction;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static const rb_data_type_t scalar_function_data_type = {
    "DuckDB/ScalarFunction",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBScalarFunction *p = (rubyDuckDBScalarFunction *)ctx;
    duckdb_destroy_scalar_function(&(p->scalar_function));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBScalarFunction *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBScalarFunction));
    return TypedData_Wrap_Struct(klass, &scalar_function_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBScalarFunction);
}

void rbduckdb_init_duckdb_scalar_function(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBScalarFunction = rb_define_class_under(mDuckDB, "ScalarFunction", rb_cObject);
    rb_define_alloc_func(cDuckDBScalarFunction, allocate);
}
