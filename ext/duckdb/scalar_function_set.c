#include "ruby-duckdb.h"

VALUE cDuckDBScalarFunctionSet;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_scalar_function_set__initialize(VALUE self, VALUE name);

static const rb_data_type_t scalar_function_set_data_type = {
    "DuckDB/ScalarFunctionSet",
    {NULL, deallocate, memsize, NULL},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBScalarFunctionSet *p = (rubyDuckDBScalarFunctionSet *)ctx;
    duckdb_destroy_scalar_function_set(&(p->scalar_function_set));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBScalarFunctionSet *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBScalarFunctionSet));
    return TypedData_Wrap_Struct(klass, &scalar_function_set_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBScalarFunctionSet);
}

rubyDuckDBScalarFunctionSet *get_struct_scalar_function_set(VALUE obj) {
    rubyDuckDBScalarFunctionSet *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBScalarFunctionSet, &scalar_function_set_data_type, ctx);
    return ctx;
}

/*
 * call-seq:
 *   _initialize(name) -> self
 *
 * Creates the underlying +duckdb_scalar_function_set+ with the given name.
 * Called from the Ruby +initialize+ wrapper after argument validation.
 */
static VALUE rbduckdb_scalar_function_set__initialize(VALUE self, VALUE name) {
    rubyDuckDBScalarFunctionSet *p;

    TypedData_Get_Struct(self, rubyDuckDBScalarFunctionSet, &scalar_function_set_data_type, p);
    p->scalar_function_set = duckdb_create_scalar_function_set(StringValueCStr(name));
    return self;
}

void rbduckdb_init_duckdb_scalar_function_set(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBScalarFunctionSet = rb_define_class_under(mDuckDB, "ScalarFunctionSet", rb_cObject);
    rb_define_alloc_func(cDuckDBScalarFunctionSet, allocate);
    rb_define_private_method(cDuckDBScalarFunctionSet, "_initialize", rbduckdb_scalar_function_set__initialize, 1);
}
