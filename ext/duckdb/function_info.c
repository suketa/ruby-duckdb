#include "ruby-duckdb.h"

VALUE cDuckDBFunctionInfo;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_function_info_set_error(VALUE self, VALUE error);

static const rb_data_type_t function_info_data_type = {
    "DuckDB/FunctionInfo",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBFunctionInfo *p = (rubyDuckDBFunctionInfo *)ctx;
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBFunctionInfo *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBFunctionInfo));
    return TypedData_Wrap_Struct(klass, &function_info_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBFunctionInfo);
}

rubyDuckDBFunctionInfo *get_struct_function_info(VALUE obj) {
    rubyDuckDBFunctionInfo *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBFunctionInfo, &function_info_data_type, ctx);
    return ctx;
}

/*
 * call-seq:
 *   function_info.set_error(error_message) -> self
 *
 * Sets an error message for the function execution.
 * This will cause the query to fail with the specified error.
 *
 *   function_info.set_error('Invalid parameter value')
 */
static VALUE rbduckdb_function_info_set_error(VALUE self, VALUE error) {
    rubyDuckDBFunctionInfo *ctx;
    const char *error_msg;

    TypedData_Get_Struct(self, rubyDuckDBFunctionInfo, &function_info_data_type, ctx);

    error_msg = StringValueCStr(error);
    duckdb_function_set_error(ctx->info, error_msg);

    return self;
}

void rbduckdb_init_duckdb_function_info(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBFunctionInfo = rb_define_class_under(mDuckDB, "FunctionInfo", rb_cObject);
    rb_define_alloc_func(cDuckDBFunctionInfo, allocate);

    rb_define_method(cDuckDBFunctionInfo, "set_error", rbduckdb_function_info_set_error, 1);
}
