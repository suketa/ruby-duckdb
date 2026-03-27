#include "ruby-duckdb.h"

VALUE cDuckDBScalarFunctionBindInfo;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_scalar_function_bind_info_argument_count(VALUE self);
static VALUE rbduckdb_scalar_function_bind_info_set_error(VALUE self, VALUE error);
static VALUE rbduckdb_scalar_function_bind_info_get_argument(VALUE self, VALUE index);

static const rb_data_type_t scalar_function_bind_info_data_type = {
    "DuckDB/ScalarFunction/BindInfo",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBScalarFunctionBindInfo *p = (rubyDuckDBScalarFunctionBindInfo *)ctx;
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBScalarFunctionBindInfo *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBScalarFunctionBindInfo));
    return TypedData_Wrap_Struct(klass, &scalar_function_bind_info_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBScalarFunctionBindInfo);
}

VALUE rbduckdb_scalar_function_bind_info_new(duckdb_bind_info bind_info) {
    rubyDuckDBScalarFunctionBindInfo *ctx;
    VALUE obj = allocate(cDuckDBScalarFunctionBindInfo);
    TypedData_Get_Struct(obj, rubyDuckDBScalarFunctionBindInfo, &scalar_function_bind_info_data_type, ctx);
    ctx->bind_info = bind_info;
    return obj;
}

/*
 * call-seq:
 *   bind_info.argument_count -> Integer
 *
 * Returns the number of arguments declared for the scalar function.
 *
 *   bind_info.argument_count  # => 2
 */
static VALUE rbduckdb_scalar_function_bind_info_argument_count(VALUE self) {
    rubyDuckDBScalarFunctionBindInfo *ctx;
    TypedData_Get_Struct(self, rubyDuckDBScalarFunctionBindInfo, &scalar_function_bind_info_data_type, ctx);
    return ULL2NUM(duckdb_scalar_function_bind_get_argument_count(ctx->bind_info));
}

/*
 * call-seq:
 *   bind_info.set_error(message) -> self
 *
 * Reports an error during the bind phase. The error message will be
 * propagated as a DuckDB::Error when the query is executed.
 *
 *   bind_info.set_error('invalid argument')
 */
static VALUE rbduckdb_scalar_function_bind_info_set_error(VALUE self, VALUE error) {
    rubyDuckDBScalarFunctionBindInfo *ctx;
    TypedData_Get_Struct(self, rubyDuckDBScalarFunctionBindInfo, &scalar_function_bind_info_data_type, ctx);
    duckdb_scalar_function_bind_set_error(ctx->bind_info, StringValueCStr(error));
    return self;
}

/*
 * call-seq:
 *   bind_info._get_argument(index) -> DuckDB::Expression
 *
 * Returns the expression at the given argument index.
 * Called internally by +get_argument+ after index validation.
 */
static VALUE rbduckdb_scalar_function_bind_info_get_argument(VALUE self, VALUE index) {
    rubyDuckDBScalarFunctionBindInfo *ctx;
    TypedData_Get_Struct(self, rubyDuckDBScalarFunctionBindInfo, &scalar_function_bind_info_data_type, ctx);
    duckdb_expression expr = duckdb_scalar_function_bind_get_argument(ctx->bind_info, (idx_t)NUM2ULL(index));
    return rbduckdb_expression_new(expr);
}

void rbduckdb_init_duckdb_scalar_function_bind_info(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBScalarFunctionBindInfo = rb_define_class_under(cDuckDBScalarFunction, "BindInfo", rb_cObject);
    rb_define_alloc_func(cDuckDBScalarFunctionBindInfo, allocate);
    rb_define_method(cDuckDBScalarFunctionBindInfo, "argument_count", rbduckdb_scalar_function_bind_info_argument_count, 0);
    rb_define_method(cDuckDBScalarFunctionBindInfo, "set_error", rbduckdb_scalar_function_bind_info_set_error, 1);
    rb_define_private_method(cDuckDBScalarFunctionBindInfo, "_get_argument", rbduckdb_scalar_function_bind_info_get_argument, 1);
}
