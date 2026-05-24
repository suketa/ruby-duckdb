#include "ruby-duckdb.h"

VALUE cDuckDBTableFunctionInitInfo;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_init_info_set_error(VALUE self, VALUE error);
static VALUE rbduckdb_init_info_set_max_threads(VALUE self, VALUE max_threads);

static const rb_data_type_t init_info_data_type = {
    "DuckDB/TableFunctionInitInfo",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBInitInfo *p = (rubyDuckDBInitInfo *)ctx;
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBInitInfo *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBInitInfo));
    return TypedData_Wrap_Struct(klass, &init_info_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBInitInfo);
}

rubyDuckDBInitInfo *get_struct_init_info(VALUE obj) {
    rubyDuckDBInitInfo *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBInitInfo, &init_info_data_type, ctx);
    return ctx;
}

/*
 * call-seq:
 *   init_info.set_error(error_message) -> self
 *
 * Sets an error message for the init phase.
 * This will cause the query to fail with the specified error.
 *
 *   init_info.set_error('Invalid initialization')
 */
static VALUE rbduckdb_init_info_set_error(VALUE self, VALUE error) {
    rubyDuckDBInitInfo *ctx;
    const char *error_msg;

    TypedData_Get_Struct(self, rubyDuckDBInitInfo, &init_info_data_type, ctx);

    error_msg = StringValueCStr(error);
    duckdb_init_set_error(ctx->info, error_msg);

    return self;
}

/*
 * call-seq:
 *   init_info.set_max_threads(max_threads) -> self
 *   init_info.max_threads = max_threads
 *
 * Sets the maximum number of threads that can execute the table function concurrently.
 * This is a hint to DuckDB's scheduler; the actual number of threads is also bounded
 * by the configured worker pool size (e.g., +SET threads+).
 *
 *   init_info.max_threads = 4
 */
static VALUE rbduckdb_init_info_set_max_threads(VALUE self, VALUE max_threads) {
    rubyDuckDBInitInfo *ctx;

    TypedData_Get_Struct(self, rubyDuckDBInitInfo, &init_info_data_type, ctx);

    duckdb_init_set_max_threads(ctx->info, NUM2ULL(max_threads));

    return self;
}

void rbduckdb_init_duckdb_table_function_init_info(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBTableFunctionInitInfo = rb_define_class_under(cDuckDBTableFunction, "InitInfo", rb_cObject);
    rb_define_alloc_func(cDuckDBTableFunctionInitInfo, allocate);

    rb_define_method(cDuckDBTableFunctionInitInfo, "set_error", rbduckdb_init_info_set_error, 1);
    rb_define_method(cDuckDBTableFunctionInitInfo, "set_max_threads", rbduckdb_init_info_set_max_threads, 1);
    rb_define_method(cDuckDBTableFunctionInitInfo, "max_threads=", rbduckdb_init_info_set_max_threads, 1);
}
