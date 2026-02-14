#include "ruby-duckdb.h"

VALUE cDuckDBInitInfo;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_init_info_set_error(VALUE self, VALUE error);

static const rb_data_type_t init_info_data_type = {
    "DuckDB/InitInfo",
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

void rbduckdb_init_duckdb_init_info(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBInitInfo = rb_define_class_under(mDuckDB, "InitInfo", rb_cObject);
    rb_define_alloc_func(cDuckDBInitInfo, allocate);

    rb_define_method(cDuckDBInitInfo, "set_error", rbduckdb_init_info_set_error, 1);
}
