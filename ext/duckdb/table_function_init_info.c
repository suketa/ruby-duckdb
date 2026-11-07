#include "ruby-duckdb.h"

VALUE cDuckDBTableFunctionInitInfo;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE table_function_init_info_set_error(VALUE self, VALUE error);
static VALUE table_function_init_info_set_max_threads(VALUE self, VALUE max_threads);
static VALUE table_function_init_info_column_count(VALUE self);
static VALUE table_function_init_info_column_index(VALUE self, VALUE index);
static VALUE table_function_init_info_bind_data(VALUE self);

static const rb_data_type_t init_info_data_type = {
    "DuckDB/TableFunctionInitInfo",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY | RUBY_TYPED_WB_PROTECTED
};

static void deallocate(void *ctx) {
    rubyDuckDBInitInfo *p = (rubyDuckDBInitInfo *)ctx;
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBInitInfo *ctx;
    return TypedData_Make_Struct(klass, rubyDuckDBInitInfo, &init_info_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBInitInfo);
}

rubyDuckDBInitInfo *rbduckdb_get_struct_init_info(VALUE obj) {
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
static VALUE table_function_init_info_set_error(VALUE self, VALUE error) {
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
static VALUE table_function_init_info_set_max_threads(VALUE self, VALUE max_threads) {
    rubyDuckDBInitInfo *ctx;

    TypedData_Get_Struct(self, rubyDuckDBInitInfo, &init_info_data_type, ctx);

    duckdb_init_set_max_threads(ctx->info, NUM2ULL(max_threads));

    return self;
}

/*
 * call-seq:
 *   init_info.column_count -> Integer
 *
 * Returns the number of columns DuckDB needs from this scan, including
 * columns used only in filters (e.g. +score+ in
 * <code>SELECT name FROM f() WHERE score > 0</code>). A column selected more
 * than once is counted once, and a query that references no column
 * (e.g. <code>SELECT count(*)</code>) still reports one column.
 * With DuckDB::TableFunction#supports_projection_pushdown= set to +true+,
 * the output chunk passed to the execute callback holds exactly these
 * columns. Otherwise the output chunk still holds every result column added
 * in the bind callback.
 *
 *   init_info.column_count # => 2
 */
static VALUE table_function_init_info_column_count(VALUE self) {
    rubyDuckDBInitInfo *ctx;

    TypedData_Get_Struct(self, rubyDuckDBInitInfo, &init_info_data_type, ctx);

    return ULL2NUM(duckdb_init_get_column_count(ctx->info));
}

/*
 * call-seq:
 *   init_info.column_index(index) -> Integer
 *
 * Returns the column index of the projected result column at +index+
 * (0 <= +index+ < column_count), i.e. the position of that column among the
 * columns added in the bind callback. With projection pushdown enabled, the
 * +index+-th vector of the output chunk corresponds to this column.
 *
 *   init_info.column_index(0) # => 0
 */
static VALUE table_function_init_info_column_index(VALUE self, VALUE index) {
    rubyDuckDBInitInfo *ctx;

    TypedData_Get_Struct(self, rubyDuckDBInitInfo, &init_info_data_type, ctx);

    return ULL2NUM(duckdb_init_get_column_index(ctx->info, NUM2ULL(index)));
}

/*
 * call-seq:
 *   init_info.bind_data -> object or nil
 *
 * Returns the object stored during the bind phase with
 * DuckDB::TableFunction::BindInfo#bind_data=, or nil if none was set.
 *
 *   data = init_info.bind_data
 */
static VALUE table_function_init_info_bind_data(VALUE self) {
    rubyDuckDBInitInfo *ctx;

    TypedData_Get_Struct(self, rubyDuckDBInitInfo, &init_info_data_type, ctx);

    return rbduckdb_function_data_lookup(duckdb_init_get_bind_data(ctx->info));
}

void rbduckdb_init_table_function_init_info(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBTableFunctionInitInfo = rb_define_class_under(cDuckDBTableFunction, "InitInfo", rb_cObject);
    rb_define_alloc_func(cDuckDBTableFunctionInitInfo, allocate);

    rb_define_method(cDuckDBTableFunctionInitInfo, "set_error", table_function_init_info_set_error, 1);
    rb_define_method(cDuckDBTableFunctionInitInfo, "set_max_threads", table_function_init_info_set_max_threads, 1);
    rb_define_method(cDuckDBTableFunctionInitInfo, "max_threads=", table_function_init_info_set_max_threads, 1);
    rb_define_method(cDuckDBTableFunctionInitInfo, "column_count", table_function_init_info_column_count, 0);
    rb_define_method(cDuckDBTableFunctionInitInfo, "column_index", table_function_init_info_column_index, 1);
    rb_define_method(cDuckDBTableFunctionInitInfo, "bind_data", table_function_init_info_bind_data, 0);
}
