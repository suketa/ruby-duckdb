#include "ruby-duckdb.h"

static VALUE cDuckDBResult;

static void deallocate(void *ctx)
{
    rubyDuckDBResult *p = (rubyDuckDBResult *)ctx;

    duckdb_destroy_result(&(p->result));
    xfree(p);
}

static VALUE allocate(VALUE klass)
{
    rubyDuckDBResult *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBResult));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

static VALUE row_array(rubyDuckDBResult *ctx, size_t row_idx) {
    size_t col_idx;
    VALUE ary = rb_ary_new2(ctx->result.column_count);
    for(col_idx = 0; col_idx < ctx->result.column_count; col_idx++) {
        char *p = duckdb_value_varchar(&(ctx->result), col_idx, row_idx);
        rb_ary_store(ary, col_idx, rb_str_new2(p));

        free(p);
    }
    return ary;
}

static VALUE duckdb_result_each(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    size_t row_idx = 0;

    // RETURN_ENUMERATOR(oDuckDBResult, 0, 0);

    Data_Get_Struct(oDuckDBResult, rubyDuckDBResult, ctx);
    for (row_idx = 0; row_idx < ctx->result.row_count; row_idx++) {
        rb_yield(row_array(ctx, row_idx));
    }
    return oDuckDBResult;
}

VALUE create_result(void) {
    return allocate(cDuckDBResult);
}

void init_duckdb_result(void)
{
    cDuckDBResult = rb_define_class_under(mDuckDB, "Result", rb_cObject);
    rb_define_alloc_func(cDuckDBResult, allocate);

    rb_define_method(cDuckDBResult, "each", duckdb_result_each, 0);
}
