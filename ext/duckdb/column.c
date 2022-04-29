#include "ruby-duckdb.h"

static VALUE cDuckDBColumn;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static VALUE duckdb_column__type(VALUE oDuckDBColumn);
static VALUE duckdb_column_get_name(VALUE oDuckDBColumn);

static void deallocate(void *ctx) {
    rubyDuckDBColumn *p = (rubyDuckDBColumn *)ctx;

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBColumn *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBColumn));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

/*
 *
 */
VALUE duckdb_column__type(VALUE oDuckDBColumn) {
    rubyDuckDBColumn *ctx;
    Data_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, ctx);

    VALUE result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));
    rubyDuckDBResult *ctxresult;
    Data_Get_Struct(result, rubyDuckDBResult, ctxresult);
    duckdb_type type = duckdb_column_type(&(ctxresult->result), ctx->col);

    return INT2FIX(type);
}

/*
 *  call-seq:
 *    column.name -> string.
 *
 *  Returns the column name.
 *
 */
VALUE duckdb_column_get_name(VALUE oDuckDBColumn) {
    rubyDuckDBColumn *ctx;
    Data_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, ctx);

    VALUE result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));
    rubyDuckDBResult *ctxresult;
    Data_Get_Struct(result, rubyDuckDBResult, ctxresult);

    return rb_str_new2(duckdb_column_name(&(ctxresult->result), ctx->col));
}

VALUE create_column(VALUE oDuckDBResult, idx_t col) {
    VALUE obj;

    obj = allocate(cDuckDBColumn);
    rubyDuckDBColumn *ctx;
    Data_Get_Struct(obj, rubyDuckDBColumn, ctx);

    rb_ivar_set(obj, rb_intern("result"), oDuckDBResult);
    ctx->col = col;

    return obj;
}

void init_duckdb_column(void) {
    cDuckDBColumn = rb_define_class_under(mDuckDB, "Column", rb_cObject);
    rb_define_alloc_func(cDuckDBColumn, allocate);

    rb_define_private_method(cDuckDBColumn, "_type", duckdb_column__type, 0);
    rb_define_method(cDuckDBColumn, "name", duckdb_column_get_name, 0);
}
