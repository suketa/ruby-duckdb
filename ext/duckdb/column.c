#include "ruby-duckdb.h"

static VALUE cDuckDBColumn;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_column__type(VALUE oDuckDBColumn);
static VALUE duckdb_column_get_name(VALUE oDuckDBColumn);

static const rb_data_type_t column_data_type = {
    "DuckDB/Column",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBColumn *p = (rubyDuckDBColumn *)ctx;

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBColumn *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBColumn));
    return TypedData_Wrap_Struct(klass, &column_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBColumn);
}

/*
 *
 */
VALUE duckdb_column__type(VALUE oDuckDBColumn) {
    rubyDuckDBColumn *ctx;
    rubyDuckDBResult *ctxresult;
    VALUE result;
    duckdb_type type;

    TypedData_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, &column_data_type, ctx);

    result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));
    ctxresult = get_struct_result(result);
    type = duckdb_column_type(&(ctxresult->result), ctx->col);

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
    VALUE result;
    rubyDuckDBResult *ctxresult;

    TypedData_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, &column_data_type, ctx);

    result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));

    ctxresult = get_struct_result(result);

    return rb_utf8_str_new_cstr(duckdb_column_name(&(ctxresult->result), ctx->col));
}

VALUE rbduckdb_create_column(VALUE oDuckDBResult, idx_t col) {
    VALUE obj;
    rubyDuckDBColumn *ctx;

    obj = allocate(cDuckDBColumn);
    TypedData_Get_Struct(obj, rubyDuckDBColumn, &column_data_type, ctx);

    rb_ivar_set(obj, rb_intern("result"), oDuckDBResult);
    ctx->col = col;

    return obj;
}

void rbduckdb_init_duckdb_column(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBColumn = rb_define_class_under(mDuckDB, "Column", rb_cObject);
    rb_define_alloc_func(cDuckDBColumn, allocate);

    rb_define_private_method(cDuckDBColumn, "_type", duckdb_column__type, 0);
    rb_define_method(cDuckDBColumn, "name", duckdb_column_get_name, 0);
}
