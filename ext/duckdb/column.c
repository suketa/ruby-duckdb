#include "ruby-duckdb.h"

static VALUE cDuckDBColumn;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static VALUE duckdb_column_get_type(VALUE oDuckDBColumn);
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
 *  call-seq:
 *    column.type -> symbol.
 *
 *  Returns the column type.
 *
 */
VALUE duckdb_column_get_type(VALUE oDuckDBColumn) {
    rubyDuckDBColumn *ctx;
    Data_Get_Struct(oDuckDBColumn, rubyDuckDBColumn, ctx);

    VALUE result = rb_ivar_get(oDuckDBColumn, rb_intern("result"));
    rubyDuckDBResult *ctxresult;
    Data_Get_Struct(result, rubyDuckDBResult, ctxresult);
    duckdb_type type = duckdb_column_type(&(ctxresult->result), ctx->col);

    switch (type) {
    case DUCKDB_TYPE_BOOLEAN:
        return ID2SYM(rb_intern("boolean"));
    case DUCKDB_TYPE_TINYINT:
        return ID2SYM(rb_intern("tinyint"));
    case DUCKDB_TYPE_SMALLINT:
        return ID2SYM(rb_intern("smallint"));
    case DUCKDB_TYPE_INTEGER:
        return ID2SYM(rb_intern("integer"));
    case DUCKDB_TYPE_BIGINT:
        return ID2SYM(rb_intern("bigint"));
    case DUCKDB_TYPE_UTINYINT:
        return ID2SYM(rb_intern("utinyint"));
    case DUCKDB_TYPE_USMALLINT:
        return ID2SYM(rb_intern("usmallint"));
    case DUCKDB_TYPE_UINTEGER:
        return ID2SYM(rb_intern("uinteger"));
    case DUCKDB_TYPE_UBIGINT:
        return ID2SYM(rb_intern("ubigint"));
    case DUCKDB_TYPE_FLOAT:
        return ID2SYM(rb_intern("float"));
    case DUCKDB_TYPE_DOUBLE:
        return ID2SYM(rb_intern("double"));
    case DUCKDB_TYPE_TIMESTAMP:
        return ID2SYM(rb_intern("timestamp"));
    case DUCKDB_TYPE_DATE:
        return ID2SYM(rb_intern("date"));
    case DUCKDB_TYPE_TIME:
        return ID2SYM(rb_intern("time"));
    case DUCKDB_TYPE_INTERVAL:
        return ID2SYM(rb_intern("interval"));
    case DUCKDB_TYPE_HUGEINT:
        return ID2SYM(rb_intern("hugeint"));
    case DUCKDB_TYPE_VARCHAR:
        return ID2SYM(rb_intern("vachar"));
    case DUCKDB_TYPE_BLOB:
        return ID2SYM(rb_intern("blob"));
#ifdef HAVE_DUCKDB_HEADER_VERSION_033
    case DUCKDB_TYPE_DECIMAL:
        return ID2SYM(rb_intern("decimal"));
#endif
    default:
        return ID2SYM(rb_intern("invalid"));
    }
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

    rb_define_method(cDuckDBColumn, "type", duckdb_column_get_type, 0);
    rb_define_method(cDuckDBColumn, "name", duckdb_column_get_name, 0);
}
