#include "ruby-duckdb.h"

static VALUE cDuckDBResult;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static VALUE to_ruby_obj_boolean(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_smallint(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_integer(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_bigint(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_float(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_double(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_string_from_blob(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE row_array(rubyDuckDBResult *ctx, idx_t row_idx);
static VALUE duckdb_result_row_size(VALUE oDuckDBResult, VALUE args, VALUE obj);
static VALUE duckdb_result_each(VALUE oDuckDBResult);
static VALUE duckdb_result_rows_changed(VALUE oDuckDBResult);
static VALUE duckdb_result_columns(VALUE oDuckDBResult);

static void deallocate(void *ctx) {
    rubyDuckDBResult *p = (rubyDuckDBResult *)ctx;

    duckdb_destroy_result(&(p->result));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBResult *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBResult));
    return Data_Wrap_Struct(klass, NULL, deallocate, ctx);
}

static VALUE to_ruby_obj_boolean(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    bool bval = duckdb_value_boolean(result, col_idx, row_idx);
    return bval ? Qtrue : Qnil;
}

static VALUE to_ruby_obj_smallint(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    int16_t i16val = duckdb_value_int16(result, col_idx, row_idx);
    return INT2FIX(i16val);
}

static VALUE to_ruby_obj_integer(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    int32_t i32val = duckdb_value_int32(result, col_idx, row_idx);
    return INT2NUM(i32val);
}

static VALUE to_ruby_obj_bigint(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    int64_t i64val = duckdb_value_int64(result, col_idx, row_idx);
    return rb_int2big(i64val);
}

static VALUE to_ruby_obj_float(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    float fval = duckdb_value_float(result, col_idx, row_idx);
    return DBL2NUM(fval);
}

static VALUE to_ruby_obj_double(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    double dval = duckdb_value_double(result, col_idx, row_idx);
    return DBL2NUM(dval);
}

static VALUE to_ruby_obj_string_from_blob(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    VALUE str;
    duckdb_blob bval = duckdb_value_blob(result, col_idx, row_idx);
    str = rb_str_new(bval.data, bval.size);

    if (bval.data) {
        duckdb_free(bval.data);
    }

    return str;
}

static VALUE to_ruby_obj(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    char *p;
    VALUE obj = Qnil;
    if (duckdb_value_is_null(result, col_idx, row_idx)) {
        return obj;
    }
    switch(duckdb_column_type(result, col_idx)) {
    case DUCKDB_TYPE_BOOLEAN:
        return to_ruby_obj_boolean(result, col_idx, row_idx);
    case DUCKDB_TYPE_SMALLINT:
        return to_ruby_obj_smallint(result, col_idx, row_idx);
    case DUCKDB_TYPE_INTEGER:
        return to_ruby_obj_integer(result, col_idx, row_idx);
    case DUCKDB_TYPE_BIGINT:
        return to_ruby_obj_bigint(result, col_idx, row_idx);
    case DUCKDB_TYPE_FLOAT:
        return to_ruby_obj_float(result, col_idx, row_idx);
    case DUCKDB_TYPE_DOUBLE:
        return to_ruby_obj_double(result, col_idx, row_idx);
    case DUCKDB_TYPE_BLOB:
        return to_ruby_obj_string_from_blob(result, col_idx, row_idx);
    default:
        p = duckdb_value_varchar(result, col_idx, row_idx);
        if (p) {
            obj = rb_str_new2(p);
            duckdb_free(p);
            if (duckdb_column_type(result, col_idx) == DUCKDB_TYPE_HUGEINT) {
                obj = rb_funcall(obj, rb_intern("to_i"), 0);
            }
        }
    }
    return obj;
}

static VALUE row_array(rubyDuckDBResult *ctx, idx_t row_idx) {
    idx_t col_idx;
    idx_t column_count = duckdb_column_count(&(ctx->result));

    VALUE ary = rb_ary_new2(column_count);
    for(col_idx = 0; col_idx < column_count; col_idx++) {
        rb_ary_store(ary, col_idx, to_ruby_obj(&(ctx->result), col_idx, row_idx));
    }
    return ary;
}

static VALUE duckdb_result_row_size(VALUE oDuckDBResult, VALUE args, VALUE obj) {
    rubyDuckDBResult *ctx;
    Data_Get_Struct(oDuckDBResult, rubyDuckDBResult, ctx);

    return LONG2FIX(duckdb_row_count(&(ctx->result)));
}

static VALUE duckdb_result_each(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    idx_t row_idx = 0;
    idx_t row_count = 0;

    RETURN_SIZED_ENUMERATOR(oDuckDBResult, 0, 0, duckdb_result_row_size);

    Data_Get_Struct(oDuckDBResult, rubyDuckDBResult, ctx);
    row_count = duckdb_row_count(&(ctx->result));
    for (row_idx = 0; row_idx < row_count; row_idx++) {
        rb_yield(row_array(ctx, row_idx));
    }
    return oDuckDBResult;
}

/*
 *  call-seq:
 *    result.rows_changed -> integer
 *
 *  Returns the count of rows changed.
 *
 *    DuckDB::Database.open do |db|
 *      db.connect do |con|
 *        r = con.query('CREATE TABLE t2 (id INT)')
 *        r.rows_changed # => 0
 *        r = con.query('INSERT INTO t2 VALUES (1), (2), (3)')
 *        r.rows_changed # => 3
 *        r = con.query('UPDATE t2 SET id = id + 1 WHERE id > 1')
 *        r.rows_changed # => 2
 *        r = con.query('DELETE FROM t2 WHERE id = 0')
 *        r.rows_changed # => 0
 *        r = con.query('DELETE FROM t2 WHERE id = 4')
 *        r.rows_changed # => 1
 *      end
 *    end
 *
 */
static VALUE duckdb_result_rows_changed(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    Data_Get_Struct(oDuckDBResult, rubyDuckDBResult, ctx);
    return LL2NUM(duckdb_rows_changed(&(ctx->result)));
}

/*
 *  call-seq:
 *    result.columns -> DuckDB::Column[]
 *
 *  Returns the column class Lists.
 *
 */
static VALUE duckdb_result_columns(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    Data_Get_Struct(oDuckDBResult, rubyDuckDBResult, ctx);

    idx_t col_idx;
    idx_t column_count = duckdb_column_count(&(ctx->result));

    VALUE ary = rb_ary_new2(column_count);
    for(col_idx = 0; col_idx < column_count; col_idx++) {
        VALUE column = create_column(oDuckDBResult, col_idx);
        rb_ary_store(ary, col_idx, column);
    }
    return ary;
}

VALUE create_result(void) {
    return allocate(cDuckDBResult);
}

void init_duckdb_result(void) {
    cDuckDBResult = rb_define_class_under(mDuckDB, "Result", rb_cObject);
    rb_define_alloc_func(cDuckDBResult, allocate);

    rb_define_method(cDuckDBResult, "each", duckdb_result_each, 0);
    rb_define_method(cDuckDBResult, "rows_changed", duckdb_result_rows_changed, 0);
    rb_define_method(cDuckDBResult, "columns", duckdb_result_columns, 0);
}
