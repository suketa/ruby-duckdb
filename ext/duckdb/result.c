#include "ruby-duckdb.h"

static VALUE cDuckDBResult;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE to_ruby_obj_boolean(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_smallint(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_utinyint(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_integer(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_bigint(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_float(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_double(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_blob(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE duckdb_result_column_count(VALUE oDuckDBResult);
static VALUE duckdb_result_row_count(VALUE oDuckDBResult);
static VALUE duckdb_result_rows_changed(VALUE oDuckDBResult);
static VALUE duckdb_result_columns(VALUE oDuckDBResult);
static VALUE duckdb_result__column_type(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__is_null(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_boolean(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_smallint(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_utinyint(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_integer(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_bigint(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_float(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_double(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_string(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_blob(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__enum_internal_type(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__enum_dictionary_size(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__enum_dictionary_value(VALUE oDuckDBResult, VALUE col_idx, VALUE idx);

static const rb_data_type_t result_data_type = {
    "DuckDB/Result",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBResult *p = (rubyDuckDBResult *)ctx;

    duckdb_destroy_result(&(p->result));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBResult *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBResult));
    return TypedData_Wrap_Struct(klass, &result_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBResult);
}

rubyDuckDBResult *get_struct_result(VALUE obj) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBResult, &result_data_type, ctx);
    return ctx;
}

static VALUE to_ruby_obj_boolean(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    bool bval = duckdb_value_boolean(result, col_idx, row_idx);
    return bval ? Qtrue : Qfalse;
}

static VALUE to_ruby_obj_smallint(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    int16_t i16val = duckdb_value_int16(result, col_idx, row_idx);
    return INT2FIX(i16val);
}

static VALUE to_ruby_obj_utinyint(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    uint8_t ui8val = duckdb_value_uint8(result, col_idx, row_idx);
    return UINT2NUM(ui8val);
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

static VALUE to_ruby_obj_blob(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    VALUE str;
    duckdb_blob bval = duckdb_value_blob(result, col_idx, row_idx);
    str = rb_str_new(bval.data, bval.size);

    if (bval.data) {
        duckdb_free(bval.data);
    }

    return str;
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
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_rows_changed(&(ctx->result)));
}

/*
 *  call-seq:
 *    result.column_count -> Integer
 *
 *  Returns the column size of the result.
 *
 *    DuckDB::Database.open do |db|
 *      db.connect do |con|
 *        r = con.query('CREATE TABLE t2 (id INT, name VARCHAR(128))')
 *        r = con.query("INSERT INTO t2 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Catherine')")
 *        r = con.query('SELECT id FROM t2')
 *        r.column_count # => 1
 *        r = con.query('SELECT id, name FROM t2')
 *        r.column_count # => 2
 *      end
 *    end
 *
 */
static VALUE duckdb_result_column_count(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_column_count(&(ctx->result)));
}

/*
 *  call-seq:
 *    result.row_count -> Integer
 *
 *  Returns the column size of the result.
 *
 *    DuckDB::Database.open do |db|
 *      db.connect do |con|
 *        r = con.query('CREATE TABLE t2 (id INT, name VARCHAR(128))')
 *        r = con.query("INSERT INTO t2 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Catherine')")
 *        r = con.query('SELECT * FROM t2')
 *        r.row_count # => 3
 *        r = con.query('SELECT * FROM t2 where id = 1')
 *        r.row_count # => 1
 *      end
 *    end
 *
 */
static VALUE duckdb_result_row_count(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_row_count(&(ctx->result)));
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
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    idx_t col_idx;
    idx_t column_count = duckdb_column_count(&(ctx->result));

    VALUE ary = rb_ary_new2(column_count);
    for(col_idx = 0; col_idx < column_count; col_idx++) {
        VALUE column = create_column(oDuckDBResult, col_idx);
        rb_ary_store(ary, col_idx, column);
    }
    return ary;
}

static VALUE duckdb_result__column_type(VALUE oDuckDBResult, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_column_type(&(ctx->result), NUM2LL(col_idx)));
}

static VALUE duckdb_result__is_null(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    bool is_null;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    is_null = duckdb_value_is_null(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
    return is_null ? Qtrue : Qfalse;
}

static VALUE duckdb_result__to_boolean(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_boolean(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx)) ? Qtrue : Qfalse;
}

static VALUE duckdb_result__to_smallint(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_smallint(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result__to_utinyint(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_utinyint(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result__to_integer(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_integer(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result__to_bigint(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_bigint(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result__to_float(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_float(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result__to_double(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_double(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result__to_string(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    char *p;
    VALUE obj;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    p = duckdb_value_varchar(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
    if (p) {
        obj = rb_utf8_str_new_cstr(p);
        duckdb_free(p);
        return obj;
    }
    return Qnil;
}

static VALUE duckdb_result__to_blob(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_blob(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result__enum_internal_type(VALUE oDuckDBResult, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    VALUE type = Qnil;
    duckdb_logical_type logical_type;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    logical_type = duckdb_column_logical_type(&(ctx->result), NUM2LL(col_idx));
    if (logical_type) {
        type = LL2NUM(duckdb_enum_internal_type(logical_type));
    }
    return type;
}

static VALUE duckdb_result__enum_dictionary_size(VALUE oDuckDBResult, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    VALUE size = Qnil;
    duckdb_logical_type logical_type;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    logical_type = duckdb_column_logical_type(&(ctx->result), NUM2LL(col_idx));
    if (logical_type) {
        size = UINT2NUM(duckdb_enum_dictionary_size(logical_type));
    }
    return size;
}

static VALUE duckdb_result__enum_dictionary_value(VALUE oDuckDBResult, VALUE col_idx, VALUE idx) {
    rubyDuckDBResult *ctx;
    VALUE value = Qnil;
    duckdb_logical_type logical_type;
    char *p;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    logical_type = duckdb_column_logical_type(&(ctx->result), NUM2LL(col_idx));
    if (logical_type) {
        p = duckdb_enum_dictionary_value(logical_type, NUM2LL(idx));
        if (p) {
            value = rb_utf8_str_new_cstr(p);
            duckdb_free(p);
        }
    }
    return value;
}

VALUE create_result(void) {
    return allocate(cDuckDBResult);
}

void init_duckdb_result(void) {
    cDuckDBResult = rb_define_class_under(mDuckDB, "Result", rb_cObject);
    rb_define_alloc_func(cDuckDBResult, allocate);

    rb_define_method(cDuckDBResult, "column_count", duckdb_result_column_count, 0);
    rb_define_method(cDuckDBResult, "row_count", duckdb_result_row_count, 0);
    rb_define_method(cDuckDBResult, "rows_changed", duckdb_result_rows_changed, 0);
    rb_define_method(cDuckDBResult, "columns", duckdb_result_columns, 0);
    rb_define_private_method(cDuckDBResult, "_column_type", duckdb_result__column_type, 1);
    rb_define_private_method(cDuckDBResult, "_null?", duckdb_result__is_null, 2);
    rb_define_private_method(cDuckDBResult, "_to_boolean", duckdb_result__to_boolean, 2);
    rb_define_private_method(cDuckDBResult, "_to_smallint", duckdb_result__to_smallint, 2);
    rb_define_private_method(cDuckDBResult, "_to_utinyint", duckdb_result__to_utinyint, 2);
    rb_define_private_method(cDuckDBResult, "_to_integer", duckdb_result__to_integer, 2);
    rb_define_private_method(cDuckDBResult, "_to_bigint", duckdb_result__to_bigint, 2);
    rb_define_private_method(cDuckDBResult, "_to_float", duckdb_result__to_float, 2);
    rb_define_private_method(cDuckDBResult, "_to_double", duckdb_result__to_double, 2);
    rb_define_private_method(cDuckDBResult, "_to_string", duckdb_result__to_string, 2);
    rb_define_private_method(cDuckDBResult, "_to_blob", duckdb_result__to_blob, 2);
    rb_define_private_method(cDuckDBResult, "_enum_internal_type", duckdb_result__enum_internal_type, 1);
    rb_define_private_method(cDuckDBResult, "_enum_dictionary_size", duckdb_result__enum_dictionary_size, 1);
    rb_define_private_method(cDuckDBResult, "_enum_dictionary_value", duckdb_result__enum_dictionary_value, 2);
}
