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
static VALUE to_ruby_obj_hugeint(duckdb_result *result, idx_t col_idx, idx_t row_idx);
static VALUE to_ruby_obj_decimal(duckdb_result *result, idx_t col_idx, idx_t row_idx);
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
static VALUE duckdb_result___to_hugeint_internal(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result___to_decimal_internal(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_float(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_double(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_string(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_string_internal(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__to_blob(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx);
static VALUE duckdb_result__enum_internal_type(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__enum_dictionary_size(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__enum_dictionary_value(VALUE oDuckDBResult, VALUE col_idx, VALUE idx);

#ifdef HAVE_DUCKDB_H_GE_V080
static VALUE vector_date(void *vector_data, idx_t row_idx);
static VALUE vector_timestamp(void* vector_data, idx_t row_idx);
static VALUE vector_interval(void* vector_data, idx_t row_idx);
static VALUE vector_blob(void* vector_data, idx_t row_idx);
static VALUE vector_varchar(void* vector_data, idx_t row_idx);
static VALUE vector_hugeint(void* vector_data, idx_t row_idx);
static VALUE vector_decimal(duckdb_logical_type ty, void* vector_data, idx_t row_idx);
static VALUE vector_enum(duckdb_logical_type ty, void* vector_data, idx_t row_idx);
static VALUE vector_list(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_map(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_uuid(void* vector_data, idx_t row_idx);
static VALUE vector_value(duckdb_vector vector, idx_t row_idx);
static VALUE duckdb_result_chunk_each(VALUE oDuckDBResult);
#endif

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
    return LL2NUM(i64val);
}

static VALUE to_ruby_obj_hugeint(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    duckdb_hugeint hugeint = duckdb_value_hugeint(result, col_idx, row_idx);
    return rb_ary_new3(2, ULL2NUM(hugeint.lower), LL2NUM(hugeint.upper));
}

static VALUE to_ruby_obj_decimal(duckdb_result *result, idx_t col_idx, idx_t row_idx) {
    duckdb_decimal decimal = duckdb_value_decimal(result, col_idx, row_idx);
    return rb_ary_new3(4, ULL2NUM(decimal.value.lower), LL2NUM(decimal.value.upper), UINT2NUM(decimal.width), UINT2NUM(decimal.scale));
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

static VALUE duckdb_result___to_hugeint_internal(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_hugeint(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
}

static VALUE duckdb_result___to_decimal_internal(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    return to_ruby_obj_decimal(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
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
    duckdb_string p;
    VALUE obj;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    p = duckdb_value_string(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
    if (p.data) {
        obj = rb_utf8_str_new(p.data, p.size);
        duckdb_free(p.data);
        return obj;
    }
    return Qnil;
}

static VALUE duckdb_result__to_string_internal(VALUE oDuckDBResult, VALUE row_idx, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    duckdb_string p;
    VALUE obj;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    p = duckdb_value_string_internal(&(ctx->result), NUM2LL(col_idx), NUM2LL(row_idx));
    if (p.data) {
        obj = rb_utf8_str_new(p.data, p.size);
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

#ifdef HAVE_DUCKDB_H_GE_V080
static VALUE vector_date(void *vector_data, idx_t row_idx) {
    duckdb_date_struct date = duckdb_from_date(((duckdb_date *) vector_data)[row_idx]);
    VALUE mConverter = rb_const_get(mDuckDB, rb_intern("Converter"));

    return rb_funcall(mConverter, rb_intern("_to_date"), 3,
            INT2FIX(date.year),
            INT2FIX(date.month),
            INT2FIX(date.day)
            );
}

static VALUE vector_timestamp(void* vector_data, idx_t row_idx) {
    duckdb_timestamp_struct data = duckdb_from_timestamp(((duckdb_timestamp *)vector_data)[row_idx]);
    VALUE mConverter = rb_const_get(mDuckDB, rb_intern("Converter"));
    return rb_funcall(mConverter, rb_intern("_to_time"), 7,
            INT2FIX(data.date.year),
            INT2FIX(data.date.month),
            INT2FIX(data.date.day),
            INT2FIX(data.time.hour),
            INT2FIX(data.time.min),
            INT2FIX(data.time.sec),
            INT2NUM(data.time.micros)
            );
}

static VALUE vector_interval(void* vector_data, idx_t row_idx) {
    duckdb_interval data = ((duckdb_interval *)vector_data)[row_idx];
    VALUE mConverter = rb_const_get(mDuckDB, rb_intern("Converter"));
    return rb_funcall(mConverter, rb_intern("_to_interval_from_vector"), 3,
            INT2NUM(data.months),
            INT2NUM(data.days),
            LL2NUM(data.micros)
            );
}

static VALUE vector_blob(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    if(duckdb_string_is_inlined(s)) {
        return rb_str_new(s.value.inlined.inlined, s.value.inlined.length);
    } else {
        return rb_str_new(s.value.pointer.ptr, s.value.pointer.length);
    }
}

static VALUE vector_varchar(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    if(duckdb_string_is_inlined(s)) {
        return rb_str_new(s.value.inlined.inlined, s.value.inlined.length);
    } else {
        return rb_str_new(s.value.pointer.ptr, s.value.pointer.length);
    }
}

static VALUE vector_hugeint(void* vector_data, idx_t row_idx) {
    duckdb_hugeint hugeint = ((duckdb_hugeint *)vector_data)[row_idx];
    VALUE mConverter = rb_const_get(mDuckDB, rb_intern("Converter"));
    return rb_funcall(mConverter, rb_intern("_to_hugeint_from_vector"), 2,
            ULL2NUM(hugeint.lower),
            LL2NUM(hugeint.upper)
            );
}

static VALUE vector_decimal(duckdb_logical_type ty, void* vector_data, idx_t row_idx) {
    uint8_t width = duckdb_decimal_width(ty);
    uint8_t scale = duckdb_decimal_scale(ty);
    VALUE mConverter = rb_const_get(mDuckDB, rb_intern("Converter"));
    duckdb_type type = duckdb_decimal_internal_type(ty);
    duckdb_hugeint value;

    value.upper = 0;
    value.lower = 0;

    switch(duckdb_decimal_internal_type(ty)) {
        case DUCKDB_TYPE_HUGEINT:
            value = ((duckdb_hugeint *) vector_data)[row_idx];
            break;
        default:
            rb_warn("Unknown decimal internal type %d", type);
    }

    return rb_funcall(mConverter, rb_intern("_to_decimal_from_vector"), 4,
            INT2FIX(width),
            INT2FIX(scale),
            ULL2NUM(value.lower),
            LL2NUM(value.upper)
            );
}

static VALUE vector_enum(duckdb_logical_type ty, void* vector_data, idx_t row_idx) {
    duckdb_type type = duckdb_enum_internal_type(ty);
    uint8_t index;
    char *p;
    VALUE value = Qnil;

    switch(type) {
        case DUCKDB_TYPE_UTINYINT:
            index = ((uint8_t *) vector_data)[row_idx];
            p = duckdb_enum_dictionary_value(ty, index);
            if (p) {
                value = rb_utf8_str_new_cstr(p);
                duckdb_free(p);
            }
            break;
        default:
            rb_warn("Unknown enum internal type %d", type);
    }
    return value;
}

static VALUE vector_list(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    // Lists are stored as vectors within vectors

    VALUE ary = Qnil;
    VALUE element = Qnil;
    idx_t i;

    // rb_warn("ruby-duckdb does not support List yet");

    duckdb_logical_type child_logical_type = duckdb_list_type_child_type(ty);
    // duckdb_type child_type = duckdb_get_type_id(child_logical_type);

    duckdb_list_entry list_entry = ((duckdb_list_entry *)vector)[row_idx];
    ary = rb_ary_new2(list_entry.length);

    for (i = list_entry.offset; i < list_entry.offset + list_entry.length; ++i) {
        /*
         * FIXME: How to get the child element?
         */
        // element = ???
        rb_ary_store(ary, i - list_entry.offset, element);
    }
    duckdb_destroy_logical_type(&child_logical_type);
    return ary;
}

static VALUE vector_map(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    VALUE hash = rb_hash_new();

    duckdb_logical_type key_logical_type = duckdb_map_type_key_type(ty);
    duckdb_logical_type value_logical_type = duckdb_map_type_value_type(ty);
    // duckdb_type key_type = duckdb_get_type_id(key_logical_type);
    // duckdb_type value_type = duckdb_get_type_id(value_logical_type);

    /*
     * FIXME: How to get key and value?
     *
     * rb_hash_aset(hash, key, value);
     */
    duckdb_destroy_logical_type(&key_logical_type);
    duckdb_destroy_logical_type(&value_logical_type);
    return hash;
}

static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    VALUE hash = rb_hash_new();
    VALUE value = Qnil;
    VALUE key = Qnil;
    char *p;

    idx_t child_count = duckdb_struct_type_child_count(ty);

    for (idx_t i = 0; i < child_count; ++i) {
        p = duckdb_struct_type_child_name(ty, i);
        if (p) {
            key = rb_str_new2(p);
            // FIXME
            // How to get Struct values?
            // value = ???
            // duckdb_vector child_vector = duckdb_struct_vector_get_child(vector, i);
            // VALUE value = vector_value(child_vector, i);
            rb_hash_aset(hash, key, value);
            duckdb_free(p);
        }
    }

    return hash;
}

static VALUE vector_uuid(void* vector_data, idx_t row_idx) {
    duckdb_hugeint hugeint = ((duckdb_hugeint *)vector_data)[row_idx];
    VALUE mConverter = rb_const_get(mDuckDB, rb_intern("Converter"));
    return rb_funcall(mConverter, rb_intern("_to_uuid_from_vector"), 2,
            ULL2NUM(hugeint.lower),
            LL2NUM(hugeint.upper)
            );
}

static VALUE vector_value(duckdb_vector vector, idx_t row_idx) {
    uint64_t *validity;
    duckdb_logical_type ty;
    duckdb_type type_id;
    void* vector_data;
    VALUE obj = Qnil;

    validity = duckdb_vector_get_validity(vector);
    if (!duckdb_validity_row_is_valid(validity, row_idx)) {
        return Qnil;
    }

    ty = duckdb_vector_get_column_type(vector);
    type_id = duckdb_get_type_id(ty);
    vector_data = duckdb_vector_get_data(vector);

    switch(type_id) {
        case DUCKDB_TYPE_INVALID:
            obj = Qnil;
            break;
        case DUCKDB_TYPE_BOOLEAN:
            obj = (((bool*) vector_data)[row_idx]) ? Qtrue : Qfalse;
            break;
        case DUCKDB_TYPE_TINYINT:
            obj = INT2FIX(((int8_t *) vector_data)[row_idx]);
            break;
        case DUCKDB_TYPE_SMALLINT:
            obj = INT2FIX(((int16_t *) vector_data)[row_idx]);
            break;
        case DUCKDB_TYPE_INTEGER:
            obj = INT2NUM(((int32_t *) vector_data)[row_idx]);
            break;
        case DUCKDB_TYPE_BIGINT:
            obj = LL2NUM(((int64_t *) vector_data)[row_idx]);
            break;
        case DUCKDB_TYPE_HUGEINT:
            obj = vector_hugeint(vector_data, row_idx);
            break;
        case DUCKDB_TYPE_FLOAT:
            obj = DBL2NUM((((float *) vector_data)[row_idx]));
            break;
        case DUCKDB_TYPE_DOUBLE:
            obj = DBL2NUM((((double *) vector_data)[row_idx]));
            break;
        case DUCKDB_TYPE_DATE:
            obj = vector_date(vector_data, row_idx);
            break;
        case DUCKDB_TYPE_TIMESTAMP:
            obj = vector_timestamp(vector_data, row_idx);
            break;
        case DUCKDB_TYPE_INTERVAL:
            obj = vector_interval(vector_data, row_idx);
            break;
        case DUCKDB_TYPE_VARCHAR:
            obj = vector_varchar(vector_data, row_idx);
            break;
        case DUCKDB_TYPE_BLOB:
            obj = vector_blob(vector_data, row_idx);
            break;
        case DUCKDB_TYPE_DECIMAL:
            obj = vector_decimal(ty, vector_data, row_idx);
            break;
        case DUCKDB_TYPE_ENUM:
            obj = vector_enum(ty, vector_data, row_idx);
            break;
        case DUCKDB_TYPE_LIST:
            obj = vector_list(ty, vector_data, row_idx);
            break;
        case DUCKDB_TYPE_MAP:
            obj = vector_map(ty, vector_data, row_idx);
            break;
        case DUCKDB_TYPE_STRUCT:
            obj = vector_struct(ty, vector_data, row_idx);
            break;
        case DUCKDB_TYPE_UUID:
            obj = vector_uuid(vector_data, row_idx);
            break;
        default:
            rb_warn("Unknown type %d", type_id);
            obj = Qnil;
    }

    duckdb_destroy_logical_type(&ty);
    return obj;
}

static VALUE duckdb_result_chunk_each(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    VALUE row;
    idx_t col_count;
    idx_t row_count;
    idx_t chunk_count;
    idx_t col_idx;
    idx_t row_idx;
    idx_t chunk_idx;
    duckdb_data_chunk chunk;
    duckdb_vector vector;
    VALUE val;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    col_count = duckdb_column_count(&(ctx->result));
    chunk_count = duckdb_result_chunk_count(ctx->result);

    RETURN_ENUMERATOR(oDuckDBResult, 0, 0);

    for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
        chunk = duckdb_result_get_chunk(ctx->result, chunk_idx);
        row_count = duckdb_data_chunk_get_size(chunk);
        for (row_idx = 0; row_idx < row_count; row_idx++) {
            row = rb_ary_new2(col_count);
            for (col_idx = 0; col_idx < col_count; col_idx++) {
                vector = duckdb_data_chunk_get_vector(chunk, col_idx);
                val = vector_value(vector, row_idx);
                rb_ary_store(row, col_idx, val);
            }
            rb_yield(row);
        }
        duckdb_destroy_data_chunk(&chunk);
    }
    return Qnil;
}
#endif

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
    rb_define_private_method(cDuckDBResult, "__to_hugeint_internal", duckdb_result___to_hugeint_internal, 2);
    rb_define_private_method(cDuckDBResult, "__to_decimal_internal", duckdb_result___to_decimal_internal, 2);
    rb_define_private_method(cDuckDBResult, "_to_float", duckdb_result__to_float, 2);
    rb_define_private_method(cDuckDBResult, "_to_double", duckdb_result__to_double, 2);
    rb_define_private_method(cDuckDBResult, "_to_string", duckdb_result__to_string, 2);
    rb_define_private_method(cDuckDBResult, "_to_string_internal", duckdb_result__to_string_internal, 2);
    rb_define_private_method(cDuckDBResult, "_to_blob", duckdb_result__to_blob, 2);
    rb_define_private_method(cDuckDBResult, "_enum_internal_type", duckdb_result__enum_internal_type, 1);
    rb_define_private_method(cDuckDBResult, "_enum_dictionary_size", duckdb_result__enum_dictionary_size, 1);
    rb_define_private_method(cDuckDBResult, "_enum_dictionary_value", duckdb_result__enum_dictionary_value, 2);
#ifdef HAVE_DUCKDB_H_GE_V080
    rb_define_method(cDuckDBResult, "chunk_each", duckdb_result_chunk_each, 0);
#endif
}
