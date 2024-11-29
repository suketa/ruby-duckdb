#include "ruby-duckdb.h"

struct chunk_arg {
    duckdb_data_chunk chunk;
    idx_t col_count;
};

static VALUE cDuckDBResult;
static ID id__to_date;
static ID id__to_time;
static ID id__to_time_from_duckdb_time;
static ID id__to_interval_from_vector;
static ID id__to_hugeint_from_vector;
static ID id__to_decimal_from_hugeint;
static ID id__to_uuid_from_vector;
static ID id__to_time_from_duckdb_timestamp_s;
static ID id__to_time_from_duckdb_timestamp_ms;
static ID id__to_time_from_duckdb_timestamp_ns;
static ID id__to_time_from_duckdb_time_tz;
static ID id__to_time_from_duckdb_timestamp_tz;
static ID id__to_infinity;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_result_column_count(VALUE oDuckDBResult);
static VALUE duckdb_result_row_count(VALUE oDuckDBResult);
static VALUE duckdb_result_rows_changed(VALUE oDuckDBResult);
static VALUE duckdb_result_columns(VALUE oDuckDBResult);
static VALUE duckdb_result_streaming_p(VALUE oDuckDBResult);
static VALUE destroy_data_chunk(VALUE arg);
static VALUE duckdb_result_chunk_each(VALUE oDuckDBResult);

static VALUE duckdb_result__chunk_stream(VALUE oDuckDBResult);
static VALUE yield_rows(VALUE arg);
static VALUE duckdb_result__column_type(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__return_type(VALUE oDuckDBResult);
static VALUE duckdb_result__statement_type(VALUE oDuckDBResult);
static VALUE duckdb_result__enum_internal_type(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__enum_dictionary_size(VALUE oDuckDBResult, VALUE col_idx);
static VALUE duckdb_result__enum_dictionary_value(VALUE oDuckDBResult, VALUE col_idx, VALUE idx);

static VALUE infinite_date_value(duckdb_date date);
static VALUE vector_date(void *vector_data, idx_t row_idx);
static VALUE vector_timestamp(void* vector_data, idx_t row_idx);
static VALUE vector_time(void* vector_data, idx_t row_idx);
static VALUE vector_interval(void* vector_data, idx_t row_idx);
static VALUE vector_blob(void* vector_data, idx_t row_idx);
static VALUE vector_varchar(void* vector_data, idx_t row_idx);
static VALUE vector_hugeint(void* vector_data, idx_t row_idx);
static VALUE vector_uhugeint(void* vector_data, idx_t row_idx);
static VALUE vector_decimal(duckdb_logical_type ty, void* vector_data, idx_t row_idx);
static VALUE infinite_timestamp_value(duckdb_timestamp timestamp);
static VALUE vector_timestamp_s(void* vector_data, idx_t row_idx);
static VALUE vector_timestamp_ms(void* vector_data, idx_t row_idx);
static VALUE vector_timestamp_ns(void* vector_data, idx_t row_idx);
static VALUE vector_enum(duckdb_logical_type ty, void* vector_data, idx_t row_idx);
static VALUE vector_array(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index);
static VALUE vector_list(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx);
static VALUE vector_map(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx);
static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_union(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx);
static VALUE vector_bit(void* vector_data, idx_t row_idx);
static VALUE vector_time_tz(void* vector_data, idx_t row_idx);
static VALUE vector_timestamp_tz(void* vector_data, idx_t row_idx);
static VALUE vector_uuid(void* vector_data, idx_t row_idx);
static VALUE vector_value(duckdb_vector vector, idx_t row_idx);

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

#ifdef DUCKDB_API_NO_DEPRECATED
    return Qnil;
#else
    rubyDuckDBResult *ctx;
    rb_warn("`row_count` will be deprecated in the future.");
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_row_count(&(ctx->result)));
#endif
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
        VALUE column = rbduckdb_create_column(oDuckDBResult, col_idx);
        rb_ary_store(ary, col_idx, column);
    }
    return ary;
}

/*
 *  call-seq:
 *    result.streaming? -> Boolean
 *
 *  Returns true if the result is streaming, otherwise false.
 *
 */
static VALUE duckdb_result_streaming_p(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;

#ifdef DUCKDB_API_NO_DEPRECATED
    return Qtrue;
#else
    /* FIXME streaming is allways true. so this method is not useful and deprecated. */
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return duckdb_result_is_streaming(ctx->result) ? Qtrue : Qfalse;
#endif
}

static VALUE destroy_data_chunk(VALUE arg) {
    struct chunk_arg *p = (struct chunk_arg *)arg;
    duckdb_destroy_data_chunk(&(p->chunk));
    return Qnil;
}

static VALUE duckdb_result_chunk_each(VALUE oDuckDBResult) {
/*
#ifdef HAVE_DUCKDB_H_GE_V1_0_0
    return duckdb_result__chunk_stream(oDuckDBResult);
#else
*/
    rubyDuckDBResult *ctx;
    struct chunk_arg arg;
    idx_t chunk_count;
    idx_t chunk_idx;

#ifdef DUCKDB_API_NO_DEPRECATED
    //TODO: use duckdb_fetch_chunk instead of duckdb_result_chunk_count and duckdb_result_get_chunk.
    // duckdb_result_chunk_count will be deprecated in the future.
    // duckdb_result_get_chunk will be deprecated in the future.
#else
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    arg.col_count = duckdb_column_count(&(ctx->result));
    chunk_count = duckdb_result_chunk_count(ctx->result);

    RETURN_ENUMERATOR(oDuckDBResult, 0, 0);

    for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
        arg.chunk = duckdb_result_get_chunk(ctx->result, chunk_idx);
        rb_ensure(yield_rows, (VALUE)&arg, destroy_data_chunk, (VALUE)&arg);
    }
#endif
    return Qnil;
/*
#endif
*/
}

static VALUE duckdb_result__chunk_stream(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    struct chunk_arg arg;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    RETURN_ENUMERATOR(oDuckDBResult, 0, 0);

    arg.col_count = duckdb_column_count(&(ctx->result));

#ifdef HAVE_DUCKDB_H_GE_V1_0_0
    while((arg.chunk = duckdb_fetch_chunk(ctx->result)) != NULL) {
#else
    while((arg.chunk = duckdb_stream_fetch_chunk(ctx->result)) != NULL) {
#endif
        rb_ensure(yield_rows, (VALUE)&arg, destroy_data_chunk, (VALUE)&arg);
    }
    return Qnil;
}

static VALUE yield_rows(VALUE arg) {
    idx_t row_count;
    idx_t row_idx;
    idx_t col_idx;
    duckdb_vector vector;
    VALUE row;
    VALUE val;

    struct chunk_arg *p = (struct chunk_arg *)arg;

    row_count = duckdb_data_chunk_get_size(p->chunk);
    for (row_idx = 0; row_idx < row_count; row_idx++) {
        row = rb_ary_new2(p->col_count);
        for (col_idx = 0; col_idx < p->col_count; col_idx++) {
            vector = duckdb_data_chunk_get_vector(p->chunk, col_idx);
            val = vector_value(vector, row_idx);
            rb_ary_store(row, col_idx, val);
        }
        rb_yield(row);
    }
    return Qnil;
}

static VALUE duckdb_result__column_type(VALUE oDuckDBResult, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_column_type(&(ctx->result), NUM2LL(col_idx)));
}

static VALUE duckdb_result__return_type(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
/*
 * remove this #if ... #else statement when dropping duckdb 1.1.0.
 */
#if !defined(HAVE_DUCKDB_H_GE_V1_1_1) && defined(HAVE_DUCKDB_H_GE_V1_1_0) && defined(DUCKDB_API_NO_DEPRECATED)
    rb_raise(eDuckDBError, "duckdb_result_return_type C-API is not available with duckdb v1.1.0 with enabled DUCKDB_API_NO_DEPRECATED.");
#else
    return INT2FIX(duckdb_result_return_type(ctx->result));
#endif
}

static VALUE duckdb_result__statement_type(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return INT2FIX(duckdb_result_statement_type(ctx->result));
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
    duckdb_destroy_logical_type(&logical_type);
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
    duckdb_destroy_logical_type(&logical_type);
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
    duckdb_destroy_logical_type(&logical_type);
    return value;
}

VALUE rbduckdb_create_result(void) {
    return allocate(cDuckDBResult);
}

static VALUE infinite_date_value(duckdb_date date) {
    if (duckdb_is_finite_date(date) == false) {
        return rb_funcall(mDuckDBConverter, id__to_infinity, 1,
                          INT2NUM(date.days)
                         );
    }
    return Qnil;
}

static VALUE vector_date(void *vector_data, idx_t row_idx) {
    duckdb_date date = ((duckdb_date *) vector_data)[row_idx];
    VALUE obj = infinite_date_value(date);

    if (obj == Qnil) {
        duckdb_date_struct date_st = duckdb_from_date(date);
        obj = rb_funcall(mDuckDBConverter, id__to_date, 3,
                         INT2FIX(date_st.year),
                         INT2FIX(date_st.month),
                         INT2FIX(date_st.day)
                         );
    }
    return obj;
}

static VALUE vector_timestamp(void* vector_data, idx_t row_idx) {
    duckdb_timestamp data = ((duckdb_timestamp *)vector_data)[row_idx];
    VALUE obj = infinite_timestamp_value(data);

    if (obj == Qnil) {
        duckdb_timestamp_struct data_st = duckdb_from_timestamp(data);
        return rb_funcall(mDuckDBConverter, id__to_time, 7,
                          INT2FIX(data_st.date.year),
                          INT2FIX(data_st.date.month),
                          INT2FIX(data_st.date.day),
                          INT2FIX(data_st.time.hour),
                          INT2FIX(data_st.time.min),
                          INT2FIX(data_st.time.sec),
                          INT2NUM(data_st.time.micros)
                          );
    }
    return obj;
}

static VALUE vector_time(void* vector_data, idx_t row_idx) {
    duckdb_time_struct data = duckdb_from_time(((duckdb_time *)vector_data)[row_idx]);
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_time, 4,
                      INT2FIX(data.hour),
                      INT2FIX(data.min),
                      INT2FIX(data.sec),
                      INT2NUM(data.micros)
                      );
}


static VALUE vector_interval(void* vector_data, idx_t row_idx) {
    duckdb_interval data = ((duckdb_interval *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_interval_from_vector, 3,
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
        return rb_utf8_str_new(s.value.inlined.inlined, s.value.inlined.length);
    } else {
        return rb_utf8_str_new(s.value.pointer.ptr, s.value.pointer.length);
    }
}

static VALUE vector_hugeint(void* vector_data, idx_t row_idx) {
    duckdb_hugeint hugeint = ((duckdb_hugeint *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_hugeint_from_vector, 2,
                      ULL2NUM(hugeint.lower),
                      LL2NUM(hugeint.upper)
                      );
}

static VALUE vector_uhugeint(void* vector_data, idx_t row_idx) {
    duckdb_uhugeint uhugeint = ((duckdb_uhugeint *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_hugeint_from_vector, 2,
                      ULL2NUM(uhugeint.lower),
                      ULL2NUM(uhugeint.upper)
                      );
}

static VALUE vector_decimal(duckdb_logical_type ty, void* vector_data, idx_t row_idx) {
    VALUE width = INT2FIX(duckdb_decimal_width(ty));
    VALUE scale = INT2FIX(duckdb_decimal_scale(ty));
    duckdb_type type = duckdb_decimal_internal_type(ty);
    duckdb_hugeint value;
    VALUE upper = Qnil;
    VALUE lower = Qnil;

    value.upper = 0;
    value.lower = 0;

    switch(type) {
        case DUCKDB_TYPE_HUGEINT:
            value = ((duckdb_hugeint *) vector_data)[row_idx];
            upper = LL2NUM(value.upper);
            lower = ULL2NUM(value.lower);
            break;
        case DUCKDB_TYPE_SMALLINT:
            upper = INT2FIX(((int16_t *) vector_data)[row_idx]);
            break;
        case DUCKDB_TYPE_INTEGER:
            upper = INT2NUM(((int32_t *) vector_data)[row_idx]);
            break;
        case DUCKDB_TYPE_BIGINT:
            upper = LL2NUM(((int64_t *) vector_data)[row_idx]);
            break;
        default:
            rb_warn("Unknown decimal internal type %d", type);
    }

    return rb_funcall(mDuckDBConverter, id__to_decimal_from_hugeint, 4,
                      width,
                      scale,
                      upper,
                      lower
                      );
}

static VALUE infinite_timestamp_value(duckdb_timestamp timestamp) {
    if (duckdb_is_finite_timestamp(timestamp) == false) {
        return rb_funcall(mDuckDBConverter, id__to_infinity, 1,
                          LL2NUM(timestamp.micros)
                         );
    }
    return Qnil;
}

static VALUE vector_timestamp_s(void* vector_data, idx_t row_idx) {
    duckdb_timestamp data = ((duckdb_timestamp *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_s, 1,
                      LL2NUM(data.micros)
                     );
}

static VALUE vector_timestamp_ms(void* vector_data, idx_t row_idx) {
    duckdb_timestamp data = ((duckdb_timestamp *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_ms, 1,
                      LL2NUM(data.micros)
                      );
}

static VALUE vector_timestamp_ns(void* vector_data, idx_t row_idx) {
    duckdb_timestamp data = ((duckdb_timestamp *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_ns, 1,
                      LL2NUM(data.micros)
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

static VALUE vector_array(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    VALUE ary = Qnil;
    VALUE value = Qnil;

    duckdb_logical_type child_logical_type = duckdb_array_type_child_type(ty);
    idx_t size = duckdb_array_type_array_size(ty);
    idx_t bgn = row_idx * size;
    idx_t end = bgn + size;
	duckdb_vector child = duckdb_array_vector_get_child(vector);

    ary = rb_ary_new2(size);
    for (idx_t i = bgn; i < end; ++i) {
        value = vector_value_at(child, child_logical_type, i);
        rb_ary_store(ary, i - bgn, value);
    }

    duckdb_destroy_logical_type(&child_logical_type);
    return ary;
}

static VALUE vector_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index) {
    uint64_t *validity;
    duckdb_type type_id;
    void* vector_data;
    VALUE obj = Qnil;

    validity = duckdb_vector_get_validity(vector);
    if (!duckdb_validity_row_is_valid(validity, index)) {
        return Qnil;
    }

    type_id = duckdb_get_type_id(element_type);
    vector_data = duckdb_vector_get_data(vector);

    switch(type_id) {
        case DUCKDB_TYPE_INVALID:
            obj = Qnil;
            break;
        case DUCKDB_TYPE_BOOLEAN:
            obj = (((bool*) vector_data)[index]) ? Qtrue : Qfalse;
            break;
        case DUCKDB_TYPE_TINYINT:
            obj = INT2FIX(((int8_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_SMALLINT:
            obj = INT2FIX(((int16_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_INTEGER:
            obj = INT2NUM(((int32_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_BIGINT:
            obj = LL2NUM(((int64_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_UTINYINT:
            obj = INT2FIX(((uint8_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_USMALLINT:
            obj = INT2FIX(((uint16_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_UINTEGER:
            obj = UINT2NUM(((uint32_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_UBIGINT:
            obj = ULL2NUM(((uint64_t *) vector_data)[index]);
            break;
        case DUCKDB_TYPE_FLOAT:
            obj = DBL2NUM((((float *) vector_data)[index]));
            break;
        case DUCKDB_TYPE_DOUBLE:
            obj = DBL2NUM((((double *) vector_data)[index]));
            break;
        case DUCKDB_TYPE_TIMESTAMP:
            obj = vector_timestamp(vector_data, index);
            break;
        case DUCKDB_TYPE_DATE:
            obj = vector_date(vector_data, index);
            break;
        case DUCKDB_TYPE_TIME:
            obj = vector_time(vector_data, index);
            break;
        case DUCKDB_TYPE_INTERVAL:
            obj = vector_interval(vector_data, index);
            break;
        case DUCKDB_TYPE_HUGEINT:
            obj = vector_hugeint(vector_data, index);
            break;
        case DUCKDB_TYPE_UHUGEINT:
            obj = vector_uhugeint(vector_data, index);
            break;
        case DUCKDB_TYPE_VARCHAR:
            obj = vector_varchar(vector_data, index);
            break;
        case DUCKDB_TYPE_BLOB:
            obj = vector_blob(vector_data, index);
            break;
        case DUCKDB_TYPE_DECIMAL:
            obj = vector_decimal(element_type, vector_data, index);
            break;
        case DUCKDB_TYPE_TIMESTAMP_S:
            obj = vector_timestamp_s(vector_data, index);
            break;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            obj = vector_timestamp_ms(vector_data, index);
            break;
        case DUCKDB_TYPE_TIMESTAMP_NS:
            obj = vector_timestamp_ns(vector_data, index);
            break;
        case DUCKDB_TYPE_ENUM:
            obj = vector_enum(element_type, vector_data, index);
            break;
        case DUCKDB_TYPE_LIST:
            obj = vector_list(element_type, vector, vector_data, index);
            break;
        case DUCKDB_TYPE_STRUCT:
            obj = vector_struct(element_type, vector, index);
            break;
        case DUCKDB_TYPE_MAP:
            obj = vector_map(element_type, vector, vector_data, index);
            break;
        case DUCKDB_TYPE_ARRAY:
            obj = vector_array(element_type, vector, index);
            break;
        case DUCKDB_TYPE_UUID:
            obj = vector_uuid(vector_data, index);
            break;
        case DUCKDB_TYPE_UNION:
            obj = vector_union(element_type, vector, vector_data, index);
            break;
        case DUCKDB_TYPE_BIT:
            obj = vector_bit(vector_data, index);
            break;
        case DUCKDB_TYPE_TIME_TZ:
            obj = vector_time_tz(vector_data, index);
            break;
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            obj = vector_timestamp_tz(vector_data, index);
            break;
        default:
            rb_warn("Unknown type %d", type_id);
            obj = Qnil;
    }

    return obj;
}

static VALUE vector_list(duckdb_logical_type ty, duckdb_vector vector, void * vector_data, idx_t row_idx) {
    VALUE ary = Qnil;
    VALUE value = Qnil;
    idx_t i;

    duckdb_logical_type child_logical_type = duckdb_list_type_child_type(ty);

    duckdb_list_entry list_entry = ((duckdb_list_entry *)vector_data)[row_idx];
    idx_t bgn = list_entry.offset;
    idx_t end = bgn + list_entry.length;
    ary = rb_ary_new2(list_entry.length);

    duckdb_vector child = duckdb_list_vector_get_child(vector);

    for (i = bgn; i < end; ++i) {
        value = vector_value_at(child, child_logical_type, i);
        rb_ary_store(ary, i - bgn, value);
    }
    duckdb_destroy_logical_type(&child_logical_type);
    return ary;
}

static VALUE vector_map(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx) {
    VALUE hash = rb_hash_new();
    VALUE key = Qnil;
    VALUE value = Qnil;

    duckdb_logical_type key_logical_type = duckdb_map_type_key_type(ty);
    duckdb_logical_type value_logical_type = duckdb_map_type_value_type(ty);

    duckdb_list_entry list_entry = ((duckdb_list_entry *)vector_data)[row_idx];

    idx_t bgn = list_entry.offset;
    idx_t end = bgn + list_entry.length;

    duckdb_vector child = duckdb_list_vector_get_child(vector);
    duckdb_vector key_vector = duckdb_struct_vector_get_child(child, 0);
    duckdb_vector value_vector = duckdb_struct_vector_get_child(child, 1);

    for (idx_t i = bgn; i < end; ++i) {
        key = vector_value_at(key_vector, key_logical_type, i);
        value = vector_value_at(value_vector, value_logical_type, i);
        rb_hash_aset(hash, key, value);
    }

    duckdb_destroy_logical_type(&key_logical_type);
    duckdb_destroy_logical_type(&value_logical_type);
    return hash;
}

static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    VALUE hash = rb_hash_new();
    VALUE value = Qnil;
    VALUE key = Qnil;
    duckdb_vector child;
    duckdb_logical_type child_type;
    char *p;

    idx_t child_count = duckdb_struct_type_child_count(ty);

    for (idx_t i = 0; i < child_count; ++i) {
        p = duckdb_struct_type_child_name(ty, i);
        if (p) {
            key = ID2SYM(rb_intern_const(p));
            child = duckdb_struct_vector_get_child(vector, i);
            child_type = duckdb_struct_type_child_type(ty, i);
            value = vector_value_at(child, child_type, row_idx);
            rb_hash_aset(hash, key, value);
            duckdb_destroy_logical_type(&child_type);
            duckdb_free(p);
        }
    }

    return hash;
}

static VALUE vector_union(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx){
    VALUE value = Qnil;
    duckdb_vector type_vector = duckdb_struct_vector_get_child(vector, 0);
    void *data = duckdb_vector_get_data(type_vector);
    uint8_t index = ((int8_t *)data)[row_idx];

    duckdb_logical_type  child_type = duckdb_union_type_member_type(ty, index);

    duckdb_vector vector_value = duckdb_struct_vector_get_child(vector, index + 1);
    value = vector_value_at(vector_value, child_type, row_idx);
    duckdb_destroy_logical_type(&child_type);
    return value;
}

static VALUE str_concat_byte(VALUE str, unsigned char byte, int offset) {
    char x[8];
    char *p = x;
    for (int j = 7; j >=0; j--) {
        if (byte % 2 == 1) {
            x[j] = '1';
        } else {
            x[j] = '0';
        }
        byte = (byte >> 1);
    }
    if (offset > 0 && offset < 8) {
        p = x + offset;
    }
    return rb_str_cat(str, p, 8 - offset);
}

static VALUE bytes_to_string(char *bytes, uint32_t length, int offset) {
    VALUE str = rb_str_new_literal("");
    str = str_concat_byte(str, bytes[0], offset);
    for (uint32_t i = 1; i < length; i++) {
        str = str_concat_byte(str, bytes[i], 0);
    }
    return str;
}

static VALUE vector_bit(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    char *p;
    int offset;
    uint32_t length;
    VALUE str = Qnil;

    if(duckdb_string_is_inlined(s)) {
        length = s.value.inlined.length - 1;
        p = &s.value.inlined.inlined[1];
        offset = s.value.inlined.inlined[0];
        str = bytes_to_string(p, length, offset);
    } else {
        length = s.value.pointer.length - 1;
        p = &s.value.pointer.ptr[1];
        offset = s.value.pointer.ptr[0];
        str = bytes_to_string(p, length, offset);
    }
    return str;
}

static VALUE vector_time_tz(void* vector_data, idx_t row_idx) {
    duckdb_time_tz_struct data = duckdb_from_time_tz(((duckdb_time_tz *)vector_data)[row_idx]);
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_time_tz, 5,
                      INT2FIX(data.time.hour),
                      INT2FIX(data.time.min),
                      INT2FIX(data.time.sec),
                      INT2NUM(data.time.micros),
                      INT2NUM(data.offset)
                      );
}

static VALUE vector_timestamp_tz(void* vector_data, idx_t row_idx) {
    duckdb_time_tz data = ((duckdb_time_tz *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_time_from_duckdb_timestamp_tz, 1,
                      ULL2NUM(data.bits)
                      );
}

static VALUE vector_uuid(void* vector_data, idx_t row_idx) {
    duckdb_hugeint hugeint = ((duckdb_hugeint *)vector_data)[row_idx];
    return rb_funcall(mDuckDBConverter, id__to_uuid_from_vector, 2,
                      ULL2NUM(hugeint.lower),
                      LL2NUM(hugeint.upper)
                      );
}

static VALUE vector_value(duckdb_vector vector, idx_t row_idx) {
    duckdb_logical_type ty;
    VALUE obj = Qnil;

    ty = duckdb_vector_get_column_type(vector);

    obj = vector_value_at(vector, ty, row_idx);

    duckdb_destroy_logical_type(&ty);
    return obj;
}

void rbduckdb_init_duckdb_result(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBResult = rb_define_class_under(mDuckDB, "Result", rb_cObject);
    id__to_date = rb_intern("_to_date");
    id__to_time = rb_intern("_to_time");
    id__to_time_from_duckdb_time = rb_intern("_to_time_from_duckdb_time");
    id__to_interval_from_vector = rb_intern("_to_interval_from_vector");
    id__to_hugeint_from_vector = rb_intern("_to_hugeint_from_vector");
    id__to_decimal_from_hugeint = rb_intern("_to_decimal_from_hugeint");
    id__to_uuid_from_vector = rb_intern("_to_uuid_from_vector");
    id__to_time_from_duckdb_timestamp_s = rb_intern("_to_time_from_duckdb_timestamp_s");
    id__to_time_from_duckdb_timestamp_ms = rb_intern("_to_time_from_duckdb_timestamp_ms");
    id__to_time_from_duckdb_timestamp_ns = rb_intern("_to_time_from_duckdb_timestamp_ns");
    id__to_time_from_duckdb_time_tz = rb_intern("_to_time_from_duckdb_time_tz");
    id__to_time_from_duckdb_timestamp_tz = rb_intern("_to_time_from_duckdb_timestamp_tz");
    id__to_infinity = rb_intern("_to_infinity");

    rb_define_alloc_func(cDuckDBResult, allocate);

    rb_define_method(cDuckDBResult, "column_count", duckdb_result_column_count, 0);
    rb_define_method(cDuckDBResult, "row_count", duckdb_result_row_count, 0); /* deprecated */
    rb_define_method(cDuckDBResult, "rows_changed", duckdb_result_rows_changed, 0);
    rb_define_method(cDuckDBResult, "columns", duckdb_result_columns, 0);
    rb_define_method(cDuckDBResult, "streaming?", duckdb_result_streaming_p, 0);
    rb_define_method(cDuckDBResult, "chunk_each", duckdb_result_chunk_each, 0);
    rb_define_private_method(cDuckDBResult, "_chunk_stream", duckdb_result__chunk_stream, 0);
    rb_define_private_method(cDuckDBResult, "_column_type", duckdb_result__column_type, 1);
    rb_define_private_method(cDuckDBResult, "_return_type", duckdb_result__return_type, 0);
    rb_define_private_method(cDuckDBResult, "_statement_type", duckdb_result__statement_type, 0);

    rb_define_private_method(cDuckDBResult, "_enum_internal_type", duckdb_result__enum_internal_type, 1);
    rb_define_private_method(cDuckDBResult, "_enum_dictionary_size", duckdb_result__enum_dictionary_size, 1);
    rb_define_private_method(cDuckDBResult, "_enum_dictionary_value", duckdb_result__enum_dictionary_value, 2);
}
