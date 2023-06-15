#include "ruby-duckdb.h"

static VALUE cDuckDBResult;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE vector_value(duckdb_vector vector, idx_t row_idx);
static VALUE vector_blob(void* vector_data, idx_t row_idx);
static VALUE vector_date(void *vector_data, idx_t row_idx);
static VALUE vector_decimal(duckdb_logical_type ty, void* vector_data, idx_t row_idx);
static VALUE vector_list(duckdb_vector vector, idx_t row_idx);
static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_timestamp(void* vector_data, idx_t row_idx);

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
    // FIXME do I need to allocate the columns pointer separately?
    rubyDuckDBResult *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBResult));
    return TypedData_Wrap_Struct(klass, &result_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBResult);
}

static VALUE rb_duckdb_rows(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    idx_t col_count = duckdb_column_count(&(ctx->result));
    idx_t chunk_count = duckdb_result_chunk_count(ctx->result);

    RETURN_ENUMERATOR(oDuckDBResult, 0, 0);

    for (idx_t chunk_idx = 0; chunk_idx < chunk_count; ++chunk_idx) {
        duckdb_data_chunk data = duckdb_result_get_chunk(ctx->result, chunk_idx);
        idx_t row_count = duckdb_data_chunk_get_size(data);

        for (idx_t row_idx = 0; row_idx < row_count; ++row_idx) {
            VALUE result = rb_ary_new2(col_count);

            for (idx_t col_idx = 0; col_idx < col_count; ++col_idx) {
                duckdb_vector vector = duckdb_data_chunk_get_vector(data, col_idx);
                VALUE val = vector_value(vector, row_idx);
                rb_ary_push(result, val);
            }
            rb_yield(result);
        }
    }

    return Qnil;
}

rubyDuckDBResult *get_struct_result(VALUE obj) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBResult, &result_data_type, ctx);
    return ctx;
}

VALUE create_result(void) {
    return allocate(cDuckDBResult);
}

void init_duckdb_result(void) {
    cDuckDBResult = rb_define_class_under(mDuckDB, "Result", rb_cObject);
    rb_define_alloc_func(cDuckDBResult, allocate);

    /* rb_define_method(cDuckDBResult, "column_count", duckdb_result_column_count, 0); */
    /* rb_define_method(cDuckDBResult, "row_count", duckdb_result_row_count, 0); */
    /* rb_define_method(cDuckDBResult, "rows_changed", duckdb_result_rows_changed, 0); */
    /* rb_define_method(cDuckDBResult, "columns", duckdb_result_columns, 0); */
    rb_define_method(cDuckDBResult, "each", rb_duckdb_rows, 0);
}

static VALUE vector_value(duckdb_vector vector, idx_t row_idx) {
    uint64_t *validity = duckdb_vector_get_validity(vector);
    if (!duckdb_validity_row_is_valid(validity, row_idx)) {
        return Qnil;
    }

    // FIXME - we need to free this logical type
    duckdb_logical_type ty = duckdb_vector_get_column_type(vector);
    duckdb_type type_id = duckdb_get_type_id(ty);
    void* vector_data = duckdb_vector_get_data(vector);

    switch(type_id) {
        case DUCKDB_TYPE_INVALID:
            return Qnil;
        case DUCKDB_TYPE_BOOLEAN:
            return (((bool*) vector_data)[row_idx]) ? Qtrue : Qfalse;
        case DUCKDB_TYPE_TINYINT:
            return LL2NUM(((int8_t *) vector_data)[row_idx]);
        case DUCKDB_TYPE_SMALLINT:
            return LL2NUM(((int16_t *) vector_data)[row_idx]);
        case DUCKDB_TYPE_INTEGER:
            return LL2NUM(((int32_t *) vector_data)[row_idx]);
        case DUCKDB_TYPE_BIGINT:
            return LL2NUM(((int64_t *) vector_data)[row_idx]);
        case DUCKDB_TYPE_HUGEINT:
            // not done
        case DUCKDB_TYPE_FLOAT:
            return rb_float_new((((float *) vector_data)[row_idx]));
        case DUCKDB_TYPE_DOUBLE:
            return rb_float_new((((double *) vector_data)[row_idx]));
        case DUCKDB_TYPE_DATE:
            return vector_date(vector_data, row_idx);
        case DUCKDB_TYPE_TIMESTAMP:
            return vector_timestamp(vector_data, row_idx);
        case DUCKDB_TYPE_INTERVAL:
            // not done
        case DUCKDB_TYPE_VARCHAR:
        case DUCKDB_TYPE_BLOB:
            return vector_blob(vector_data, row_idx);
        case DUCKDB_TYPE_DECIMAL:
            return vector_decimal(ty, vector_data, row_idx);
        case DUCKDB_TYPE_LIST:
            return vector_list(vector, row_idx);
        case DUCKDB_TYPE_MAP:
        case DUCKDB_TYPE_STRUCT:
            return vector_struct(ty, vector, row_idx);
        case DUCKDB_TYPE_UUID:
            // this is the same as the hugeint, we just flip the sign bit
        default:
            return Qnil;
    }
}

const int stringInlineLength = 12;

static VALUE vector_timestamp(void* vector_data, idx_t row_idx) {
    duckdb_timestamp ts = ((duckdb_timestamp *)vector_data)[row_idx];
    return rb_time_new(ts.micros, 0);
}

static VALUE vector_blob(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    if(duckdb_string_is_inlined(s)) {
        return rb_str_new2(s.value.inlined.inlined);
    } else {
        return rb_str_new2(s.value.pointer.ptr);
    }
}

static VALUE vector_date(void *vector_data, idx_t row_idx) {
    duckdb_date_struct date = duckdb_from_date(((duckdb_date *) vector_data)[row_idx]);
    VALUE cTime = rb_const_get(rb_cObject, rb_intern("Time"));
    VALUE time = rb_funcall(
        cTime,
        rb_intern("new"),
        7,
        LL2NUM(date.year),  // Year
        LL2NUM(date.month), // Month
        LL2NUM(date.day),   // Day
        LL2NUM(0),                  // Hour
        LL2NUM(0),                  // Min
        LL2NUM(0),                  // Sec
        LL2NUM(0)                   // Zone - UTC
    );

    return time;
}

static VALUE vector_decimal(duckdb_logical_type ty, void* vector_data, idx_t row_idx) {
    uint8_t scale = duckdb_decimal_scale(ty);
    uint8_t width = duckdb_decimal_scale(ty);
    duckdb_hugeint value;
    switch(duckdb_decimal_internal_type(ty)) {
        case DUCKDB_TYPE_SMALLINT:
            value.lower = ((uint16_t *) vector_data)[row_idx];
        case DUCKDB_TYPE_INTEGER:
            value.lower = ((uint64_t *) vector_data)[row_idx];
        case DUCKDB_TYPE_BIGINT:
            value.lower = ((uint64_t *) vector_data)[row_idx];
        case DUCKDB_TYPE_HUGEINT:
            value = ((duckdb_hugeint *) vector_data)[row_idx];
        default:
            return 0;
    }
    duckdb_decimal decimal = { width, scale, value };
    /* return float64(C.duckdb_decimal_to_double(decimal)), nil */
    // we need to pass the digits to big decimal for this to work
    return rb_funcall(rb_mKernel,rb_intern("BigDecimal"), duckdb_decimal_to_double(decimal), 0);
}

static VALUE vector_list(duckdb_vector vector, idx_t row_idx) {
   // Lists are stored as vectors within vectors
   duckdb_vector child_vector = duckdb_list_vector_get_child(vector);
   void* vector_data = duckdb_vector_get_data(vector);
   duckdb_list_entry list_entry = ((duckdb_list_entry *)vector_data)[row_idx];
   VALUE converted = rb_ary_new2(list_entry.length);

   for (idx_t i = list_entry.offset; i < list_entry.offset + list_entry.length; ++i) {
       VALUE child = vector_value(child_vector, i);
       rb_ary_push(converted, child);
   }

   return converted;
}

static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    VALUE hash = rb_hash_new();
    idx_t child_count = duckdb_struct_type_child_count(ty);
    for (idx_t i = 0; i < child_count; ++i) {
        VALUE key = rb_str_new2(duckdb_struct_type_child_name(ty, i));
        duckdb_vector child_vector = duckdb_struct_vector_get_child(vector, i);
        VALUE value = vector_value(child_vector, i);
        rb_hash_aset(hash, key, value);
    }

    return hash;
}
