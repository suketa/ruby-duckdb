#include "ruby-duckdb.h"
#include "ruby/encoding.h"

/*
 * Converts a cell of a DuckDB vector to a Ruby object.
 *
 * This is the single source of truth for "DuckDB value -> Ruby object"
 * conversion: query results, scalar/aggregate function arguments and
 * DuckDB::Value#to_ruby all go through rbduckdb_vector_value_at.
 */

struct owned_type_arg {
    duckdb_vector vector;
    duckdb_logical_type type;
    idx_t bgn;
    idx_t end;
};

struct struct_fields_arg {
    duckdb_logical_type ty;
    duckdb_vector vector;
    idx_t row_idx;
    duckdb_logical_type child_type;
};

struct map_entries_arg {
    duckdb_vector key_vector;
    duckdb_vector value_vector;
    duckdb_logical_type key_type;
    duckdb_logical_type value_type;
    idx_t bgn;
    idx_t end;
};

static VALUE vector_blob(void* vector_data, idx_t row_idx);
static VALUE vector_varchar(void* vector_data, idx_t row_idx);
static VALUE vector_decimal(duckdb_logical_type ty, void* vector_data, idx_t row_idx);

static VALUE vector_enum(duckdb_logical_type ty, void* vector_data, idx_t row_idx);
static VALUE vector_array(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_list(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx);
static VALUE vector_map(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx);
static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx);
static VALUE vector_union(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx);
static VALUE vector_bit(void* vector_data, idx_t row_idx);
static VALUE vector_bignum(void* vector_data, idx_t row_idx);
static VALUE vector_time_tz(void* vector_data, idx_t row_idx);
static VALUE vector_timestamp_tz(void* vector_data, idx_t row_idx);
static VALUE vector_uuid(void* vector_data, idx_t row_idx);
static VALUE convert_values_in_range(VALUE varg);
static VALUE destroy_owned_type(VALUE varg);
static VALUE vector_values_with_owned_type(duckdb_vector vector, duckdb_logical_type type, idx_t bgn, idx_t end);
static VALUE convert_map_entries(VALUE varg);
static VALUE destroy_map_types(VALUE varg);
static VALUE convert_struct_fields(VALUE varg);
static VALUE destroy_struct_child_type(VALUE varg);

static VALUE vector_blob(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    return rb_str_new(duckdb_string_t_data(&s), duckdb_string_t_length(s));
}

static VALUE vector_varchar(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    return rb_utf8_str_new(duckdb_string_t_data(&s), duckdb_string_t_length(s));
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

static VALUE vector_enum(duckdb_logical_type ty, void* vector_data, idx_t row_idx) {
    duckdb_type type = duckdb_enum_internal_type(ty);
    idx_t index;
    char *p;
    VALUE value = Qnil;

    switch(type) {
        case DUCKDB_TYPE_UTINYINT:
            index = ((uint8_t *) vector_data)[row_idx];
            break;
        case DUCKDB_TYPE_USMALLINT:
            index = ((uint16_t *) vector_data)[row_idx];
            break;
        case DUCKDB_TYPE_UINTEGER:
            index = ((uint32_t *) vector_data)[row_idx];
            break;
        default:
            rb_warn("Unknown enum internal type %d", type);
            return Qnil;
    }
    p = duckdb_enum_dictionary_value(ty, index);
    if (p) {
        value = rb_utf8_str_new_cstr(p);
        duckdb_free(p);
    }
    return value;
}

static VALUE vector_array(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    idx_t size = duckdb_array_type_array_size(ty);
    idx_t bgn = row_idx * size;
    duckdb_vector child = duckdb_array_vector_get_child(vector);

    return vector_values_with_owned_type(child, duckdb_array_type_child_type(ty), bgn, bgn + size);
}

VALUE rbduckdb_vector_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index) {
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
            obj = rbduckdb_timestamp_to_ruby(((duckdb_timestamp *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_DATE:
            obj = rbduckdb_date_to_ruby(((duckdb_date *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_TIME:
            obj = rbduckdb_time_to_ruby(((duckdb_time *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_INTERVAL:
            obj = rbduckdb_interval_to_ruby(((duckdb_interval *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_HUGEINT:
            obj = rbduckdb_hugeint_to_ruby(((duckdb_hugeint *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_UHUGEINT:
            obj = rbduckdb_uhugeint_to_ruby(((duckdb_uhugeint *)vector_data)[index]);
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
            obj = rbduckdb_timestamp_s_to_ruby(((duckdb_timestamp_s *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            obj = rbduckdb_timestamp_ms_to_ruby(((duckdb_timestamp_ms *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_TIMESTAMP_NS:
            obj = rbduckdb_timestamp_ns_to_ruby(((duckdb_timestamp_ns *)vector_data)[index]);
            break;
        case DUCKDB_TYPE_TIME_NS:
            obj = rbduckdb_time_ns_to_ruby(((duckdb_time_ns *)vector_data)[index]);
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
        case DUCKDB_TYPE_BIGNUM:
            obj = vector_bignum(vector_data, index);
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
    duckdb_list_entry list_entry = ((duckdb_list_entry *)vector_data)[row_idx];
    idx_t bgn = list_entry.offset;
    duckdb_vector child = duckdb_list_vector_get_child(vector);

    return vector_values_with_owned_type(child, duckdb_list_type_child_type(ty), bgn, bgn + list_entry.length);
}

static VALUE vector_map(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx) {
    struct map_entries_arg arg;
    duckdb_list_entry list_entry = ((duckdb_list_entry *)vector_data)[row_idx];
    duckdb_vector child = duckdb_list_vector_get_child(vector);

    arg.key_vector = duckdb_struct_vector_get_child(child, 0);
    arg.value_vector = duckdb_struct_vector_get_child(child, 1);
    arg.key_type = duckdb_map_type_key_type(ty);
    arg.value_type = duckdb_map_type_value_type(ty);
    arg.bgn = list_entry.offset;
    arg.end = list_entry.offset + list_entry.length;

    return rb_ensure(convert_map_entries, (VALUE)&arg, destroy_map_types, (VALUE)&arg);
}

static VALUE convert_map_entries(VALUE varg) {
    struct map_entries_arg *arg = (struct map_entries_arg *)varg;
    VALUE hash = rb_hash_new_capa((long)(arg->end - arg->bgn));
    VALUE key;
    VALUE value;

    for (idx_t i = arg->bgn; i < arg->end; ++i) {
        key = rbduckdb_vector_value_at(arg->key_vector, arg->key_type, i);
        value = rbduckdb_vector_value_at(arg->value_vector, arg->value_type, i);
        rb_hash_aset(hash, key, value);
    }
    return hash;
}

static VALUE destroy_map_types(VALUE varg) {
    struct map_entries_arg *arg = (struct map_entries_arg *)varg;

    duckdb_destroy_logical_type(&(arg->key_type));
    duckdb_destroy_logical_type(&(arg->value_type));
    return Qnil;
}

static VALUE vector_struct(duckdb_logical_type ty, duckdb_vector vector, idx_t row_idx) {
    struct struct_fields_arg arg = {ty, vector, row_idx, NULL};

    return rb_ensure(convert_struct_fields, (VALUE)&arg, destroy_struct_child_type, (VALUE)&arg);
}

static VALUE convert_struct_fields(VALUE varg) {
    struct struct_fields_arg *arg = (struct struct_fields_arg *)varg;
    idx_t child_count = duckdb_struct_type_child_count(arg->ty);
    VALUE hash = rb_hash_new_capa((long)child_count);
    VALUE value = Qnil;
    VALUE key = Qnil;
    duckdb_vector child;
    char *p;


    for (idx_t i = 0; i < child_count; ++i) {
        p = duckdb_struct_type_child_name(arg->ty, i);
        if (p) {
            key = ID2SYM(rb_intern3(p, (long)strlen(p), rb_utf8_encoding()));
            duckdb_free(p);
            child = duckdb_struct_vector_get_child(arg->vector, i);
            arg->child_type = duckdb_struct_type_child_type(arg->ty, i);
            value = rbduckdb_vector_value_at(child, arg->child_type, arg->row_idx);
            duckdb_destroy_logical_type(&(arg->child_type));
            rb_hash_aset(hash, key, value);
        }
    }

    return hash;
}

static VALUE destroy_struct_child_type(VALUE varg) {
    struct struct_fields_arg *arg = (struct struct_fields_arg *)varg;

    duckdb_destroy_logical_type(&(arg->child_type));
    return Qnil;
}

static VALUE vector_union(duckdb_logical_type ty, duckdb_vector vector, void* vector_data, idx_t row_idx){
    duckdb_vector type_vector = duckdb_struct_vector_get_child(vector, 0);
    void *data = duckdb_vector_get_data(type_vector);
    uint8_t index = ((int8_t *)data)[row_idx];

    duckdb_vector member_vector = duckdb_struct_vector_get_child(vector, index + 1);
    VALUE values = vector_values_with_owned_type(member_vector, duckdb_union_type_member_type(ty, index), row_idx, row_idx + 1);

    return rb_ary_entry(values, 0);
}

static VALUE convert_values_in_range(VALUE varg) {
    struct owned_type_arg *arg = (struct owned_type_arg *)varg;
    VALUE ary = rb_ary_new2(arg->end - arg->bgn);

    for (idx_t i = arg->bgn; i < arg->end; ++i) {
        rb_ary_store(ary, i - arg->bgn, rbduckdb_vector_value_at(arg->vector, arg->type, i));
    }
    return ary;
}

static VALUE destroy_owned_type(VALUE varg) {
    struct owned_type_arg *arg = (struct owned_type_arg *)varg;

    duckdb_destroy_logical_type(&(arg->type));
    return Qnil;
}

static VALUE vector_values_with_owned_type(duckdb_vector vector, duckdb_logical_type type, idx_t bgn, idx_t end) {
    struct owned_type_arg arg = {vector, type, bgn, end};

    return rb_ensure(convert_values_in_range, (VALUE)&arg, destroy_owned_type, (VALUE)&arg);
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

static VALUE bytes_to_string(const char *bytes, uint32_t length, int offset) {
    VALUE str = rb_str_new_literal("");
    str = str_concat_byte(str, bytes[0], offset);
    for (uint32_t i = 1; i < length; i++) {
        str = str_concat_byte(str, bytes[i], 0);
    }
    return str;
}

static VALUE vector_bit(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    const char *p = duckdb_string_t_data(&s);
    uint32_t length = duckdb_string_t_length(s);
    int offset;
    VALUE str = Qnil;

    if (length < 2 || (unsigned char)p[0] > 7) {
        rb_raise(eDuckDBError, "invalid BIT value");
    }
    offset = (unsigned char)p[0];
    str = bytes_to_string(p + 1, length - 1, offset);
    return str;
}

/*
 * BIGNUM is stored as a blob: a 3-byte big-endian header followed by the
 * big-endian magnitude bytes. The header holds the magnitude byte count
 * with its most significant bit (0x800000) set for non-negative numbers.
 * For negative numbers every byte (header and magnitude) is bit-inverted.
 */
static VALUE vector_bignum(void* vector_data, idx_t row_idx) {
    duckdb_string_t s = (((duckdb_string_t *)vector_data)[row_idx]);
    const unsigned char *p = (const unsigned char *)duckdb_string_t_data(&s);
    uint32_t length = duckdb_string_t_length(s);
    bool is_negative;
    uint32_t i;
    VALUE buf;
    char *bytes;
    VALUE result;

    if (length < 4) {
        rb_raise(eDuckDBError, "invalid BIGNUM value");
    }
    is_negative = (p[0] & 0x80) == 0;
    length -= 3;
    buf = rb_str_new(NULL, length);
    bytes = RSTRING_PTR(buf);
    for (i = 0; i < length; i++) {
        bytes[i] = (char)(is_negative ? (unsigned char)~p[i + 3] : p[i + 3]);
    }
    result = rb_integer_unpack(bytes, length, 1, 0, INTEGER_PACK_BIG_ENDIAN);
    RB_GC_GUARD(buf);
    if (is_negative) {
        result = rb_funcall(result, rb_intern("-@"), 0);
    }
    return result;
}

static VALUE vector_time_tz(void* vector_data, idx_t row_idx) {
    return rbduckdb_time_tz_to_ruby(((duckdb_time_tz *)vector_data)[row_idx]);
}

static VALUE vector_timestamp_tz(void* vector_data, idx_t row_idx) {
    return rbduckdb_timestamp_tz_to_ruby(((duckdb_timestamp *)vector_data)[row_idx]);
}

static VALUE vector_uuid(void* vector_data, idx_t row_idx) {
    return rbduckdb_uuid_to_ruby(((duckdb_hugeint *)vector_data)[row_idx]);
}

