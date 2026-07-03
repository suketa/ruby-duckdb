#include "ruby-duckdb.h"

VALUE cDuckDBValue;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE value_s__create_bool(VALUE klass, VALUE flag);
static VALUE value_s__create_int8(VALUE klass, VALUE val);
static VALUE value_s__create_int16(VALUE klass, VALUE val);
static VALUE value_s__create_int32(VALUE klass, VALUE val);
static VALUE value_s__create_int64(VALUE klass, VALUE val);
static VALUE value_s__create_uint8(VALUE klass, VALUE val);
static VALUE value_s__create_uint16(VALUE klass, VALUE val);
static VALUE value_s__create_uint32(VALUE klass, VALUE val);
static VALUE value_s__create_uint64(VALUE klass, VALUE val);
static VALUE value_s__create_float(VALUE klass, VALUE val);
static VALUE value_s__create_double(VALUE klass, VALUE val);
static VALUE value_s__create_varchar(VALUE klass, VALUE str);
static VALUE value_s__create_blob(VALUE klass, VALUE str);
static VALUE value_s__create_hugeint(VALUE klass, VALUE lower, VALUE upper);
static VALUE value_s__create_uhugeint(VALUE klass, VALUE lower, VALUE upper);
static VALUE value_s__create_decimal(VALUE klass, VALUE lower, VALUE upper, VALUE width, VALUE scale);
static VALUE value_s_create_null(VALUE klass);
static idx_t marshal_values(VALUE ary, duckdb_value **out, volatile VALUE *guard);
static VALUE value_s__create_list(VALUE klass, VALUE ltype, VALUE values);
static VALUE value_s__create_array(VALUE klass, VALUE ltype, VALUE values);
static VALUE value_s__create_struct(VALUE klass, VALUE ltype, VALUE values);
static VALUE value_s__create_map(VALUE klass, VALUE ltype, VALUE keys, VALUE values);
static VALUE value_list_size(VALUE self);
static VALUE value_list_child(VALUE self, VALUE vidx);
static VALUE value_struct_child(VALUE self, VALUE vidx);
static VALUE value_map_size(VALUE self);
static VALUE value_map_key(VALUE self, VALUE vidx);
static VALUE value_map_value(VALUE self, VALUE vidx);
static VALUE value_to_ruby(VALUE self);

static const rb_data_type_t value_data_type = {
    "DuckDB/Value",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBValue *p = (rubyDuckDBValue *)ctx;

    duckdb_destroy_value(&(p->value));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBValue *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBValue));
    return TypedData_Wrap_Struct(klass, &value_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBValue);
}

static VALUE value_s__create_bool(VALUE klass, VALUE flag) {
    duckdb_value value = duckdb_create_bool(RTEST(flag) ? true : false);
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_int8(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_int8((int8_t)NUM2INT(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_int16(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_int16((int16_t)NUM2INT(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_int32(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_int32(NUM2INT(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_int64(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_int64((int64_t)NUM2LL(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_uint8(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_uint8((uint8_t)NUM2UINT(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_uint16(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_uint16((uint16_t)NUM2UINT(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_uint32(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_uint32((uint32_t)NUM2UINT(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_uint64(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_uint64((uint64_t)RB_NUM2ULL(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_float(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_float((float)NUM2DBL(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_double(VALUE klass, VALUE val) {
    duckdb_value value = duckdb_create_double(NUM2DBL(val));
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_varchar(VALUE klass, VALUE str) {
    const char *str_ptr = StringValuePtr(str);
    idx_t str_len = RSTRING_LEN(str);
    duckdb_value value = duckdb_create_varchar_length(str_ptr, str_len);
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_blob(VALUE klass, VALUE str) {
    const uint8_t *data_ptr = (const uint8_t *)StringValuePtr(str);
    idx_t data_len = RSTRING_LEN(str);
    duckdb_value value = duckdb_create_blob(data_ptr, data_len);
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_hugeint(VALUE klass, VALUE lower, VALUE upper) {
    duckdb_hugeint hugeint;
    hugeint.lower = NUM2ULL(lower);
    hugeint.upper = NUM2LL(upper);
    duckdb_value value = duckdb_create_hugeint(hugeint);
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_uhugeint(VALUE klass, VALUE lower, VALUE upper) {
    duckdb_uhugeint uhugeint;
    uhugeint.lower = NUM2ULL(lower);
    uhugeint.upper = NUM2ULL(upper);
    duckdb_value value = duckdb_create_uhugeint(uhugeint);
    return rbduckdb_value_new(value);
}

static VALUE value_s__create_decimal(VALUE klass, VALUE lower, VALUE upper, VALUE width, VALUE scale) {
    duckdb_decimal decimal;
    decimal.value.lower = NUM2ULL(lower);
    decimal.value.upper = NUM2LL(upper);
    decimal.width = (uint8_t)NUM2UINT(width);
    decimal.scale = (uint8_t)NUM2UINT(scale);
    duckdb_value value = duckdb_create_decimal(decimal);
    return rbduckdb_value_new(value);
}

/*
 * call-seq:
 *   DuckDB::Value.create_null -> DuckDB::Value
 *
 * Creates a new DuckDB::Value representing SQL NULL.
 *
 *   require 'duckdb'
 *   value = DuckDB::Value.create_null
 */
static VALUE value_s_create_null(VALUE klass) {
    duckdb_value value = duckdb_create_null_value();
    return rbduckdb_value_new(value);
}

/*
 * Fills *out with the unwrapped duckdb_values of a Ruby Array of
 * DuckDB::Value objects and returns the array length. The buffer is
 * an ALLOCV tmpbuf; the caller must call ALLOCV_END(*guard) after use.
 * The buffer is non-NULL even for an empty array because
 * duckdb_create_*_value functions reject a NULL values pointer.
 *
 * This always allocates via rb_alloc_tmp_buffer_with_count (heap-backed)
 * rather than the ALLOCV_N macro, because that macro's small-size fast
 * path uses alloca() in *this* function's stack frame: the returned
 * pointer would dangle once marshal_values returns, and a second call
 * (e.g. keys then values for MAP) would silently reuse and clobber the
 * same stack slot as the first call's "buffer".
 */
static idx_t marshal_values(VALUE ary, duckdb_value **out, volatile VALUE *guard) {
    idx_t n = (idx_t)RARRAY_LEN(ary);
    idx_t count = n == 0 ? 1 : n;
    duckdb_value *buf = (duckdb_value *)rb_alloc_tmp_buffer_with_count(guard, count * sizeof(duckdb_value), count);
    idx_t i;

    for (i = 0; i < n; i++) {
        buf[i] = rbduckdb_get_struct_value(RARRAY_AREF(ary, i))->value;
    }
    *out = buf;
    return n;
}

/* :nodoc: */
static VALUE value_s__create_list(VALUE klass, VALUE ltype, VALUE values) {
    duckdb_logical_type type = rbduckdb_get_struct_logical_type(ltype)->logical_type;
    duckdb_value *buf;
    volatile VALUE guard;
    idx_t n = marshal_values(values, &buf, &guard);
    duckdb_value value = duckdb_create_list_value(type, buf, n);

    RB_GC_GUARD(values);
    ALLOCV_END(guard);
    if (value == NULL) {
        rb_raise(eDuckDBError, "failed to create LIST value");
    }
    return rbduckdb_value_new(value);
}

/* :nodoc: */
static VALUE value_s__create_array(VALUE klass, VALUE ltype, VALUE values) {
    duckdb_logical_type type = rbduckdb_get_struct_logical_type(ltype)->logical_type;
    duckdb_value *buf;
    volatile VALUE guard;
    idx_t n = marshal_values(values, &buf, &guard);
    duckdb_value value = duckdb_create_array_value(type, buf, n);

    RB_GC_GUARD(values);
    ALLOCV_END(guard);
    if (value == NULL) {
        rb_raise(eDuckDBError, "failed to create ARRAY value");
    }
    return rbduckdb_value_new(value);
}

/* :nodoc: */
static VALUE value_s__create_struct(VALUE klass, VALUE ltype, VALUE values) {
    duckdb_logical_type type = rbduckdb_get_struct_logical_type(ltype)->logical_type;
    duckdb_value *buf;
    volatile VALUE guard;
    idx_t n = marshal_values(values, &buf, &guard);
    duckdb_value value = duckdb_create_struct_value(type, buf);

    (void)n;
    RB_GC_GUARD(values);
    ALLOCV_END(guard);
    if (value == NULL) {
        rb_raise(eDuckDBError, "failed to create STRUCT value");
    }
    return rbduckdb_value_new(value);
}

/* :nodoc: */
static VALUE value_s__create_map(VALUE klass, VALUE ltype, VALUE keys, VALUE values) {
    duckdb_logical_type type = rbduckdb_get_struct_logical_type(ltype)->logical_type;
    duckdb_value *kbuf;
    duckdb_value *vbuf;
    volatile VALUE kguard;
    volatile VALUE vguard;
    idx_t n = marshal_values(keys, &kbuf, &kguard);
    duckdb_value value;

    marshal_values(values, &vbuf, &vguard);   /* same length, validated in Ruby */
    value = duckdb_create_map_value(type, kbuf, vbuf, n);
    RB_GC_GUARD(keys);
    RB_GC_GUARD(values);
    ALLOCV_END(kguard);
    ALLOCV_END(vguard);
    if (value == NULL) {
        rb_raise(eDuckDBError, "failed to create MAP value");
    }
    return rbduckdb_value_new(value);
}

/*
 *  call-seq:
 *    value.list_size -> Integer
 *
 *  Returns the number of elements of a LIST value.
 *
 *    require 'duckdb'
 *    child_type = DuckDB::LogicalType.resolve(:integer)
 *    values = [1, 2, 3].map { |i| DuckDB::Value.create_int32(i) }
 *    list = DuckDB::Value.create_list(child_type, values)
 *    list.list_size # => 3
 */
static VALUE value_list_size(VALUE self) {
    return ULL2NUM(duckdb_get_list_size(rbduckdb_get_struct_value(self)->value));
}

/*
 *  call-seq:
 *    value.list_child(index) -> DuckDB::Value
 *
 *  Returns the element at the specified index (0-based) of a LIST value
 *  as a DuckDB::Value.
 *  Raises IndexError if the index is out of range or the value is not a LIST.
 *
 *    require 'duckdb'
 *    child_type = DuckDB::LogicalType.resolve(:integer)
 *    values = [1, 2, 3].map { |i| DuckDB::Value.create_int32(i) }
 *    list = DuckDB::Value.create_list(child_type, values)
 *    list.list_child(0) # => DuckDB::Value
 */
static VALUE value_list_child(VALUE self, VALUE vidx) {
    duckdb_value child = duckdb_get_list_child(rbduckdb_get_struct_value(self)->value, (idx_t)NUM2ULL(vidx));

    if (child == NULL) {
        rb_raise(rb_eIndexError, "list index out of range (or the value is not a LIST)");
    }
    return rbduckdb_value_new(child);
}

/*
 *  call-seq:
 *    value.struct_child(index) -> DuckDB::Value
 *
 *  Returns the field at the specified index (0-based) of a STRUCT value
 *  as a DuckDB::Value.
 *  Raises IndexError if the index is out of range or the value is not a STRUCT.
 *
 *    require 'duckdb'
 *    struct_type = DuckDB::LogicalType.create_struct(a: :integer, b: :varchar)
 *    values = [DuckDB::Value.create_int32(1), DuckDB::Value.create_varchar('x')]
 *    struct = DuckDB::Value.create_struct(struct_type, values)
 *    struct.struct_child(0) # => DuckDB::Value
 */
static VALUE value_struct_child(VALUE self, VALUE vidx) {
    duckdb_value child = duckdb_get_struct_child(rbduckdb_get_struct_value(self)->value, (idx_t)NUM2ULL(vidx));

    if (child == NULL) {
        rb_raise(rb_eIndexError, "struct index out of range (or the value is not a STRUCT)");
    }
    return rbduckdb_value_new(child);
}

/*
 *  call-seq:
 *    value.map_size -> Integer
 *
 *  Returns the number of entries of a MAP value.
 *
 *    require 'duckdb'
 *    map_type = DuckDB::LogicalType.create_map(:varchar, :integer)
 *    keys = [DuckDB::Value.create_varchar('a')]
 *    values = [DuckDB::Value.create_int32(1)]
 *    map = DuckDB::Value.create_map(map_type, keys, values)
 *    map.map_size # => 1
 */
static VALUE value_map_size(VALUE self) {
    return ULL2NUM(duckdb_get_map_size(rbduckdb_get_struct_value(self)->value));
}

/*
 *  call-seq:
 *    value.map_key(index) -> DuckDB::Value
 *
 *  Returns the key at the specified index (0-based) of a MAP value
 *  as a DuckDB::Value.
 *  Raises IndexError if the index is out of range or the value is not a MAP.
 *
 *    require 'duckdb'
 *    map_type = DuckDB::LogicalType.create_map(:varchar, :integer)
 *    keys = [DuckDB::Value.create_varchar('a')]
 *    values = [DuckDB::Value.create_int32(1)]
 *    map = DuckDB::Value.create_map(map_type, keys, values)
 *    map.map_key(0) # => DuckDB::Value
 */
static VALUE value_map_key(VALUE self, VALUE vidx) {
    duckdb_value child = duckdb_get_map_key(rbduckdb_get_struct_value(self)->value, (idx_t)NUM2ULL(vidx));

    if (child == NULL) {
        rb_raise(rb_eIndexError, "map index out of range (or the value is not a MAP)");
    }
    return rbduckdb_value_new(child);
}

/*
 *  call-seq:
 *    value.map_value(index) -> DuckDB::Value
 *
 *  Returns the value at the specified index (0-based) of a MAP value
 *  as a DuckDB::Value.
 *  Raises IndexError if the index is out of range or the value is not a MAP.
 *
 *    require 'duckdb'
 *    map_type = DuckDB::LogicalType.create_map(:varchar, :integer)
 *    keys = [DuckDB::Value.create_varchar('a')]
 *    values = [DuckDB::Value.create_int32(1)]
 *    map = DuckDB::Value.create_map(map_type, keys, values)
 *    map.map_value(0) # => DuckDB::Value
 */
static VALUE value_map_value(VALUE self, VALUE vidx) {
    duckdb_value child = duckdb_get_map_value(rbduckdb_get_struct_value(self)->value, (idx_t)NUM2ULL(vidx));

    if (child == NULL) {
        rb_raise(rb_eIndexError, "map index out of range (or the value is not a MAP)");
    }
    return rbduckdb_value_new(child);
}

VALUE rbduckdb_value_new(duckdb_value value) {
    rubyDuckDBValue *ctx;
    VALUE obj = allocate(cDuckDBValue);
    TypedData_Get_Struct(obj, rubyDuckDBValue, &value_data_type, ctx);
    ctx->value = value;
    return obj;
}

rubyDuckDBValue *rbduckdb_get_struct_value(VALUE obj) {
    rubyDuckDBValue *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBValue, &value_data_type, ctx);
    return ctx;
}

VALUE rbduckdb_duckdb_value_to_ruby(duckdb_value val) {
    duckdb_logical_type logical_type;
    duckdb_type type_id;
    VALUE result;
    char *str;
    idx_t i;
    idx_t size;

    if (duckdb_is_null_value(val)) {
        return Qnil;
    }

    logical_type = duckdb_get_value_type(val);
    type_id = duckdb_get_type_id(logical_type);

    switch (type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            result = duckdb_get_bool(val) ? Qtrue : Qfalse;
            break;
        case DUCKDB_TYPE_TINYINT:
            result = INT2FIX(duckdb_get_int8(val));
            break;
        case DUCKDB_TYPE_SMALLINT:
            result = INT2FIX(duckdb_get_int16(val));
            break;
        case DUCKDB_TYPE_INTEGER:
            result = INT2NUM(duckdb_get_int32(val));
            break;
        case DUCKDB_TYPE_BIGINT:
            result = LL2NUM(duckdb_get_int64(val));
            break;
        case DUCKDB_TYPE_HUGEINT:
            result = rbduckdb_hugeint_to_ruby(duckdb_get_hugeint(val));
            break;
        case DUCKDB_TYPE_UHUGEINT:
            result = rbduckdb_uhugeint_to_ruby(duckdb_get_uhugeint(val));
            break;
        case DUCKDB_TYPE_UTINYINT:
            result = INT2FIX(duckdb_get_uint8(val));
            break;
        case DUCKDB_TYPE_USMALLINT:
            result = INT2FIX(duckdb_get_uint16(val));
            break;
        case DUCKDB_TYPE_UINTEGER:
            result = UINT2NUM(duckdb_get_uint32(val));
            break;
        case DUCKDB_TYPE_UBIGINT:
            result = ULL2NUM(duckdb_get_uint64(val));
            break;
        case DUCKDB_TYPE_FLOAT:
            result = DBL2NUM(duckdb_get_float(val));
            break;
        case DUCKDB_TYPE_DOUBLE:
            result = DBL2NUM(duckdb_get_double(val));
            break;
        case DUCKDB_TYPE_TIMESTAMP:
            result = rbduckdb_timestamp_to_ruby(duckdb_get_timestamp(val));
            break;
        case DUCKDB_TYPE_DATE:
            result = rbduckdb_date_to_ruby(duckdb_get_date(val));
            break;
        case DUCKDB_TYPE_TIME:
            result = rbduckdb_time_to_ruby(duckdb_get_time(val));
            break;
        case DUCKDB_TYPE_INTERVAL:
            result = rbduckdb_interval_to_ruby(duckdb_get_interval(val));
            break;
        case DUCKDB_TYPE_TIMESTAMP_S:
            result = rbduckdb_timestamp_s_to_ruby(duckdb_get_timestamp_s(val));
            break;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            result = rbduckdb_timestamp_ms_to_ruby(duckdb_get_timestamp_ms(val));
            break;
        case DUCKDB_TYPE_TIMESTAMP_NS:
            result = rbduckdb_timestamp_ns_to_ruby(duckdb_get_timestamp_ns(val));
            break;
        case DUCKDB_TYPE_TIME_NS:
            result = rbduckdb_time_ns_to_ruby(duckdb_get_time_ns(val));
            break;
        case DUCKDB_TYPE_TIME_TZ:
            result = rbduckdb_time_tz_to_ruby(duckdb_get_time_tz(val));
            break;
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            result = rbduckdb_timestamp_tz_to_ruby(duckdb_get_timestamp_tz(val));
            break;
        case DUCKDB_TYPE_VARCHAR:
            str = duckdb_get_varchar(val);
            result = rb_str_new_cstr(str);
            duckdb_free(str);
            break;
        case DUCKDB_TYPE_UUID:
            result = rbduckdb_uuid_uhugeint_to_ruby(duckdb_get_uuid(val));
            break;
        case DUCKDB_TYPE_LIST:
            size = duckdb_get_list_size(val);
            result = rb_ary_new_capa(size);
            for (i = 0; i < size; i++) {
                duckdb_value child = duckdb_get_list_child(val, i);
                VALUE elem = rbduckdb_duckdb_value_to_ruby(child);
                duckdb_destroy_value(&child);
                rb_ary_push(result, elem);
            }
            break;
        case DUCKDB_TYPE_STRUCT:
            size = duckdb_struct_type_child_count(logical_type);
            result = rb_hash_new();
            for (i = 0; i < size; i++) {
                char *name = duckdb_struct_type_child_name(logical_type, i);
                duckdb_value child = duckdb_get_struct_child(val, i);
                VALUE elem = rbduckdb_duckdb_value_to_ruby(child);
                duckdb_destroy_value(&child);
                rb_hash_aset(result, ID2SYM(rb_intern(name)), elem);
                duckdb_free(name);
            }
            break;
        case DUCKDB_TYPE_MAP:
            size = duckdb_get_map_size(val);
            result = rb_hash_new();
            for (i = 0; i < size; i++) {
                duckdb_value k = duckdb_get_map_key(val, i);
                duckdb_value v = duckdb_get_map_value(val, i);
                VALUE rk = rbduckdb_duckdb_value_to_ruby(k);
                VALUE rv = rbduckdb_duckdb_value_to_ruby(v);
                duckdb_destroy_value(&k);
                duckdb_destroy_value(&v);
                rb_hash_aset(result, rk, rv);
            }
            break;
        case DUCKDB_TYPE_ARRAY: {
            /* the C API has no duckdb_get_array_size/child; read via a vector */
            duckdb_logical_type child_type = duckdb_array_type_child_type(logical_type);
            duckdb_vector vec = duckdb_create_vector(logical_type, 1);
            duckdb_vector child;

            size = duckdb_array_type_array_size(logical_type);
            duckdb_vector_reference_value(vec, val);
            child = duckdb_array_vector_get_child(vec);
            result = rb_ary_new_capa(size);
            for (i = 0; i < size; i++) {
                rb_ary_push(result, rbduckdb_vector_value_at(child, child_type, i));
            }
            duckdb_destroy_logical_type(&child_type);
            duckdb_destroy_vector(&vec);
            break;
        }
        default:
            result = Qnil;
            break;
    }

    /* logical_type from duckdb_get_value_type is borrowed and must not be destroyed */
    return result;
}

/*
 *  call-seq:
 *    value.to_ruby -> Object
 *
 *  Converts the DuckDB::Value to a Ruby object. Scalar types are converted
 *  to their natural Ruby classes. LIST values are converted to Array
 *  recursively. NULL is converted to nil. Returns nil for unsupported types.
 *
 *    require 'duckdb'
 *    child_type = DuckDB::LogicalType.resolve(:integer)
 *    values = [1, 2, 3].map { |i| DuckDB::Value.create_int32(i) }
 *    list = DuckDB::Value.create_list(child_type, values)
 *    list.to_ruby # => [1, 2, 3]
 */
static VALUE value_to_ruby(VALUE self) {
    return rbduckdb_duckdb_value_to_ruby(rbduckdb_get_struct_value(self)->value);
}

void rbduckdb_init_value(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBValue = rb_define_class_under(mDuckDB, "Value", rb_cObject);
    rb_define_alloc_func(cDuckDBValue, allocate);

    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_bool", value_s__create_bool, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_int8", value_s__create_int8, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_int16", value_s__create_int16, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_int32", value_s__create_int32, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_int64", value_s__create_int64, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_uint8", value_s__create_uint8, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_uint16", value_s__create_uint16, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_uint32", value_s__create_uint32, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_uint64", value_s__create_uint64, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_float", value_s__create_float, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_double", value_s__create_double, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_varchar", value_s__create_varchar, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_blob", value_s__create_blob, 1);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_hugeint", value_s__create_hugeint, 2);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_uhugeint", value_s__create_uhugeint, 2);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_decimal", value_s__create_decimal, 4);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_list", value_s__create_list, 2);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_array", value_s__create_array, 2);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_struct", value_s__create_struct, 2);
    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_map", value_s__create_map, 3);
    rb_define_singleton_method(cDuckDBValue, "create_null", value_s_create_null, 0);

    rb_define_method(cDuckDBValue, "list_size", value_list_size, 0);
    rb_define_method(cDuckDBValue, "list_child", value_list_child, 1);
    rb_define_method(cDuckDBValue, "struct_child", value_struct_child, 1);
    rb_define_method(cDuckDBValue, "map_size", value_map_size, 0);
    rb_define_method(cDuckDBValue, "map_key", value_map_key, 1);
    rb_define_method(cDuckDBValue, "map_value", value_map_value, 1);
    rb_define_method(cDuckDBValue, "to_ruby", value_to_ruby, 0);
}
