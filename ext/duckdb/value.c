#include "ruby-duckdb.h"

VALUE cDuckDBValue;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_value_s__create_bool(VALUE klass, VALUE flag);

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

static VALUE duckdb_value_s__create_bool(VALUE klass, VALUE flag) {
    duckdb_value value = duckdb_create_bool(RTEST(flag) ? true : false);
    return rbduckdb_value_new(value);
}

VALUE rbduckdb_value_new(duckdb_value value) {
    rubyDuckDBValue *ctx;
    VALUE obj = allocate(cDuckDBValue);
    TypedData_Get_Struct(obj, rubyDuckDBValue, &value_data_type, ctx);
    ctx->value = value;
    return obj;
}

rubyDuckDBValue *get_struct_value(VALUE obj) {
    rubyDuckDBValue *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBValue, &value_data_type, ctx);
    return ctx;
}

VALUE rbduckdb_duckdb_value_to_ruby(duckdb_value val) {
    duckdb_logical_type logical_type;
    duckdb_type type_id;
    VALUE result;
    char *str;

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
        default:
            result = Qnil;
            break;
    }

    return result;
}

void rbduckdb_init_duckdb_value(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBValue = rb_define_class_under(mDuckDB, "Value", rb_cObject);
    rb_define_alloc_func(cDuckDBValue, allocate);

    rb_define_private_method(rb_singleton_class(cDuckDBValue), "_create_bool", duckdb_value_s__create_bool, 1);
}

