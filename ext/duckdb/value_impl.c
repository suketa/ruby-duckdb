#include "ruby-duckdb.h"

VALUE cDuckDBValueImpl;

static void deallocate(void *);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static const rb_data_type_t value_impl_data_type = {
    "DuckDB/ValueImpl",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void * ctx) {
    rubyDuckDBValueImpl *p = (rubyDuckDBValueImpl *)ctx;

    duckdb_destroy_value(&(p->value));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBValueImpl *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBValueImpl));
    return TypedData_Wrap_Struct(klass, &value_impl_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBValueImpl);
}

VALUE rbduckdb_value_impl_new(duckdb_value value) {
    rubyDuckDBValueImpl *ctx;
    VALUE obj = allocate(cDuckDBValueImpl);
    TypedData_Get_Struct(obj, rubyDuckDBValueImpl, &value_impl_data_type, ctx);
    ctx->value = value;
    return obj;
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
        case DUCKDB_TYPE_TIMESTAMP_S:
            result = rbduckdb_timestamp_s_to_ruby(duckdb_get_timestamp_s(val));
            break;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            result = rbduckdb_timestamp_ms_to_ruby(duckdb_get_timestamp_ms(val));
            break;
        case DUCKDB_TYPE_VARCHAR:
            str = duckdb_get_varchar(val);
            result = rb_str_new_cstr(str);
            duckdb_free(str);
            break;
        default:
            result = Qnil;
            break;
    }

    return result;
}

void rbduckdb_init_duckdb_value_impl(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBValueImpl = rb_define_class_under(mDuckDB, "ValueImpl", rb_cObject);
    rb_define_alloc_func(cDuckDBValueImpl, allocate);
}

