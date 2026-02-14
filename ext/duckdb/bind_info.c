#include "ruby-duckdb.h"

VALUE cDuckDBBindInfo;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE convert_duckdb_value_to_ruby(duckdb_value param_value);
static VALUE rbduckdb_bind_info_parameter_count(VALUE self);
static VALUE rbduckdb_bind_info_get_parameter(VALUE self, VALUE index);
static VALUE rbduckdb_bind_info_get_named_parameter(VALUE self, VALUE name);
static VALUE rbduckdb_bind_info_add_result_column(VALUE self, VALUE column_name, VALUE logical_type);
static VALUE rbduckdb_bind_info_set_cardinality(VALUE self, VALUE cardinality, VALUE is_exact);
static VALUE rbduckdb_bind_info_set_error(VALUE self, VALUE error);

static const rb_data_type_t bind_info_data_type = {
    "DuckDB/BindInfo",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBBindInfo *p = (rubyDuckDBBindInfo *)ctx;
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBBindInfo *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBBindInfo));
    return TypedData_Wrap_Struct(klass, &bind_info_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBBindInfo);
}

rubyDuckDBBindInfo *get_struct_bind_info(VALUE obj) {
    rubyDuckDBBindInfo *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBBindInfo, &bind_info_data_type, ctx);
    return ctx;
}

/*
 * Converts a duckdb_value to a Ruby VALUE.
 * Handles basic types: BIGINT, INTEGER, VARCHAR, DOUBLE, BOOLEAN.
 * Returns Qnil for unsupported types.
 * Note: Caller must destroy duckdb_value and duckdb_logical_type.
 */
static VALUE convert_duckdb_value_to_ruby(duckdb_value param_value) {
    duckdb_logical_type logical_type;
    duckdb_type type_id;
    VALUE result;

    logical_type = duckdb_get_value_type(param_value);
    type_id = duckdb_get_type_id(logical_type);

    switch (type_id) {
        case DUCKDB_TYPE_BIGINT:
            result = LL2NUM(duckdb_get_int64(param_value));
            break;
        case DUCKDB_TYPE_INTEGER:
            result = INT2NUM(duckdb_get_int32(param_value));
            break;
        case DUCKDB_TYPE_VARCHAR: {
            char *str = duckdb_get_varchar(param_value);
            result = rb_str_new_cstr(str);
            duckdb_free(str);
            break;
        }
        case DUCKDB_TYPE_DOUBLE:
            result = DBL2NUM(duckdb_get_double(param_value));
            break;
        case DUCKDB_TYPE_BOOLEAN:
            result = duckdb_get_bool(param_value) ? Qtrue : Qfalse;
            break;
        default:
            // For unsupported types, return nil
            result = Qnil;
            break;
    }

    duckdb_destroy_logical_type(&logical_type);

    return result;
}

/*
 * call-seq:
 *   bind_info.parameter_count -> Integer
 *
 * Returns the number of parameters passed to the table function.
 *
 *   bind_info.parameter_count  # => 2
 */
static VALUE rbduckdb_bind_info_parameter_count(VALUE self) {
    rubyDuckDBBindInfo *ctx;
    idx_t count;

    TypedData_Get_Struct(self, rubyDuckDBBindInfo, &bind_info_data_type, ctx);

    count = duckdb_bind_get_parameter_count(ctx->bind_info);

    return ULL2NUM(count);
}

/*
 * call-seq:
 *   bind_info.get_parameter(index) -> value
 *
 * Gets the parameter value at the given index.
 *
 *   param = bind_info.get_parameter(0)
 */
static VALUE rbduckdb_bind_info_get_parameter(VALUE self, VALUE index) {
    rubyDuckDBBindInfo *ctx;
    idx_t idx;
    duckdb_value param_value;
    VALUE result;

    TypedData_Get_Struct(self, rubyDuckDBBindInfo, &bind_info_data_type, ctx);

    idx = NUM2ULL(index);
    param_value = duckdb_bind_get_parameter(ctx->bind_info, idx);

    result = convert_duckdb_value_to_ruby(param_value);

    duckdb_destroy_value(&param_value);

    return result;
}

/*
 * call-seq:
 *   bind_info.get_named_parameter(name) -> value or nil
 *
 * Gets the named parameter value, or nil if not provided.
 *
 *   param = bind_info.get_named_parameter('limit')
 */
static VALUE rbduckdb_bind_info_get_named_parameter(VALUE self, VALUE name) {
    rubyDuckDBBindInfo *ctx;
    const char *param_name;
    duckdb_value param_value;
    VALUE result;

    TypedData_Get_Struct(self, rubyDuckDBBindInfo, &bind_info_data_type, ctx);

    param_name = StringValueCStr(name);
    param_value = duckdb_bind_get_named_parameter(ctx->bind_info, param_name);

    // If parameter not found, return nil
    if (!param_value) {
        return Qnil;
    }

    result = convert_duckdb_value_to_ruby(param_value);

    duckdb_destroy_value(&param_value);

    return result;
}

/*
 * call-seq:
 *   bind_info.add_result_column(name, logical_type) -> self
 *
 * Adds a column to the output schema.
 *
 *   bind_info.add_result_column('id', DuckDB::LogicalType::BIGINT)
 *   bind_info.add_result_column('name', DuckDB::LogicalType::VARCHAR)
 */
static VALUE rbduckdb_bind_info_add_result_column(VALUE self, VALUE column_name, VALUE logical_type) {
    rubyDuckDBBindInfo *ctx;
    rubyDuckDBLogicalType *ctx_logical_type;
    const char *col_name;

    TypedData_Get_Struct(self, rubyDuckDBBindInfo, &bind_info_data_type, ctx);
    ctx_logical_type = get_struct_logical_type(logical_type);

    col_name = StringValueCStr(column_name);
    duckdb_bind_add_result_column(ctx->bind_info, col_name, ctx_logical_type->logical_type);

    return self;
}

/*
 * call-seq:
 *   bind_info.set_cardinality(cardinality, is_exact) -> self
 *
 * Sets the estimated number of rows this function will return.
 *
 *   bind_info.set_cardinality(100, true)  # Exactly 100 rows
 *   bind_info.set_cardinality(1000, false)  # Approximately 1000 rows
 */
static VALUE rbduckdb_bind_info_set_cardinality(VALUE self, VALUE cardinality, VALUE is_exact) {
    rubyDuckDBBindInfo *ctx;
    idx_t card;
    bool exact;

    TypedData_Get_Struct(self, rubyDuckDBBindInfo, &bind_info_data_type, ctx);

    card = NUM2ULL(cardinality);
    exact = RTEST(is_exact);

    duckdb_bind_set_cardinality(ctx->bind_info, card, exact);

    return self;
}

/*
 * call-seq:
 *   bind_info.set_error(error_message) -> self
 *
 * Reports an error during bind phase.
 *
 *   bind_info.set_error('Invalid parameter value')
 */
static VALUE rbduckdb_bind_info_set_error(VALUE self, VALUE error) {
    rubyDuckDBBindInfo *ctx;
    const char *error_msg;

    TypedData_Get_Struct(self, rubyDuckDBBindInfo, &bind_info_data_type, ctx);

    error_msg = StringValueCStr(error);
    duckdb_bind_set_error(ctx->bind_info, error_msg);

    return self;
}

void rbduckdb_init_duckdb_bind_info(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBBindInfo = rb_define_class_under(mDuckDB, "BindInfo", rb_cObject);
    rb_define_alloc_func(cDuckDBBindInfo, allocate);

    rb_define_method(cDuckDBBindInfo, "parameter_count", rbduckdb_bind_info_parameter_count, 0);
    rb_define_method(cDuckDBBindInfo, "get_parameter", rbduckdb_bind_info_get_parameter, 1);
    rb_define_method(cDuckDBBindInfo, "get_named_parameter", rbduckdb_bind_info_get_named_parameter, 1);
    rb_define_method(cDuckDBBindInfo, "add_result_column", rbduckdb_bind_info_add_result_column, 2);
    rb_define_method(cDuckDBBindInfo, "set_cardinality", rbduckdb_bind_info_set_cardinality, 2);
    rb_define_method(cDuckDBBindInfo, "set_error", rbduckdb_bind_info_set_error, 1);
}
