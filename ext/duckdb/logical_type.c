#include "ruby-duckdb.h"

static VALUE cDuckDBLogicalType;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_logical_type__type(VALUE self);
static VALUE duckdb_logical_type_width(VALUE self);
static VALUE duckdb_logical_type_scale(VALUE self);
static VALUE duckdb_logical_type_child_count(VALUE self);
static VALUE duckdb_logical_type_child_name_at(VALUE self, VALUE cidx);
static VALUE duckdb_logical_type_child_type(VALUE self);
static VALUE duckdb_logical_type_child_type_at(VALUE self, VALUE cidx);
static VALUE duckdb_logical_type_size(VALUE self);
static VALUE duckdb_logical_type_key_type(VALUE self);
static VALUE duckdb_logical_type_value_type(VALUE self);
static VALUE duckdb_logical_type_member_count(VALUE self);
static VALUE duckdb_logical_type_member_name_at(VALUE self, VALUE midx);
static VALUE duckdb_logical_type_member_type_at(VALUE self, VALUE midx);
static VALUE duckdb_logical_type__internal_type(VALUE self);
static VALUE duckdb_logical_type_dictionary_size(VALUE self);
static VALUE duckdb_logical_type_dictionary_value_at(VALUE self, VALUE didx);
static VALUE duckdb_logical_type__get_alias(VALUE self);
static VALUE duckdb_logical_type__set_alias(VALUE self, VALUE aname);
static VALUE initialize(VALUE self, VALUE type_id_arg);

static const rb_data_type_t logical_type_data_type = {
    "DuckDB/LogicalType",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBLogicalType *p = (rubyDuckDBLogicalType *)ctx;

    if (p->logical_type) {
        duckdb_destroy_logical_type(&(p->logical_type));
    }

    xfree(p);
}

rubyDuckDBLogicalType *get_struct_logical_type(VALUE obj) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return ctx;
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBLogicalType *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBLogicalType));
    return TypedData_Wrap_Struct(klass, &logical_type_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBLogicalType);
}

static VALUE initialize(VALUE self, VALUE type_id_arg) {
    rubyDuckDBLogicalType *ctx;
    duckdb_type type = (duckdb_type)NUM2INT(type_id_arg);
    duckdb_logical_type new_logical_type;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    if (ctx->logical_type) {
        duckdb_destroy_logical_type(&(ctx->logical_type));
    }

    new_logical_type = duckdb_create_logical_type(type);

    if (!new_logical_type || duckdb_get_type_id(new_logical_type) == DUCKDB_TYPE_INVALID) {
        if (new_logical_type) {
            duckdb_destroy_logical_type(&new_logical_type);
        }
        rb_raise(rb_eArgError, "Invalid or unsupported logical type ID: %d", type);
    }

    ctx->logical_type = new_logical_type;

    return self;
}

/*
 *  call-seq:
 *    decimal_col.logical_type.type -> Symbol
 *
 *  Returns the logical type's type symbol.
 *
 */
static VALUE duckdb_logical_type__type(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_get_type_id(ctx->logical_type));
}

/*
 *  call-seq:
 *    decimal_col.logical_type.width -> Integer
 *
 *  Returns the width of the decimal column.
 *
 */
static VALUE duckdb_logical_type_width(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_decimal_width(ctx->logical_type));
}

/*
 *  call-seq:
 *    decimal_col.logical_type.scale -> Integer
 *
 *  Returns the scale of the decimal column.
 *
 */
static VALUE duckdb_logical_type_scale(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_decimal_scale(ctx->logical_type));
}

/*
 *  call-seq:
 *    struct_col.logical_type.child_count -> Integer
 *
 *  Returns the number of children of a struct type, otherwise 0.
 *
 */
static VALUE duckdb_logical_type_child_count(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_struct_type_child_count(ctx->logical_type));
}

/*
 *  call-seq:
 *    struct_col.logical_type.child_name(index) -> String
 *
 *  Returns the name of the struct child at the specified index.
 *
 */
static VALUE duckdb_logical_type_child_name_at(VALUE self, VALUE cidx) {
    rubyDuckDBLogicalType *ctx;
    VALUE cname;
    const char *child_name;
    idx_t idx = NUM2ULL(cidx);

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    child_name = duckdb_struct_type_child_name(ctx->logical_type, idx);
    if (child_name == NULL) {
        rb_raise(eDuckDBError, "fail to get name of %llu child", (unsigned long long)idx);
    }
    cname = rb_str_new_cstr(child_name);
    duckdb_free((void *)child_name);
    return cname;
}

/*
 *  call-seq:
 *    list_col.logical_type.child_type -> DuckDB::LogicalType
 *
 *  Returns the child logical type for list and map types, otherwise nil.
 *
 */
static VALUE duckdb_logical_type_child_type(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    duckdb_type type_id;
    duckdb_logical_type child_logical_type;
    VALUE logical_type = Qnil;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    type_id = duckdb_get_type_id(ctx->logical_type);

    switch(type_id) {
        case DUCKDB_TYPE_LIST:
        case DUCKDB_TYPE_MAP:
            child_logical_type = duckdb_list_type_child_type(ctx->logical_type);
            logical_type = rbduckdb_create_logical_type(child_logical_type);
            break;
        case DUCKDB_TYPE_ARRAY:
            child_logical_type = duckdb_array_type_child_type(ctx->logical_type);
            logical_type = rbduckdb_create_logical_type(child_logical_type);
            break;
        default:
            logical_type = Qnil;
    }
    return logical_type;
}

/*
 *  call-seq:
 *    struct_col.logical_type.child_type_at(index) -> DuckDB::LogicalType
 *
 *  Returns the child logical type for struct types at the specified index as a
 *  DuckDB::LogicalType object.
 *
 */
static VALUE duckdb_logical_type_child_type_at(VALUE self, VALUE cidx) {
    rubyDuckDBLogicalType *ctx;
    duckdb_logical_type struct_child_type;
    idx_t idx = NUM2ULL(cidx);

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    struct_child_type = duckdb_struct_type_child_type(ctx->logical_type, idx);
    if (struct_child_type == NULL) {
        rb_raise(eDuckDBError,
                 "Failed to get the struct child type at index %llu",
                 (unsigned long long)idx);
    }

    return rbduckdb_create_logical_type(struct_child_type);
}

/*
 *  call-seq:
 *    list_col.logical_type.size -> Integer
 *
 *  Returns the size of the array column, otherwise 0.
 *
 */
static VALUE duckdb_logical_type_size(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_array_type_array_size(ctx->logical_type));
}

/*
 *  call-seq:
 *    map_col.logical_type.key_type -> DuckDB::LogicalType
 *
 *  Returns the key logical type for map type, otherwise nil.
 *
 */
static VALUE duckdb_logical_type_key_type(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    duckdb_logical_type key_logical_type;
    VALUE logical_type = Qnil;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    key_logical_type = duckdb_map_type_key_type(ctx->logical_type);
    logical_type = rbduckdb_create_logical_type(key_logical_type);
    return logical_type;
}

/*
 *  call-seq:
 *    map_col.logical_type.value_type -> DuckDB::LogicalType
 *
 *  Returns the value logical type for map type, otherwise nil.
 *
 */
static VALUE duckdb_logical_type_value_type(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    duckdb_logical_type value_logical_type;
    VALUE logical_type = Qnil;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    value_logical_type = duckdb_map_type_value_type(ctx->logical_type);
    logical_type = rbduckdb_create_logical_type(value_logical_type);
    return logical_type;
}

/*
 *  call-seq:
 *    member_col.logical_type.member_count -> Integer
 *
 *  Returns the member count of union type, otherwise 0.
 *
 */
static VALUE duckdb_logical_type_member_count(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_union_type_member_count(ctx->logical_type));
}

/*
 *  call-seq:
 *    union_col.logical_type.member_name_at(index) -> String
 *
 *  Returns the name of the union member at the specified index.
 *
 */
static VALUE duckdb_logical_type_member_name_at(VALUE self, VALUE midx) {
    rubyDuckDBLogicalType *ctx;
    VALUE mname;
    const char *member_name;
    idx_t idx = NUM2ULL(midx);

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    member_name = duckdb_union_type_member_name(ctx->logical_type, idx);
    if (member_name == NULL) {
        rb_raise(eDuckDBError, "fail to get name of %llu member", (unsigned long long)idx);
    }
    mname = rb_str_new_cstr(member_name);
    duckdb_free((void *)member_name);
    return mname;
}

/*
 *  call-seq:
 *    union_col.logical_type.member_type_at(index) -> DuckDB::LogicalType
 *
 *  Returns the logical type of the union member at the specified index as a
 *  DuckDB::LogicalType object.
 *
 */
static VALUE duckdb_logical_type_member_type_at(VALUE self, VALUE midx) {
    rubyDuckDBLogicalType *ctx;
    duckdb_logical_type union_member_type;
    idx_t idx = NUM2ULL(midx);

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    union_member_type = duckdb_union_type_member_type(ctx->logical_type, idx);
    if (union_member_type == NULL) {
        rb_raise(eDuckDBError,
                 "Failed to get the union member type at index %llu",
                 (unsigned long long)idx);
    }

    return rbduckdb_create_logical_type(union_member_type);
}

/*
 *  call-seq:
 *    enum_col.logical_type.internal_type -> Symbol
 *
 *  Returns the logical type's internal type.
 *
 */
static VALUE duckdb_logical_type__internal_type(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    duckdb_type type_id;
    duckdb_type internal_type_id;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    type_id = duckdb_get_type_id(ctx->logical_type);
    switch (type_id) {
        case DUCKDB_TYPE_DECIMAL:
            internal_type_id = duckdb_decimal_internal_type(ctx->logical_type);
            break;
        case DUCKDB_TYPE_ENUM:
            internal_type_id = duckdb_enum_internal_type(ctx->logical_type);
            break;
        default:
            internal_type_id = DUCKDB_TYPE_INVALID;
    }

    return INT2FIX(internal_type_id);
}

/*
 *  call-seq:
 *    enum_col.logical_type.dictionary_size -> Integer
 *
 *  Returns the dictionary size of the enum type.
 *
 */
static VALUE duckdb_logical_type_dictionary_size(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    return INT2FIX(duckdb_enum_dictionary_size(ctx->logical_type));
}

/*
 *  call-seq:
 *    enum_col.logical_type.dictionary_value_at(index) -> String
 *
 *  Returns the dictionary value at the specified index.
 *
 */
static VALUE duckdb_logical_type_dictionary_value_at(VALUE self, VALUE didx) {
    rubyDuckDBLogicalType *ctx;
    VALUE dvalue;
    const char *dict_value;
    idx_t idx = NUM2ULL(didx);

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    dict_value = duckdb_enum_dictionary_value(ctx->logical_type, idx);
    if (dict_value == NULL) {
        rb_raise(eDuckDBError, "fail to get dictionary value of %llu", (unsigned long long)idx);
    }
    dvalue = rb_utf8_str_new_cstr(dict_value);
    duckdb_free((void *)dict_value);
    return dvalue;
}

/*
 *  call-seq:
 *    col.logical_type.alias -> String
 *
 *  Returns the alias of the logical type.
 *
 */
static VALUE duckdb_logical_type__get_alias(VALUE self) {
    rubyDuckDBLogicalType *ctx;
    VALUE alias = Qnil;
    const char *_alias;

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    _alias = duckdb_logical_type_get_alias(ctx->logical_type);
    if (_alias != NULL) {
        alias = rb_utf8_str_new_cstr(_alias);
    }
    duckdb_free((void *)_alias);
    return alias;
}

/*
 *  call-seq:
 *    col.logical_type.alias(alias) -> String
 *
 *  Return the set alias of the logical type.
 *
 */
static VALUE duckdb_logical_type__set_alias(VALUE self, VALUE aname) {
    rubyDuckDBLogicalType *ctx;
    VALUE alias = Qnil;
    const char *_alias = StringValuePtr(aname);

    TypedData_Get_Struct(self, rubyDuckDBLogicalType, &logical_type_data_type, ctx);
    duckdb_logical_type_set_alias(ctx->logical_type, _alias);
    if (_alias != NULL) {
        alias = rb_utf8_str_new_cstr(_alias);
    }

    return alias;
}

VALUE rbduckdb_create_logical_type(duckdb_logical_type logical_type) {
    VALUE obj;
    rubyDuckDBLogicalType *ctx;

    obj = allocate(cDuckDBLogicalType);
    TypedData_Get_Struct(obj, rubyDuckDBLogicalType, &logical_type_data_type, ctx);

    ctx->logical_type = logical_type;

    return obj;
}

void rbduckdb_init_duckdb_logical_type(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBLogicalType = rb_define_class_under(mDuckDB, "LogicalType", rb_cObject);
    rb_define_alloc_func(cDuckDBLogicalType, allocate);

    rb_define_private_method(cDuckDBLogicalType, "_type", duckdb_logical_type__type, 0);
    rb_define_method(cDuckDBLogicalType, "width", duckdb_logical_type_width, 0);
    rb_define_method(cDuckDBLogicalType, "scale", duckdb_logical_type_scale, 0);
    rb_define_method(cDuckDBLogicalType, "child_count", duckdb_logical_type_child_count, 0);
    rb_define_method(cDuckDBLogicalType, "child_name_at", duckdb_logical_type_child_name_at, 1);
    rb_define_method(cDuckDBLogicalType, "child_type", duckdb_logical_type_child_type, 0);
    rb_define_method(cDuckDBLogicalType, "child_type_at", duckdb_logical_type_child_type_at, 1);
    rb_define_method(cDuckDBLogicalType, "size", duckdb_logical_type_size, 0);
    rb_define_method(cDuckDBLogicalType, "key_type", duckdb_logical_type_key_type, 0);
    rb_define_method(cDuckDBLogicalType, "value_type", duckdb_logical_type_value_type, 0);
    rb_define_method(cDuckDBLogicalType, "member_count", duckdb_logical_type_member_count, 0);
    rb_define_method(cDuckDBLogicalType, "member_name_at", duckdb_logical_type_member_name_at, 1);
    rb_define_method(cDuckDBLogicalType, "member_type_at", duckdb_logical_type_member_type_at, 1);
    rb_define_method(cDuckDBLogicalType, "_internal_type", duckdb_logical_type__internal_type, 0);
    rb_define_method(cDuckDBLogicalType, "dictionary_size", duckdb_logical_type_dictionary_size, 0);
    rb_define_method(cDuckDBLogicalType, "dictionary_value_at", duckdb_logical_type_dictionary_value_at, 1);
    rb_define_method(cDuckDBLogicalType, "get_alias", duckdb_logical_type__get_alias, 0);
    rb_define_method(cDuckDBLogicalType, "set_alias", duckdb_logical_type__set_alias, 1);

    rb_define_method(cDuckDBLogicalType, "initialize", initialize, 1);
}
