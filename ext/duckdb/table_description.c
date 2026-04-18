#include "ruby-duckdb.h"

#ifdef HAVE_DUCKDB_H_GE_V1_5_0

VALUE cDuckDBTableDescription;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static rubyDuckDBTableDescription *get_struct_table_description(VALUE obj);
static VALUE rbduckdb_table_description_error_message(VALUE self);
static VALUE duckdb_table_description__initialize(VALUE self, VALUE conn, VALUE catalog, VALUE schema, VALUE table);

static VALUE duckdb_table_description__column_count(VALUE self);
static VALUE duckdb_table_description__column_name(VALUE self, VALUE idx);
static VALUE duckdb_table_description__column_logical_type(VALUE self, VALUE idx);
static VALUE duckdb_table_description__column_has_default(VALUE self, VALUE idx);

static const rb_data_type_t table_description_data_type = {
    "DuckDB/TableDescription",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBTableDescription *p = (rubyDuckDBTableDescription *)ctx;

    if (p->table_description) {
        duckdb_table_description_destroy(&(p->table_description));
    }
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBTableDescription *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBTableDescription));
    return TypedData_Wrap_Struct(klass, &table_description_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBTableDescription);
}

static rubyDuckDBTableDescription *get_struct_table_description(VALUE obj) {
    rubyDuckDBTableDescription *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBTableDescription, &table_description_data_type, ctx);
    return ctx;
}

static VALUE rbduckdb_table_description_error_message(VALUE self) {
    rubyDuckDBTableDescription *ctx = get_struct_table_description(self);
    const char *p = duckdb_table_description_error(ctx->table_description);
    return p ? rb_str_new2(p) : Qnil;
}

/* nodoc */
static VALUE duckdb_table_description__initialize(VALUE self, VALUE con, VALUE catalog, VALUE schema, VALUE table) {
    char *pcatalog = NULL;
    char *pschema = NULL;
    char *ptable = NULL;
    duckdb_state state;
    rubyDuckDBTableDescription *ctx;
    rubyDuckDBConnection *ctxcon;

    if (!NIL_P(catalog)) {
        pcatalog = StringValuePtr(catalog);
    }
    if (!NIL_P(schema)) {
        pschema = StringValuePtr(schema);
    }
    if (!NIL_P(table)) {
        ptable = StringValuePtr(table);
    }
    ctxcon = rbduckdb_get_struct_connection(con);
    ctx = get_struct_table_description(self);

    if (ctx->table_description) {
        duckdb_table_description_destroy(&ctx->table_description);
    }

    if (pcatalog) {
        state = duckdb_table_description_create_ext(ctxcon->con, pcatalog, pschema, ptable, &ctx->table_description);
    } else {
        state = duckdb_table_description_create(ctxcon->con, pschema, ptable, &ctx->table_description);
    }
    if (state == DuckDBError) {
        return Qfalse;
    }
    return Qtrue;
}

/* nodoc */
static VALUE duckdb_table_description__column_count(VALUE self) {
    rubyDuckDBTableDescription *ctx;
    ctx = get_struct_table_description(self);
    return ULL2NUM(duckdb_table_description_get_column_count(ctx->table_description));
}

/* nodoc */
static VALUE duckdb_table_description__column_name(VALUE self, VALUE idx) {
    VALUE name = Qnil;
    rubyDuckDBTableDescription *ctx;
    ctx = get_struct_table_description(self);
    char *p = duckdb_table_description_get_column_name(ctx->table_description, NUM2ULL(idx));
    if (p) {
        name = rb_utf8_str_new_cstr(p);
        duckdb_free(p);
    }
    return name;
}

/* nodoc */
static VALUE duckdb_table_description__column_logical_type(VALUE self, VALUE idx) {
    rubyDuckDBTableDescription *ctx;
    duckdb_logical_type lt;
    ctx = get_struct_table_description(self);
    lt = duckdb_table_description_get_column_type(ctx->table_description, NUM2ULL(idx));
    return rbduckdb_create_logical_type(lt);
}

static VALUE duckdb_table_description__column_has_default(VALUE self, VALUE idx) {
    rubyDuckDBTableDescription *ctx;
    bool has_default;
    ctx = get_struct_table_description(self);
    duckdb_state state = duckdb_column_has_default(ctx->table_description, NUM2ULL(idx), &has_default);
    if (state == DuckDBError) {
        return Qnil;
    }
    return has_default ? Qtrue : Qfalse;
}

void rbduckdb_init_duckdb_table_description(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBTableDescription = rb_define_class_under(mDuckDB, "TableDescription", rb_cObject);
    rb_define_alloc_func(cDuckDBTableDescription, allocate);
    rb_define_method(cDuckDBTableDescription, "error_message", rbduckdb_table_description_error_message, 0);
    rb_define_private_method(cDuckDBTableDescription, "_initialize", duckdb_table_description__initialize, 4);
    rb_define_private_method(cDuckDBTableDescription, "_column_count", duckdb_table_description__column_count, 0);
    rb_define_private_method(cDuckDBTableDescription, "_column_name", duckdb_table_description__column_name, 1);
    rb_define_private_method(cDuckDBTableDescription, "_column_logical_type", duckdb_table_description__column_logical_type, 1);
    rb_define_private_method(cDuckDBTableDescription, "_column_has_default", duckdb_table_description__column_has_default, 1);
}
#endif
