#include "ruby-duckdb.h"

static VALUE cDuckDBExtractedStatements;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static const rb_data_type_t extract_statements_data_type = {
    "DuckDB/ExtractedStatements",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);

static VALUE duckdb_extracted_statements_initialize(VALUE self, VALUE con, VALUE query);
static VALUE duckdb_extracted_statements_destroy(VALUE self);
static VALUE duckdb_extracted_statements_size(VALUE self);
static VALUE duckdb_extracted_statements_prepared_statement(VALUE self, VALUE con, VALUE index);

static void deallocate(void *ctx) {
    rubyDuckDBExtractedStatements *p = (rubyDuckDBExtractedStatements *)ctx;

    duckdb_destroy_extracted(&(p->extracted_statements));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBExtractedStatements *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBExtractedStatements));
    ctx->num_statements = 0;

    return TypedData_Wrap_Struct(klass, &extract_statements_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBExtractedStatements);
}

static VALUE duckdb_extracted_statements_initialize(VALUE self, VALUE con, VALUE query) {
    rubyDuckDBConnection *pcon;
    rubyDuckDBExtractedStatements *ctx;
    char *pquery;
    const char *error;

    if (rb_obj_is_kind_of(con, cDuckDBConnection) != Qtrue) {
        rb_raise(rb_eTypeError, "1st argument must be DuckDB::Connection");
    }

    pquery = StringValuePtr(query);
    pcon = get_struct_connection(con);
    TypedData_Get_Struct(self, rubyDuckDBExtractedStatements, &extract_statements_data_type, ctx);

    ctx->num_statements = duckdb_extract_statements(pcon->con, pquery, &(ctx->extracted_statements));

    if (ctx->num_statements == 0) {
        error = duckdb_extract_statements_error(ctx->extracted_statements);
        rb_raise(eDuckDBError, "%s", error ? error : "Failed to extract statements(Database connection closed?).");
    }

    return self;
}

static VALUE duckdb_extracted_statements_destroy(VALUE self) {
    rubyDuckDBExtractedStatements *ctx;

    TypedData_Get_Struct(self, rubyDuckDBExtractedStatements, &extract_statements_data_type, ctx);

    duckdb_destroy_extracted(&(ctx->extracted_statements));

    return Qnil;
}

static VALUE duckdb_extracted_statements_size(VALUE self) {
    rubyDuckDBExtractedStatements *ctx;

    TypedData_Get_Struct(self, rubyDuckDBExtractedStatements, &extract_statements_data_type, ctx);

    return ULL2NUM(ctx->num_statements);
}

static VALUE duckdb_extracted_statements_prepared_statement(VALUE self, VALUE con, VALUE index) {
    rubyDuckDBConnection *pcon;
    rubyDuckDBExtractedStatements *ctx;

    if (rb_obj_is_kind_of(con, cDuckDBConnection) != Qtrue) {
        rb_raise(rb_eTypeError, "1st argument must be DuckDB::Connection");
    }
    pcon = get_struct_connection(con);
    TypedData_Get_Struct(self, rubyDuckDBExtractedStatements, &extract_statements_data_type, ctx);

    return rbduckdb_prepared_statement_new(pcon->con, ctx->extracted_statements, NUM2ULL(index));
}

void rbduckdb_init_duckdb_extracted_statements(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBExtractedStatements = rb_define_class_under(mDuckDB, "ExtractedStatementsImpl", rb_cObject);

    rb_define_alloc_func(cDuckDBExtractedStatements, allocate);
    rb_define_method(cDuckDBExtractedStatements, "initialize", duckdb_extracted_statements_initialize, 2);
    rb_define_method(cDuckDBExtractedStatements, "destroy", duckdb_extracted_statements_destroy, 0);
    rb_define_method(cDuckDBExtractedStatements, "size", duckdb_extracted_statements_size, 0);
    rb_define_method(cDuckDBExtractedStatements, "prepared_statement", duckdb_extracted_statements_prepared_statement, 2);
}
