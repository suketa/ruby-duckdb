#include "ruby-duckdb.h"

VALUE cDuckDBExpression;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE rbduckdb_expression_foldable_p(VALUE self);
static VALUE rbduckdb_expression_fold(VALUE self, VALUE client_context);

static const rb_data_type_t expression_data_type = {
    "DuckDB/Expression",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBExpression *p = (rubyDuckDBExpression *)ctx;

    if (p->expression) {
        duckdb_destroy_expression(&(p->expression));
    }

    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBExpression *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBExpression));
    return TypedData_Wrap_Struct(klass, &expression_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBExpression);
}

VALUE rbduckdb_expression_new(duckdb_expression expression) {
    rubyDuckDBExpression *ctx;
    VALUE obj = allocate(cDuckDBExpression);

    TypedData_Get_Struct(obj, rubyDuckDBExpression, &expression_data_type, ctx);
    ctx->expression = expression;

    return obj;
}

/*
 * call-seq:
 *   expression.foldable? -> true or false
 *
 * Returns +true+ if the expression can be folded to a constant at query
 * planning time (e.g. literals, constant arithmetic), +false+ otherwise
 * (e.g. column references, non-deterministic functions).
 */
static VALUE rbduckdb_expression_foldable_p(VALUE self) {
    rubyDuckDBExpression *ctx;
    TypedData_Get_Struct(self, rubyDuckDBExpression, &expression_data_type, ctx);
    return duckdb_expression_is_foldable(ctx->expression) ? Qtrue : Qfalse;
}

/*
 * call-seq:
 *   expression.fold(client_context) -> DuckDB::Value
 *
 * Evaluates the expression at planning time and returns a DuckDB::Value
 * holding the constant result. Raises DuckDB::Error if folding fails or
 * the expression is not foldable.
 */
static VALUE rbduckdb_expression_fold(VALUE self, VALUE client_context) {
    rubyDuckDBExpression *expr_ctx;
    rubyDuckDBClientContext *cc_ctx;
    duckdb_value out_value = NULL;
    duckdb_error_data error_data;

    TypedData_Get_Struct(self, rubyDuckDBExpression, &expression_data_type, expr_ctx);
    cc_ctx = get_struct_client_context(client_context);

    error_data = duckdb_expression_fold(cc_ctx->client_context, expr_ctx->expression, &out_value);

    if (duckdb_error_data_has_error(error_data)) {
        VALUE msg = rb_str_new_cstr(duckdb_error_data_message(error_data));
        duckdb_destroy_error_data(&error_data);
        if (out_value) {
            duckdb_destroy_value(&out_value);
        }
        rb_raise(eDuckDBError, "%s", StringValueCStr(msg));
    }
    duckdb_destroy_error_data(&error_data);

    return rbduckdb_value_impl_new(out_value);
}

void rbduckdb_init_duckdb_expression(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBExpression = rb_define_class_under(mDuckDB, "Expression", rb_cObject);
    rb_define_alloc_func(cDuckDBExpression, allocate);
    rb_define_method(cDuckDBExpression, "foldable?", rbduckdb_expression_foldable_p, 0);
    rb_define_method(cDuckDBExpression, "fold", rbduckdb_expression_fold, 1);
}
