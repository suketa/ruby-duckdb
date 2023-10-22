#include "ruby-duckdb.h"

static VALUE cDuckDBPendingResult;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE duckdb_pending_result_initialize(VALUE self, VALUE oDuckDBPreparedStatement);
static VALUE duckdb_pending_result_execute_task(VALUE self);
static VALUE duckdb_pending_result_execution_finished_p(VALUE self);
static VALUE duckdb_pending_result__state(VALUE self);

static const rb_data_type_t pending_result_data_type = {
    "DuckDB/PendingResult",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBPendingResult *p = (rubyDuckDBPendingResult *)ctx;

    duckdb_destroy_pending(&(p->pending_result));
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBPendingResult *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBPendingResult));
    ctx->state = DUCKDB_PENDING_RESULT_NOT_READY;
    return TypedData_Wrap_Struct(klass, &pending_result_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBPendingResult);
}

static VALUE duckdb_pending_result_initialize(VALUE self, VALUE oDuckDBPreparedStatement) {
    rubyDuckDBPendingResult *ctx = get_struct_pending_result(self);
    rubyDuckDBPreparedStatement *stmt = get_struct_prepared_statement(oDuckDBPreparedStatement);

    if (duckdb_pending_prepared(stmt->prepared_statement, &(ctx->pending_result)) == DuckDBError) {
        rb_raise(eDuckDBError, "%s", duckdb_pending_error(ctx->pending_result));
    }
    return self;
}

static VALUE duckdb_pending_result_execute_task(VALUE self) {
    rubyDuckDBPendingResult *ctx = get_struct_pending_result(self);
    ctx->state = duckdb_pending_execute_task(ctx->pending_result);
    return Qnil;
}

static VALUE duckdb_pending_result_execution_finished_p(VALUE self) {
    rubyDuckDBPendingResult *ctx = get_struct_pending_result(self);
    return duckdb_pending_execution_is_finished(ctx->state) ? Qtrue : Qfalse;
}

static VALUE duckdb_pending_result__state(VALUE self) {
    rubyDuckDBPendingResult *ctx = get_struct_pending_result(self);
    return INT2FIX(ctx->state);
}

rubyDuckDBPendingResult *get_struct_pending_result(VALUE obj) {
    rubyDuckDBPendingResult *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBPendingResult, &pending_result_data_type, ctx);
    return ctx;
}

void init_duckdb_pending_result(void) {
    cDuckDBPendingResult = rb_define_class_under(mDuckDB, "PendingResult", rb_cObject);
    rb_define_alloc_func(cDuckDBPendingResult, allocate);

    rb_define_method(cDuckDBPendingResult, "initialize", duckdb_pending_result_initialize, 1);
    rb_define_method(cDuckDBPendingResult, "execute_task", duckdb_pending_result_execute_task, 0);
    rb_define_method(cDuckDBPendingResult, "execution_finished?", duckdb_pending_result_execution_finished_p, 0);
    rb_define_private_method(cDuckDBPendingResult, "_state", duckdb_pending_result__state, 0);
}
