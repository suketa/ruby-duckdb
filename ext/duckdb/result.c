#include "ruby-duckdb.h"

struct chunk_arg {
    duckdb_data_chunk chunk;
    idx_t col_count;
    duckdb_logical_type *types;
};

static VALUE cDuckDBResult;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static VALUE result_column_count(VALUE oDuckDBResult);
static VALUE result_rows_changed(VALUE oDuckDBResult);
static VALUE result_columns(VALUE oDuckDBResult);
static VALUE destroy_data_chunk(VALUE arg);
static duckdb_logical_type *result_column_types(rubyDuckDBResult *ctx);

static VALUE result__chunk_stream(VALUE oDuckDBResult);
static VALUE result_arrow_c_stream(VALUE oDuckDBResult);
static VALUE yield_rows(VALUE arg);
static VALUE result__return_type(VALUE oDuckDBResult);
static VALUE result__statement_type(VALUE oDuckDBResult);
static VALUE result__enum_internal_type(VALUE oDuckDBResult, VALUE col_idx);

/*
 * No RUBY_TYPED_FREE_IMMEDIATELY: deallocate can block, and must not run
 * during GC sweep: a streaming result holds a ClientContext reference; dropping the last
 * one rolls back an open transaction and may shut the database down.
 */
static const rb_data_type_t result_data_type = {
    "DuckDB/Result",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_WB_PROTECTED
};

static void deallocate(void *ctx) {
    rubyDuckDBResult *p = (rubyDuckDBResult *)ctx;

    rbduckdb_result_unref(p);
}

static VALUE allocate(VALUE klass) {
    /*
     * Use calloc (not xcalloc): rbduckdb_result_unref frees the struct, and it
     * can run from the Arrow stream release callback on a non-Ruby thread
     * where xfree is unsafe.
     */
    rubyDuckDBResult *ctx = calloc((size_t)1, sizeof(rubyDuckDBResult));

    if (ctx == NULL) {
        rb_raise(rb_eNoMemError, "failed to allocate DuckDB::Result");
    }
    ctx->refcount = 1;
    return TypedData_Wrap_Struct(klass, &result_data_type, ctx);
}

void rbduckdb_result_ref(rubyDuckDBResult *ctx) {
    RUBY_ATOMIC_FETCH_ADD(ctx->refcount, 1);
}

void rbduckdb_result_unref(rubyDuckDBResult *ctx) {
    idx_t col_idx;

    if (RUBY_ATOMIC_FETCH_SUB(ctx->refcount, 1) == 1) {
        for (col_idx = 0; col_idx < ctx->column_types_count; col_idx++) {
            duckdb_destroy_logical_type(&(ctx->column_types[col_idx]));
        }
        free(ctx->column_types);
        duckdb_destroy_result(&(ctx->result));
        free(ctx);
    }
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBResult);
}

rubyDuckDBResult *rbduckdb_get_struct_result(VALUE obj) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(obj, rubyDuckDBResult, &result_data_type, ctx);
    return ctx;
}

/*
 *  call-seq:
 *    result.rows_changed -> integer
 *
 *  Returns the count of rows changed.
 *
 *    DuckDB::Database.open do |db|
 *      db.connect do |con|
 *        r = con.query('CREATE TABLE t2 (id INT)')
 *        r.rows_changed # => 0
 *        r = con.query('INSERT INTO t2 VALUES (1), (2), (3)')
 *        r.rows_changed # => 3
 *        r = con.query('UPDATE t2 SET id = id + 1 WHERE id > 1')
 *        r.rows_changed # => 2
 *        r = con.query('DELETE FROM t2 WHERE id = 0')
 *        r.rows_changed # => 0
 *        r = con.query('DELETE FROM t2 WHERE id = 4')
 *        r.rows_changed # => 1
 *      end
 *    end
 *
 */
static VALUE result_rows_changed(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_rows_changed(&(ctx->result)));
}

/*
 *  call-seq:
 *    result.column_count -> Integer
 *
 *  Returns the column size of the result.
 *
 *    DuckDB::Database.open do |db|
 *      db.connect do |con|
 *        r = con.query('CREATE TABLE t2 (id INT, name VARCHAR(128))')
 *        r = con.query("INSERT INTO t2 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Catherine')")
 *        r = con.query('SELECT id FROM t2')
 *        r.column_count # => 1
 *        r = con.query('SELECT id, name FROM t2')
 *        r.column_count # => 2
 *      end
 *    end
 *
 */
static VALUE result_column_count(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return LL2NUM(duckdb_column_count(&(ctx->result)));
}

/*
 *  call-seq:
 *    result.columns -> DuckDB::Column[]
 *
 *  Returns the column class Lists.
 *
 */
static VALUE result_columns(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    idx_t col_idx;
    idx_t column_count = duckdb_column_count(&(ctx->result));

    VALUE ary = rb_ary_new2(column_count);
    for(col_idx = 0; col_idx < column_count; col_idx++) {
        VALUE column = rbduckdb_create_column(oDuckDBResult, col_idx);
        rb_ary_store(ary, col_idx, column);
    }
    return ary;
}

static VALUE destroy_data_chunk(VALUE arg) {
    struct chunk_arg *p = (struct chunk_arg *)arg;
    duckdb_destroy_data_chunk(&(p->chunk));
    return Qnil;
}

static duckdb_logical_type *result_column_types(rubyDuckDBResult *ctx) {
    idx_t col_idx;
    idx_t column_count;

    if (ctx->column_types == NULL) {
        column_count = duckdb_column_count(&(ctx->result));
        /* Use calloc (not xcalloc): freed by rbduckdb_result_unref (see allocate). */
        ctx->column_types = calloc(column_count + 1, sizeof(duckdb_logical_type));
        if (ctx->column_types == NULL) {
            rb_raise(rb_eNoMemError, "failed to allocate DuckDB::Result column types");
        }
        for (col_idx = 0; col_idx < column_count; col_idx++) {
            ctx->column_types[col_idx] = duckdb_column_logical_type(&(ctx->result), col_idx);
        }
        ctx->column_types_count = column_count;
    }
    return ctx->column_types;
}

/* :nodoc: */
static VALUE result__chunk_stream(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    struct chunk_arg arg;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);

    RETURN_ENUMERATOR(oDuckDBResult, 0, 0);

    arg.col_count = duckdb_column_count(&(ctx->result));
    arg.types = result_column_types(ctx);

    while((arg.chunk = duckdb_fetch_chunk(ctx->result)) != NULL) {
        rb_ensure(yield_rows, (VALUE)&arg, destroy_data_chunk, (VALUE)&arg);
    }
    return Qnil;
}

static VALUE yield_rows(VALUE arg) {
    idx_t row_count;
    idx_t row_idx;
    idx_t col_idx;
    duckdb_vector vector;
    VALUE row;
    VALUE val;

    struct chunk_arg *p = (struct chunk_arg *)arg;

    row_count = duckdb_data_chunk_get_size(p->chunk);
    for (row_idx = 0; row_idx < row_count; row_idx++) {
        row = rb_ary_new2(p->col_count);
        for (col_idx = 0; col_idx < p->col_count; col_idx++) {
            vector = duckdb_data_chunk_get_vector(p->chunk, col_idx);
            val = rbduckdb_vector_value_at(vector, p->types[col_idx], row_idx);
            rb_ary_store(row, col_idx, val);
        }
        rb_yield(row);
    }
    return Qnil;
}

/* :nodoc: */
static VALUE result__return_type(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return INT2FIX(duckdb_result_return_type(ctx->result));
}

/* :nodoc: */
static VALUE result__statement_type(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    return INT2FIX(duckdb_result_statement_type(ctx->result));
}

/* :nodoc: */
static VALUE result__enum_internal_type(VALUE oDuckDBResult, VALUE col_idx) {
    rubyDuckDBResult *ctx;
    VALUE type = Qnil;
    duckdb_logical_type logical_type;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    logical_type = duckdb_column_logical_type(&(ctx->result), NUM2LL(col_idx));
    if (logical_type) {
        type = LL2NUM(duckdb_enum_internal_type(logical_type));
    }
    duckdb_destroy_logical_type(&logical_type);
    return type;
}

VALUE rbduckdb_create_result(void) {
    return allocate(cDuckDBResult);
}

/*
 *  call-seq:
 *    result.arrow_c_stream -> DuckDB::ArrowArrayStream
 *
 *  [EXPERIMENTAL] Exports the result as an Arrow C stream
 *  (Arrow C Data Interface). The returned stream object satisfies the
 *  Ruby Arrow C stream protocol, so it can be consumed directly by
 *  ruby-polars, red-arrow and other Arrow consumers:
 *
 *    result = con.query('SELECT * FROM users')
 *    df = Polars::DataFrame.new(result)
 *
 *  The stream consumes the result's chunks; a result can be exported
 *  only once. This API is built on DuckDB's unstable Arrow C API and
 *  may change in any minor release.
 */
static VALUE result_arrow_c_stream(VALUE oDuckDBResult) {
    rubyDuckDBResult *ctx;
    VALUE stream;

    TypedData_Get_Struct(oDuckDBResult, rubyDuckDBResult, &result_data_type, ctx);
    if (ctx->arrow_exported) {
        rb_raise(eDuckDBError, "result is already exported as an Arrow stream");
    }
    stream = rbduckdb_create_arrow_array_stream(oDuckDBResult);
    ctx->arrow_exported = true;
    return stream;
}

void rbduckdb_init_result(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBResult = rb_define_class_under(mDuckDB, "Result", rb_cObject);

    rb_define_alloc_func(cDuckDBResult, allocate);

    rb_define_method(cDuckDBResult, "column_count", result_column_count, 0);
    rb_define_method(cDuckDBResult, "rows_changed", result_rows_changed, 0);
    rb_define_method(cDuckDBResult, "columns", result_columns, 0);
    rb_define_private_method(cDuckDBResult, "_chunk_stream", result__chunk_stream, 0);
    rb_define_method(cDuckDBResult, "arrow_c_stream", result_arrow_c_stream, 0);
    rb_define_private_method(cDuckDBResult, "_return_type", result__return_type, 0);
    rb_define_private_method(cDuckDBResult, "_statement_type", result__statement_type, 0);

    rb_define_private_method(cDuckDBResult, "_enum_internal_type", result__enum_internal_type, 1);
}
