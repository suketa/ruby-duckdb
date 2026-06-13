#include "ruby-duckdb.h"

/*
 * Internal helpers backing DuckDB::Connection#append_arrow. They consume an
 * Arrow producer's struct ArrowArrayStream (given by its address) and convert
 * each chunk into a DuckDB::DataChunk using DuckDB's unstable Arrow C API. The
 * Ruby layer owns the loop, the appender lifecycle, and error handling; these
 * primitives only do the raw-pointer / C-API work.
 */

static VALUE cDuckDBArrowConvertedSchema;

typedef struct {
    duckdb_arrow_converted_schema converted_schema;
} rubyDuckDBArrowConvertedSchema;

static void deallocate(void *ctx);
static VALUE allocate(VALUE klass);
static size_t memsize(const void *p);
static void raise_error_data(duckdb_error_data error_data);

static VALUE connection__arrow_converted_schema(VALUE self, VALUE address);
static VALUE connection__arrow_next_chunk(VALUE self, VALUE address, VALUE converted);
static VALUE connection__arrow_release(VALUE self, VALUE address);

static const rb_data_type_t arrow_converted_schema_data_type = {
    "DuckDB/ArrowConvertedSchema",
    {NULL, deallocate, memsize,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void deallocate(void *ctx) {
    rubyDuckDBArrowConvertedSchema *p = (rubyDuckDBArrowConvertedSchema *)ctx;

    if (p->converted_schema) {
        duckdb_destroy_arrow_converted_schema(&(p->converted_schema));
    }
    xfree(p);
}

static VALUE allocate(VALUE klass) {
    rubyDuckDBArrowConvertedSchema *ctx = xcalloc((size_t)1, sizeof(rubyDuckDBArrowConvertedSchema));
    return TypedData_Wrap_Struct(klass, &arrow_converted_schema_data_type, ctx);
}

static size_t memsize(const void *p) {
    return sizeof(rubyDuckDBArrowConvertedSchema);
}

static void raise_error_data(duckdb_error_data error_data) {
    VALUE message;

    if (error_data == NULL) {
        return;
    }
    if (!duckdb_error_data_has_error(error_data)) {
        duckdb_destroy_error_data(&error_data);
        return;
    }
    message = rb_str_new_cstr(duckdb_error_data_message(error_data));
    duckdb_destroy_error_data(&error_data);
    rb_raise(eDuckDBError, "%s", StringValueCStr(message));
}

static struct ArrowArrayStream *stream_from_address(VALUE address) {
    return (struct ArrowArrayStream *)(uintptr_t)NUM2ULL(address);
}

/* :nodoc: */
static VALUE connection__arrow_converted_schema(VALUE self, VALUE address) {
    rubyDuckDBConnection *ctx;
    struct ArrowArrayStream *stream;
    struct ArrowSchema schema;
    duckdb_arrow_converted_schema converted_schema = NULL;
    duckdb_error_data error_data;
    VALUE obj;
    rubyDuckDBArrowConvertedSchema *schema_ctx;
    int rc;

    ctx = rbduckdb_get_struct_connection(self);
    stream = stream_from_address(address);

    memset(&schema, 0, sizeof(schema));
    rc = stream->get_schema(stream, &schema);
    if (rc != 0) {
        const char *err = stream->get_last_error(stream);
        rb_raise(eDuckDBError, "failed to get Arrow schema: %s", err ? err : "unknown error");
    }

    error_data = duckdb_schema_from_arrow(ctx->con, &schema, &converted_schema);
    if (schema.release != NULL) {
        schema.release(&schema);
    }
    raise_error_data(error_data);

    obj = allocate(cDuckDBArrowConvertedSchema);
    TypedData_Get_Struct(obj, rubyDuckDBArrowConvertedSchema, &arrow_converted_schema_data_type, schema_ctx);
    schema_ctx->converted_schema = converted_schema;
    return obj;
}

/* :nodoc: */
static VALUE connection__arrow_next_chunk(VALUE self, VALUE address, VALUE converted) {
    rubyDuckDBConnection *ctx;
    rubyDuckDBArrowConvertedSchema *schema_ctx;
    struct ArrowArrayStream *stream;
    struct ArrowArray array;
    duckdb_data_chunk chunk = NULL;
    duckdb_error_data error_data;
    int rc;

    ctx = rbduckdb_get_struct_connection(self);
    TypedData_Get_Struct(converted, rubyDuckDBArrowConvertedSchema, &arrow_converted_schema_data_type, schema_ctx);
    stream = stream_from_address(address);

    memset(&array, 0, sizeof(array));
    rc = stream->get_next(stream, &array);
    if (rc != 0) {
        const char *err = stream->get_last_error(stream);
        rb_raise(eDuckDBError, "failed to get next Arrow chunk: %s", err ? err : "unknown error");
    }
    /* End of stream: a released array (release == NULL). */
    if (array.release == NULL) {
        return Qnil;
    }

    /* duckdb_data_chunk_from_arrow takes ownership of the array (nulls its
     * release). On error before that, we still own it and must release it. */
    error_data = duckdb_data_chunk_from_arrow(ctx->con, &array, schema_ctx->converted_schema, &chunk);
    if (error_data != NULL && duckdb_error_data_has_error(error_data)) {
        if (array.release != NULL) {
            array.release(&array);
        }
        raise_error_data(error_data);
    } else if (error_data != NULL) {
        duckdb_destroy_error_data(&error_data);
    }

    return rbduckdb_create_data_chunk(chunk, true);
}

/* :nodoc: */
static VALUE connection__arrow_release(VALUE self, VALUE address) {
    struct ArrowArrayStream *stream = stream_from_address(address);

    if (stream != NULL && stream->release != NULL) {
        stream->release(stream);
    }
    return Qnil;
}

void rbduckdb_init_arrow_import(void) {
#if 0
    VALUE mDuckDB = rb_define_module("DuckDB");
#endif
    cDuckDBArrowConvertedSchema = rb_define_class_under(mDuckDB, "ArrowConvertedSchema", rb_cObject);
    rb_define_alloc_func(cDuckDBArrowConvertedSchema, allocate);

    rb_define_private_method(cDuckDBConnection, "_arrow_converted_schema", connection__arrow_converted_schema, 1);
    rb_define_private_method(cDuckDBConnection, "_arrow_next_chunk", connection__arrow_next_chunk, 2);
    rb_define_private_method(cDuckDBConnection, "_arrow_release", connection__arrow_release, 1);
}
