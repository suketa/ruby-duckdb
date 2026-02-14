/* frozen_string_literal: true */

#include "ruby-duckdb.h"

static VALUE mDuckDBMemoryHelper;

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_bigint(ptr, index, value) -> nil
 *
 * Writes a 64-bit signed integer (BIGINT) to raw memory.
 *
 *   ptr = vector.get_data
 *   DuckDB::MemoryHelper.write_bigint(ptr, 0, 42)  # Write 42 at index 0
 *   DuckDB::MemoryHelper.write_bigint(ptr, 1, 84)  # Write 84 at index 1
 */
static VALUE rbduckdb_memory_helper_write_bigint(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    int64_t *data;
    idx_t idx;
    int64_t val;

    (void)self;

    /* Convert Ruby values to C types */
    data = (int64_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    val = (int64_t)NUM2LL(value);

    /* Write the value */
    data[idx] = val;

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_double(ptr, index, value) -> nil
 *
 * Writes a 64-bit floating point number (DOUBLE) to raw memory.
 *
 *   ptr = vector.get_data
 *   DuckDB::MemoryHelper.write_double(ptr, 0, 3.14)
 */
static VALUE rbduckdb_memory_helper_write_double(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    double *data;
    idx_t idx;
    double val;

    (void)self;

    data = (double *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    val = NUM2DBL(value);

    data[idx] = val;

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_integer(ptr, index, value) -> nil
 *
 * Writes a 32-bit signed integer (INTEGER) to raw memory.
 *
 *   ptr = vector.get_data
 *   DuckDB::MemoryHelper.write_integer(ptr, 0, 42)
 */
static VALUE rbduckdb_memory_helper_write_integer(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    int32_t *data;
    idx_t idx;
    int32_t val;

    (void)self;

    data = (int32_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    val = (int32_t)NUM2INT(value);

    data[idx] = val;

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_boolean(ptr, index, value) -> nil
 *
 * Writes a boolean to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_boolean(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    bool *data;
    idx_t idx;

    (void)self;

    data = (bool *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = RTEST(value);

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_tinyint(ptr, index, value) -> nil
 *
 * Writes an 8-bit signed integer (TINYINT) to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_tinyint(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    int8_t *data;
    idx_t idx;

    (void)self;

    data = (int8_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = (int8_t)NUM2INT(value);

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_smallint(ptr, index, value) -> nil
 *
 * Writes a 16-bit signed integer (SMALLINT) to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_smallint(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    int16_t *data;
    idx_t idx;

    (void)self;

    data = (int16_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = (int16_t)NUM2INT(value);

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_utinyint(ptr, index, value) -> nil
 *
 * Writes an 8-bit unsigned integer (UTINYINT) to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_utinyint(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    uint8_t *data;
    idx_t idx;

    (void)self;

    data = (uint8_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = (uint8_t)NUM2UINT(value);

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_usmallint(ptr, index, value) -> nil
 *
 * Writes a 16-bit unsigned integer (USMALLINT) to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_usmallint(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    uint16_t *data;
    idx_t idx;

    (void)self;

    data = (uint16_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = (uint16_t)NUM2UINT(value);

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_uinteger(ptr, index, value) -> nil
 *
 * Writes a 32-bit unsigned integer (UINTEGER) to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_uinteger(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    uint32_t *data;
    idx_t idx;

    (void)self;

    data = (uint32_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = (uint32_t)NUM2ULL(value);

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_ubigint(ptr, index, value) -> nil
 *
 * Writes a 64-bit unsigned integer (UBIGINT) to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_ubigint(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    uint64_t *data;
    idx_t idx;

    (void)self;

    data = (uint64_t *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = NUM2ULL(value);

    return Qnil;
}

/*
 * call-seq:
 *   DuckDB::MemoryHelper.write_float(ptr, index, value) -> nil
 *
 * Writes a 32-bit floating point number (FLOAT) to raw memory.
 */
static VALUE rbduckdb_memory_helper_write_float(VALUE self, VALUE ptr, VALUE index, VALUE value) {
    float *data;
    idx_t idx;

    (void)self;

    data = (float *)NUM2ULL(ptr);
    idx = (idx_t)NUM2ULL(index);
    data[idx] = (float)NUM2DBL(value);

    return Qnil;
}

void rbduckdb_init_memory_helper(void) {
    mDuckDBMemoryHelper = rb_define_module_under(mDuckDB, "MemoryHelper");

    rb_define_singleton_method(mDuckDBMemoryHelper, "write_bigint", rbduckdb_memory_helper_write_bigint, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_double", rbduckdb_memory_helper_write_double, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_integer", rbduckdb_memory_helper_write_integer, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_boolean", rbduckdb_memory_helper_write_boolean, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_tinyint", rbduckdb_memory_helper_write_tinyint, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_smallint", rbduckdb_memory_helper_write_smallint, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_utinyint", rbduckdb_memory_helper_write_utinyint, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_usmallint", rbduckdb_memory_helper_write_usmallint, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_uinteger", rbduckdb_memory_helper_write_uinteger, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_ubigint", rbduckdb_memory_helper_write_ubigint, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_float", rbduckdb_memory_helper_write_float, 3);
}
