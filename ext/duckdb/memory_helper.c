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

void rbduckdb_init_memory_helper(void) {
    mDuckDBMemoryHelper = rb_define_module_under(mDuckDB, "MemoryHelper");

    rb_define_singleton_method(mDuckDBMemoryHelper, "write_bigint", rbduckdb_memory_helper_write_bigint, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_double", rbduckdb_memory_helper_write_double, 3);
    rb_define_singleton_method(mDuckDBMemoryHelper, "write_integer", rbduckdb_memory_helper_write_integer, 3);
}
