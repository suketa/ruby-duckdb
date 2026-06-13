#ifndef RUBY_DUCKDB_RESULT_H
#define RUBY_DUCKDB_RESULT_H

/*
 * Allocated with plain calloc/free and reference-counted: the Ruby Result
 * object holds one reference, and each exported Arrow stream holds another,
 * so the duckdb_result stays valid for consumers that outlive the Ruby
 * objects. rbduckdb_result_unref() must not call any Ruby API: it runs from
 * GC sweep (deallocate) and from Arrow stream release callbacks.
 */
struct _rubyDuckDBResult {
    duckdb_result result;
    bool arrow_exported;
    rb_atomic_t refcount;
};

typedef struct _rubyDuckDBResult rubyDuckDBResult;

rubyDuckDBResult *rbduckdb_get_struct_result(VALUE obj);
void rbduckdb_result_ref(rubyDuckDBResult *ctx);
void rbduckdb_result_unref(rubyDuckDBResult *ctx);
void rbduckdb_init_result(void);
VALUE rbduckdb_create_result(void);
VALUE rbduckdb_vector_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index);

#endif
