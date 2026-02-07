#ifndef RUBY_DUCKDB_RESULT_H
#define RUBY_DUCKDB_RESULT_H

struct _rubyDuckDBResult {
    duckdb_result result;
};

typedef struct _rubyDuckDBResult rubyDuckDBResult;

rubyDuckDBResult *get_struct_result(VALUE obj);
void rbduckdb_init_duckdb_result(void);
VALUE rbduckdb_create_result(void);
VALUE rbduckdb_vector_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index);

#endif

