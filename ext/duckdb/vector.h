#ifndef RUBY_DUCKDB_VECTOR_H
#define RUBY_DUCKDB_VECTOR_H

struct _rubyDuckDBVector {
    duckdb_vector vector;
};

typedef struct _rubyDuckDBVector rubyDuckDBVector;

rubyDuckDBVector *get_struct_vector(VALUE obj);
void rbduckdb_init_duckdb_vector(void);

#endif
