#ifndef RUBY_DUCKDB_VECTOR_H
#define RUBY_DUCKDB_VECTOR_H

struct _rubyDuckDBVector {
    duckdb_vector vector;
};

typedef struct _rubyDuckDBVector rubyDuckDBVector;

rubyDuckDBVector *rbduckdb_get_struct_vector(VALUE obj);
void rbduckdb_init_vector(void);

#endif
