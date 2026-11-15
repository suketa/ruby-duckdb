#ifndef RUBY_DUCKDB_VECTOR_H
#define RUBY_DUCKDB_VECTOR_H

struct _rubyDuckDBVector {
    duckdb_vector vector;
    /* The DuckDB::DataChunk this vector belongs to (keeps it alive). */
    VALUE data_chunk;
    /* The chunk's generation when this vector was obtained. */
    unsigned long data_chunk_generation;
};

typedef struct _rubyDuckDBVector rubyDuckDBVector;

VALUE rbduckdb_vector_new(VALUE data_chunk, unsigned long data_chunk_generation, duckdb_vector vector);
void rbduckdb_init_vector(void);

#endif
