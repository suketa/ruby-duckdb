#ifndef RUBY_DUCKDB_DATA_CHUNK_H
#define RUBY_DUCKDB_DATA_CHUNK_H

struct _rubyDuckDBDataChunk {
    duckdb_data_chunk data_chunk;
};

typedef struct _rubyDuckDBDataChunk rubyDuckDBDataChunk;

rubyDuckDBDataChunk *get_struct_data_chunk(VALUE obj);
void rbduckdb_init_duckdb_data_chunk(void);

#endif
