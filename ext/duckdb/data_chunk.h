#ifndef RUBY_DUCKDB_DATA_CHUNK_H
#define RUBY_DUCKDB_DATA_CHUNK_H

struct _rubyDuckDBDataChunk {
    duckdb_data_chunk data_chunk;
    bool owned;
    /* Lazily loaded column types for set_value; destroyed with the chunk. */
    duckdb_logical_type *column_types;
    idx_t column_types_count;
    /*
     * Bumped whenever data_chunk stops pointing at the chunk it pointed at
     * before (invalidated or re-initialized), so that Vectors obtained from
     * the old chunk can tell they are stale.
     */
    uint64_t generation;
};

typedef struct _rubyDuckDBDataChunk rubyDuckDBDataChunk;

rubyDuckDBDataChunk *rbduckdb_get_struct_data_chunk(VALUE obj);
VALUE rbduckdb_create_data_chunk(duckdb_data_chunk chunk, bool owned);
duckdb_data_chunk rbduckdb_get_valid_data_chunk(VALUE obj);
void rbduckdb_data_chunk_invalidate(VALUE obj);
bool rbduckdb_data_chunk_is_current(VALUE obj, uint64_t generation);
void rbduckdb_init_data_chunk(void);

#endif
