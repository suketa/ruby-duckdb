#ifndef RUBY_DUCKDB_RESULT_H
#define RUBY_DUCKDB_RESULT_H

struct _rubyDuckDBResult {
  duckdb_result result;
  duckdb_data_chunk chunk;
  char **columns;
  idx_t chunk_count;
  idx_t chunk_row_count;
  idx_t chunk_idx;
  idx_t chunk_row_idx;
};

typedef struct _rubyDuckDBResult rubyDuckDBResult;

rubyDuckDBResult *get_struct_result(VALUE obj);
void init_duckdb_result(void);
VALUE create_result(void);

#endif
