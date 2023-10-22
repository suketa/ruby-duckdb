#ifndef RUBY_DUCKDB_PENDING_RESULT_H
#define RUBY_DUCKDB_PENDING_RESULT_H

struct _rubyDuckDBPendingResult {
    duckdb_pending_result pending_result;
    duckdb_pending_state state;
};

typedef struct _rubyDuckDBPendingResult rubyDuckDBPendingResult;

rubyDuckDBPendingResult *get_struct_pending_result(VALUE obj);
void init_duckdb_pending_result(void);
#endif
