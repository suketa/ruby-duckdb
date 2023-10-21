#ifndef RUBY_DUCKDB_PENDING_RESULT_H
#define RUBY_DUCKDB_PENDING_RESULT_H

struct _rubyDuckDBPendingResult {
    duckdb_pending_result pending_result;
};

typedef struct _rubyDuckDBPendingResult rubyDuckDBPendingResult;

void init_duckdb_pending_result(void);
#endif
