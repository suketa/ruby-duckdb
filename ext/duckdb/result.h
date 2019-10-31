#ifndef RUBY_DUCKDB_RESULT_H
#define RUBY_DUCKDB_RESULT_H

struct _rubyDuckDBResult {
    duckdb_result result;
};

typedef struct _rubyDuckDBResult rubyDuckDBResult;

void init_duckdb_result(void);
VALUE create_result(void);

#endif

