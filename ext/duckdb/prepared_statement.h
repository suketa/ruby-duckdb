#ifndef RUBY_DUCKDB_PREPARED_STATEMENT_H
#define RUBY_DUCKDB_PREPARED_STATEMENT_H

struct _rubyDuckDBPreparedStatement {
    duckdb_prepared_statement prepared_statement;
    index_t nparams;
};

typedef struct _rubyDuckDBPreparedStatement rubyDuckDBPreparedStatement;

void init_duckdb_prepared_statement(void);

#endif
