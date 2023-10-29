#ifndef RUBY_DUCKDB_PREPARED_STATEMENT_H
#define RUBY_DUCKDB_PREPARED_STATEMENT_H

struct _rubyDuckDBPreparedStatement {
    duckdb_prepared_statement prepared_statement;
    idx_t nparams;
};

typedef struct _rubyDuckDBPreparedStatement rubyDuckDBPreparedStatement;

rubyDuckDBPreparedStatement *get_struct_prepared_statement(VALUE self);
void rbduckdb_init_duckdb_prepared_statement(void);

#endif
