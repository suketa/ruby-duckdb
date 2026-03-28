#ifndef RUBY_DUCKDB_CLIENT_CONTEXT_H
#define RUBY_DUCKDB_CLIENT_CONTEXT_H

struct _rubyDuckDBClientContext {
    duckdb_client_context client_context;
};

typedef struct _rubyDuckDBClientContext rubyDuckDBClientContext;

void rbduckdb_init_duckdb_client_context(void);
VALUE rbduckdb_client_context_new(duckdb_client_context client_context);

#endif
