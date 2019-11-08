#ifndef RUBY_DUCKDB_CONNECTION_H
#define RUBY_DUCKDB_CONNECTION_H

struct _rubyDuckDBConnection {
    duckdb_connection con;
};

typedef struct _rubyDuckDBConnection rubyDuckDBConnection;

VALUE cDuckDBConnection;

void init_duckdb_connection(void);
VALUE create_connection(VALUE oDuckDBDatabase);

#endif
