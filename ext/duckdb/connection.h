#ifndef RUBY_DUCKDB_CONNECTION_H
#define RUBY_DUCKDB_CONNECTION_H

struct _rubyDuckDBConnection {
    duckdb_connection con;
    VALUE registered_functions;
};

typedef struct _rubyDuckDBConnection rubyDuckDBConnection;

rubyDuckDBConnection *get_struct_connection(VALUE obj);
void rbduckdb_init_duckdb_connection(void);
VALUE rbduckdb_create_connection(VALUE oDuckDBDatabase);

#endif
