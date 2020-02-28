#ifndef RUBY_DUCKDB_DATABASE_H
#define RUBY_DUCKDB_DATABASE_H

struct _rubyDuckDB {
    duckdb_database db;
};

typedef struct _rubyDuckDB rubyDuckDB;

VALUE cDuckDBDatabase;

void init_duckdb_database(void);

#endif
