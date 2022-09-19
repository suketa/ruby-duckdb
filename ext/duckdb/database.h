#ifndef RUBY_DUCKDB_DATABASE_H
#define RUBY_DUCKDB_DATABASE_H

struct _rubyDuckDB {
    duckdb_database db;
};

typedef struct _rubyDuckDB rubyDuckDB;

rubyDuckDB *get_struct_database(VALUE obj);
void init_duckdb_database(void);

#endif
