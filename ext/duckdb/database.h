#ifndef RUBY_DUCKDB_DATABASE_H
#define RUBY_DUCKDB_DATABASE_H

struct _rubyDuckDB {
    duckdb_database db;
};

typedef struct _rubyDuckDB rubyDuckDB;

rubyDuckDB *rbduckdb_get_struct_database(VALUE obj);
VALUE rbduckdb_create_database_obj(duckdb_database db);
void rbduckdb_init_duckdb_database(void);

#endif
