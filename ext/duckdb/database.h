#ifndef RUBY_DUCKDB_DATABASE_H
#define RUBY_DUCKDB_DATABASE_H

struct _rubyDuckDB {
    duckdb_database db;
    /* Functions and logical types registered on any connection to this database */
    VALUE registered_functions;
};

typedef struct _rubyDuckDB rubyDuckDB;

rubyDuckDB *rbduckdb_get_struct_database(VALUE obj);
VALUE rbduckdb_create_database_obj(duckdb_database db);
void rbduckdb_database_retain(VALUE database, VALUE obj);
void rbduckdb_init_database(void);

#endif
