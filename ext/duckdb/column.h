#ifndef RUBY_DUCKDB_COLUMN_H
#define RUBY_DUCKDB_COLUMN_H

struct _rubyDuckDBColumn {
    VALUE result;
    idx_t col;
};

typedef struct _rubyDuckDBColumn rubyDuckDBColumn;

void init_duckdb_column(void);
VALUE create_column(VALUE oDuckDBResult, idx_t col);

#endif
