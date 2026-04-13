#ifndef RUBY_DUCKDB_TABLE_DESCRIPTION_H
#define RUBY_DUCKDB_TABLE_DESCRIPTION_H

#ifdef HAVE_DUCKDB_H_GE_V1_5_0

struct _rubyDuckDBTableDescription {
    duckdb_table_description table_description;
};

typedef struct _rubyDuckDBTableDescription rubyDuckDBTableDescription;

void rbduckdb_init_duckdb_table_description(void);

#endif /* HAVE_DUCKDB_H_GE_V1_5_0 */

#endif
