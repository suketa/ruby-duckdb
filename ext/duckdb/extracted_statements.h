#ifndef RUBY_DUCKDB_EXTRACTED_STATEMENTS_H
#define RUBY_DUCKDB_EXTRACTED_STATEMENTS_H

struct _rubyDuckDBExtractedStatements {
    duckdb_extracted_statements extracted_statements;
};

typedef struct _rubyDuckDBExtractedStatements rubyDuckDBExtractedStatements;

void rbduckdb_init_duckdb_extracted_statements(void);
#endif
