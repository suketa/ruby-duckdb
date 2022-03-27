#ifndef RUBY_DUCKDB_APPENDER_H
#define RUBY_DUCKDB_APPENDER_H

struct _rubyDuckDBAppender {
    duckdb_appender appender;
};

typedef struct _rubyDuckDBAppender rubyDuckDBAppender;

void init_duckdb_appender(void);

#endif
