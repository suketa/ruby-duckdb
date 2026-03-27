#ifndef RUBY_DUCKDB_EXPRESSION_H
#define RUBY_DUCKDB_EXPRESSION_H

struct _rubyDuckDBExpression {
    duckdb_expression expression;
};

typedef struct _rubyDuckDBExpression rubyDuckDBExpression;

void rbduckdb_init_duckdb_expression(void);
VALUE rbduckdb_expression_new(duckdb_expression expression);

#endif
