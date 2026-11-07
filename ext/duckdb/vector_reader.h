#ifndef RUBY_DUCKDB_VECTOR_READER_H
#define RUBY_DUCKDB_VECTOR_READER_H

VALUE rbduckdb_vector_value_at(duckdb_vector vector, duckdb_logical_type element_type, idx_t index);

#endif
