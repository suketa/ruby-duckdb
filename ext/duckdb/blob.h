#ifndef RUBY_DUCKDB_BLOB_H
#define RUBY_DUCKDB_BLOB_H

/*
 * blob is supported by duckdb v0.2.5 or later
 */
#ifdef HAVE_DUCKDB_VALUE_BLOB

void init_duckdb_blob(void);

#endif /* HAVE_DUCKDB_VALUE_BLOB */

#endif

