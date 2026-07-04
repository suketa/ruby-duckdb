# ruby-duckdb Glossary

- **Composite Value** — a `DuckDB::Value` whose logical type is nested
  (LIST, ARRAY, STRUCT, MAP). Created from a `DuckDB::LogicalType` plus
  `DuckDB::Value` elements; never auto-converted from plain Ruby objects.
- **Value getter layers** — two ways to read a composite Value:
  *low-level getters* (`list_size`, `list_child`, `struct_child`,
  `map_size`/`map_key`/`map_value`) return `DuckDB::Value` elements;
  *to-Ruby conversion* (internal `rbduckdb_duckdb_value_to_ruby`)
  recursively yields Array/Hash, PG-array/jsonb style.
- **Prepared-statement column metadata** — result-set shape
  (column count/names/types) available on a `DuckDB::PreparedStatement`
  before execution; 0-based column index, mirroring the C API.
- **UNION values** — out of scope: the DuckDB C API offers creation but
  no getters, so ruby-duckdb exposes neither.
