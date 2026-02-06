# Copilot Instructions for ruby-duckdb

## Project Overview

This is a Ruby binding for the DuckDB database engine, implemented as a native C extension. The gem wraps DuckDB's C API to provide idiomatic Ruby interfaces for database operations.

## Build, Test, and Lint

### Building
```bash
# Build the C extension
rake compile

# Clean and rebuild
rake clobber compile

# Build with custom DuckDB paths (if needed)
rake build -- --with-duckdb-include=/path/to/headers --with-duckdb-lib=/path/to/lib
```

### Testing
```bash
# Run all tests
rake test

# Run a single test file
ruby -Ilib:test test/duckdb_test/connection_test.rb

# Run with memory leak detection (if ruby_memcheck is installed)
rake test:valgrind
```

### Linting
```bash
# Run RuboCop
rubocop

# Auto-fix issues
rubocop -a
```

## Architecture

### C Extension Layer (`ext/duckdb/`)
- **Entry point**: `duckdb.c` - Defines the `DuckDB` module and initializes all Ruby classes
- **Core components**: Each DuckDB concept has paired `.c` and `.h` files:
  - `database.{c,h}` - Database connection management
  - `connection.{c,h}` - Query execution and connection operations
  - `result.{c,h}` - Query result handling
  - `prepared_statement.{c,h}` - Prepared statement API
  - `appender.{c,h}` - High-performance bulk inserts
  - `config.{c,h}` - DuckDB configuration options
  - `pending_result.{c,h}` - Async query support
  - `scalar_function.{c,h}` - User-defined functions
  - `logical_type.{c,h}` - Type information
  - `column.{c,h}` - Column metadata
- **Support modules**:
  - `converter.{c,h}` and `value_impl.{c,h}` - Convert between Ruby and DuckDB types
  - `error.{c,h}` - Error handling
  - `blob.{c,h}` - Binary data support
  - `instance_cache.{c,h}` - Object lifecycle management
  - `util.{c,h}` - Helper functions
- **Build config**: `extconf.rb` - Checks for DuckDB >= 1.3.0, searches standard locations for headers/libs

### Ruby Layer (`lib/duckdb/`)
- Thin wrappers around C extension classes
- `duckdb.rb` - Main entry point, requires all components
- Ruby files provide:
  - Block-based interfaces (e.g., `Database.open { |db| ... }`)
  - Argument handling (bind parameters)
  - Higher-level conveniences on top of C primitives
- Key classes mirror C extension: `Database`, `Connection`, `Result`, `PreparedStatement`, `Appender`, `Config`, `PendingResult`

### Data Flow
1. Ruby method call → Ruby wrapper (`lib/duckdb/*.rb`)
2. Ruby wrapper → C extension (`ext/duckdb/*.c`)
3. C extension → DuckDB C API (via `libduckdb.so`)
4. Response converted back through `converter.c` / `value_impl.c`

## Key Conventions

### Memory Management
- C objects (Database, Connection, PreparedStatement, Appender, Config) must be explicitly destroyed or use block form
- Block form (e.g., `db.connect { |con| ... }`) ensures automatic cleanup
- Manual cleanup: call `.destroy` method when done
- `instance_cache.c` tracks Ruby object → C pointer mappings to prevent double-free

### Parameter Binding
- Positional: `query('SELECT * FROM users WHERE id = ?', 1)`
- Named: `query('SELECT * FROM users WHERE id = $id', id: 1)`
- `PreparedStatement` uses same binding interface

### Type Conversion
- Ruby → DuckDB handled by `value_impl.c` / `converter.c`
- BLOB columns require `DuckDB::Blob.new(data)` or `string.force_encoding(Encoding::BINARY)`
- Special values: `DuckDB::Infinity::POSITIVE`, `DuckDB::Infinity::NEGATIVE`
- Interval types supported via `DuckDB::Interval`

### Testing
- Framework: Minitest
- Test files in `test/duckdb_test/` named `*_test.rb`
- Test helper: `test/test_helper.rb`
- Tests use in-memory databases: `DuckDB::Database.open` (no args)
- Each test typically creates fresh database/connection to avoid state leakage

### C Extension Development
- All C symbols prefixed with `rbduckdb_` to avoid namespace conflicts
- Header guards use pattern `RUBY_DUCKDB_<NAME>_H`
- Ruby objects created via `rb_define_class_under(mDuckDB, ...)`
- VALUE type used for all Ruby objects
- Use `rb_raise` for errors, wrapped in `error.c` helpers

### Ruby Code Style
- Frozen string literals: `# frozen_string_literal: true` at top of all files
- Line length: 120 characters max
- Target Ruby version: 3.2+
- Documentation: Inline for complex Ruby logic; C functions use comment blocks with `call-seq:`
- RuboCop config excludes C extension, benchmarks, vendor, tmp, pkg

### Version Management
- Gem version: `lib/duckdb/version.rb` (`DuckDB::VERSION`)
- Library version: Dynamically retrieved from DuckDB via `DuckDB.library_version`
- Minimum DuckDB: 1.3.0 (enforced in `extconf.rb`)
- Minimum Ruby: 3.2.0 (in gemspec)

### Performance
- Use `Appender` for bulk inserts (10-50x faster than prepared statements)
- Prepared statements for repeated queries with different parameters
- Async queries via `PendingResult` for long-running operations

## Development Workflow

1. Make changes to C extension (`ext/duckdb/`) and/or Ruby layer (`lib/duckdb/`)
2. Run `rake compile` to rebuild C extension
3. Write tests in `test/duckdb_test/`
4. Run `rake test` to ensure all tests pass
5. Run `rubocop` to check Ruby style (C code excluded)
6. Update `CHANGELOG.md` if adding features or fixing bugs

### Docker Development
```bash
# Build and run container (Ubuntu)
docker compose build ubuntu
docker compose run --rm ubuntu bash

# Custom Ruby/DuckDB versions
docker compose build ubuntu --build-arg RUBY_VERSION=3.1.3 --build-arg DUCKDB_VERSION=1.0.0
```

## Common Tasks

### Adding a New DuckDB API
1. Add C implementation in `ext/duckdb/<feature>.{c,h}`
2. Register with Ruby in `Init_duckdb_native()` (in `duckdb.c`)
3. Add Ruby wrapper in `lib/duckdb/<feature>.rb`
4. Require in `lib/duckdb.rb`
5. Add tests in `test/duckdb_test/<feature>_test.rb`

### Debugging C Extension
```bash
# Build with debug symbols
rake compile

# Run with valgrind (if ruby_memcheck installed)
rake test:valgrind

# Check for memory leaks in specific test
ruby -Ilib:test test/duckdb_test/your_test.rb
```
