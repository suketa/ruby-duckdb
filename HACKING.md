# Hacking on ruby-duckdb

This document describes internal conventions for contributors working on the
C extension in `ext/duckdb/`.

---

## C Function Naming Rules

### 1. `rb_define_method` — static function name: `classname_methodname`

The C function must be named `<classname>_<methodname>`, where `classname` is
the lowercase file name (without `.c`) and `methodname` is the Ruby method name.

```c
rb_define_method(cDuckDBDatabase, "close", database_close, 0);             // OK
rb_define_method(cDuckDBDatabase, "close", duckdb_database_close, 0);      // NG — looks like DuckDB C API
rb_define_method(cDuckDBDatabase, "close", rbduckdb_database_close, 0);    // NG — rbduckdb_ is for externs only
```

### 2. `rb_define_private_method` — static function name: `classname__methodname`

The Ruby method name **must start with `_`**. The C function uses a double
underscore naturally: `classname` + `_` separator + `_methodname`.

```c
rb_define_private_method(cDuckDBDatabase, "_connect", database__connect, 0);    // OK
rb_define_private_method(cDuckDBDatabase, "_connect", database_connect, 0);     // NG — single underscore
rb_define_private_method(cDuckDBDatabase, "_connect", rbduckdb_database_connect, 0); // NG
rb_define_private_method(cDuckDBDatabase, "connect",  database_connect, 0);     // NG — Ruby name must start with '_'
```

Note: private `_initialize` methods produce C function names like
`foo__initialize`. The double underscore is intentional and consistent with
this rule.

```c
rb_define_private_method(cDuckDBFoo, "_initialize", foo__initialize, 2);  // OK — double underscore is intentional
```

### 3. `rb_define_alloc_func` — static function name: `allocate`

```c
rb_define_alloc_func(cDuckDBDatabase, allocate);           // OK
rb_define_alloc_func(cDuckDBDatabase, database_allocate);  // NG
```

### 4. Memory release — static function name: `deallocate`

```c
static void deallocate(void *ctx) { ... }    // OK
static void database_free(void *ctx) { ... } // NG
```

### 5. Memory size — static function name: `memsize`

```c
static size_t memsize(const void *p) { ... }          // OK
static size_t database_memsize(const void *p) { ... } // NG
```

### 6. Other static functions

No fixed naming rule, but the name must be meaningful and must not clash with
any DuckDB C API symbol. A `verb_noun` or `classname_verb_noun` pattern is
recommended for non-registered internal helpers.

### 7. Method name character transformations

Special Ruby method name characters map to C identifiers as follows:

| Ruby method  | C function suffix | Example                        |
|---|---|---|
| `finished?`  | `finished_p`      | `pending_result_finished_p`    |
| `name=`      | `set_name`        | `scalar_function_set_name`     |
| `method!`    | `method_bang`     | *(if needed)*                  |

Using `set_` prefix for `=` setters avoids a naming conflict with a getter of
the same base name (e.g., `name` getter → `foo_name`, `name=` setter →
`foo_set_name`).

### 8. `rb_define_singleton_method` — static function name: `classname_s_methodname`

The `s_` prefix distinguishes singleton (class) methods from instance methods.

```c
rb_define_singleton_method(cDuckDBDatabase, "open", database_s_open, 1);  // OK
```

### 9. Public Ruby methods must not start with `_`

Public Ruby methods registered via `rb_define_method` must not have a `_` prefix
in their Ruby name. The `_` prefix is reserved for the private-via-wrapper
pattern. If a method is private, use `rb_define_private_method` with a
`_`-prefixed name.

```c
rb_define_method(cDuckDBFoo, "_internal_type", foo__internal_type, 0);          // NG — public method with _ prefix
rb_define_private_method(cDuckDBFoo, "_internal_type", foo__internal_type, 0);  // OK
```

### 10. `rbduckdb_init_*` function naming — no redundant `duckdb_`

Class-init extern functions must not include a redundant `duckdb_` segment.

```c
void rbduckdb_init_database(void);         // OK
void rbduckdb_init_duckdb_database(void);  // NG — redundant duckdb_
```

### 11. Extern functions — prefix: `rbduckdb_`

All functions with external linkage (declared in `.h` files, called from other
`.c` files) must start with `rbduckdb_`.

```c
rubyDuckDB *rbduckdb_get_struct_database(VALUE obj);   // OK
rubyDuckDB *duckdb_get_struct_database(VALUE obj);     // NG — looks like DuckDB C API
rubyDuckDB *get_struct_database(VALUE obj);            // NG — no namespace
```
