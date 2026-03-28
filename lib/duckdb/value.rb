# frozen_string_literal: true

module DuckDB
  # DuckDB::Value wraps a folded DuckDB value returned by Expression#fold.
  # It is an alias for DuckDB::ValueImpl, the underlying C extension class.
  Value = ValueImpl
end
