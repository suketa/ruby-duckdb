require "test_helper"

module DuckDBTest
  class DuckDBTest < Minitest::Test
    def test_that_it_has_a_version_number
      refute_nil ::DuckDB::VERSION
    end

    def test_that_it_has_a_library_version_number
      refute_nil ::DuckDB::LIBRARY_VERSION if DuckDB.methods.include?(:library_version)
    end
  end
end
