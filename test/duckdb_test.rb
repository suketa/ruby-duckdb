require "test_helper"

module DuckDBTest
  class DuckdbTest < Minitest::Test
    def test_that_it_has_a_version_number
      refute_nil ::Duckdb::VERSION
    end
  end
end
