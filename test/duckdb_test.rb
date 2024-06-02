# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class DuckDBTest < Minitest::Test
    def test_that_it_has_a_version_number
      refute_nil ::DuckDB::VERSION
    end

    def test_that_it_has_a_library_version_number
      skip 'library_version is unavailable' unless defined? DuckDB.library_version
      refute_nil ::DuckDB::LIBRARY_VERSION
    end
  end
end
