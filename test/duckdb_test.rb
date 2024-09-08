# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class DuckDBTest < Minitest::Test
    def test_that_it_has_a_version_number
      refute_nil ::DuckDB::VERSION
    end

    def test_that_it_has_a_library_version_number
      refute_nil ::DuckDB::LIBRARY_VERSION
    end
  end
end
