# frozen_string_literal: true

require 'test_helper'

module DuckDBTest
  class BlobTest < Minitest::Test
    def test_initialize
      assert_instance_of(DuckDB::Blob, DuckDB::Blob.new('str'))
    end

    def test_superclass
      assert_equal(String, DuckDB::Blob.superclass)
    end
  end
end
