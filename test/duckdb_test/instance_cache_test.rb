# frozen_string_literal: true

require 'test_helper'

if defined?(DuckDB::InstanceCache)

module DuckDBTest
  class InstanceCacheTest < Minitest::Test
    def test_s_new
      assert_instance_of DuckDB::InstanceCache, DuckDB::InstanceCache.new
    end

    def test_destroy
      cache = DuckDB::InstanceCache.new
      assert_nil cache.destroy
    end
  end
end

end
