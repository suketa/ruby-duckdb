# frozen_string_literal: true

require 'test_helper'

if defined?(DuckDB::InstanceCache)

module DuckDBTest
  class InstanceCacheTest < Minitest::Test
    def test_s_new
      assert_instance_of DuckDB::InstanceCache, DuckDB::InstanceCache.new
    end
  end
end

end
