require 'test_helper'

module DuckDBTest
  class ConnectionTest < Minitest::Test
    def test_class_exist
      assert_instance_of(Class, DuckDB::PreparedStatement)
    end
  end
end
