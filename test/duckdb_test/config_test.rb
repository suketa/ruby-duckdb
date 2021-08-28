require 'test_helper'

if defined?(DuckDB::Config)
  module DuckDBTest
    class ConfigTest < Minitest::Test
      def test_s_new
        config = DuckDB::Config.new
        assert_instance_of(DuckDB::Config, config)
      end
    end
  end
end

