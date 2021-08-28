require 'test_helper'

if defined?(DuckDB::Config)
  module DuckDBTest
    class ConfigTest < Minitest::Test
      def test_s_new
        config = DuckDB::Config.new
        assert_instance_of(DuckDB::Config, config)
      end

      def test_s_size
        assert_operator(0, :<=, DuckDB::Config.size)
      end

      def test_s_get_config_flag
        key, value = DuckDB::Config.get_config_flag(0)
        assert_equal('access_mode', key)
        assert_match(/\AAccess mode of the database/, value)

        assert_raises(DuckDB::Error) do
          DuckDB::Config.get_config_flag(DuckDB::Config.size)
        end
      end
    end
  end
end
