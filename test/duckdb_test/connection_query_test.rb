# frozen_string_literal: true

module DuckDBTest
  class ConnectionQueryTest < Minitest::Test
    def teardown
      FileUtils.rm_f(@file)
    end

    def test_prepared_statement_destroy_in_query
      outputs = `ruby -Ilib test/ng/connection_query_ng.rb`
      @file, before, after = outputs.split("\n")

      assert_equal(before, after)
    end
  end
end
