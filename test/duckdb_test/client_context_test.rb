# frozen_string_literal: true

require 'test_helper'
require 'securerandom'

module DuckDBTest
  class ClientContextTest < Minitest::Test
    def setup
      @db = DuckDB::Database.open
      @conn = @db.connect
    end

    def teardown
      @conn.disconnect
      @db.close
    end

    # --- connection_id: wraps duckdb_client_context_get_connection_id ---

    # connection_id returns an Integer
    def test_connection_id_returns_integer
      client_context = get_client_context(@conn)

      assert_kind_of Integer, client_context.connection_id
    end

    # connection_id is positive
    def test_connection_id_is_positive
      client_context = get_client_context(@conn)

      assert_operator client_context.connection_id, :>, 0
    end

    # connection_id is consistent for the same connection
    def test_connection_id_is_consistent
      ctx1 = get_client_context(@conn)
      ctx2 = get_client_context(@conn)

      assert_equal ctx1.connection_id, ctx2.connection_id
    end

    # connection_id differs between two distinct connections
    def test_connection_id_differs_across_connections
      conn2 = @db.connect
      ctx1 = get_client_context(@conn)
      ctx2 = get_client_context(conn2)
      conn2.disconnect

      refute_equal ctx1.connection_id, ctx2.connection_id
    end

    private

    def get_client_context(conn)
      result = nil
      name = "test_get_ctx_#{conn.object_id}_#{SecureRandom.hex(4)}"
      sf = DuckDB::ScalarFunction.new
      sf.name = name
      sf.return_type = :integer
      sf.set_bind { |bind_info| result = bind_info.client_context }
      sf.set_function { 1 }
      conn.register_scalar_function(sf)
      conn.execute("SELECT #{name}()")
      result
    end
  end
end
