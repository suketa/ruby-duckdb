module DuckDB
  class Column
    def type
      types = %i[
        invalid
        boolean
        tinyint
        smallint
        integer
        bigint
        utinyint
        usmallint
        uinteger
        ubigint
        float
        double
        timestamp
        date
        time
        interval
        hugeint
        varchar
        blob
        decimal
        timestamp_s
        timestamp_ms
        timestamp_ns
        enum
        list
        struct
        map
        uuid
        json
      ]
      index = _type
      return :unknown if index >= types.size

      types[index]
    end
  end
end
