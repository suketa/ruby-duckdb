module DuckDB
  if defined?(DuckDB::Config)
    class Config
      class << self
        alias key_description get_config_flag

        def key_descriptions
          return @key_descriptions if @key_descriptions

          n = size
          @key_descriptions = (0...n).each_with_object({}) do |i, hash|
            key, description = key_description(i)
            hash[key] = description
          end
        end
      end
    end
  end
end
