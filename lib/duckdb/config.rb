module DuckDB
  if defined?(DuckDB::Config)
    # The DuckDB::Config encapsulates DuckDB Configuration.
    #
    #   require 'duckdb'
    #   config = DuckDB::Config.new
    #   config['default_order'] = 'DESC'
    #   db = DuckDB::Database.open(nil, config)
    #   con = db.connect
    #   con.query('CREATE TABLE numbers (number INTEGER)')
    #   con.query('INSERT INTO numbers VALUES (2), (1), (4), (3)')
    #
    #   # number is ordered by descending.
    #   r = con.query('SELECT number FROM numbers ORDER BY number)
    #   r.first.first # => 4
    class Config
      class << self
        #
        # returns available configuration name and the description.
        # The return value is array object. The first element is the configuration
        # name. The second is the description.
        #
        #   key, desc = DuckDB::Config.key_description(0)
        #   key # => "access_mode"
        #   desc # => "Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)"
        #
        alias key_description get_config_flag

        #
        # returns the Hash object of all available configuration names and
        # the descriptions.
        #
        # configs = DuckDB::Config.key_descriptions
        # configs['default_order'] # => "The order type used when none is specified ([ASC] or DESC)"
        #
        def key_descriptions
          return @key_descriptions if @key_descriptions

          n = size
          @key_descriptions = (0...n).each_with_object({}) do |i, hash|
            key, description = key_description(i)
            hash[key] = description
          end
        end
      end

      #
      # set configuration value
      #
      #   config = DuckDB::Config.new
      #   # config.set_config('default_order', 'DESC')
      #   config['default_order'] = 'DESC'
      #
      #   db = DuckDB::Database.open(nil, config)
      #   con = db.connect
      #   con.query('CREATE TABLE numbers (number INTEGER)')
      #   con.query('INSERT INTO numbers VALUES (2), (1), (4), (3)')
      #
      #   # numbers are ordered by descending.
      #   r = con.query('SELECT number FROM numbers ORDER BY number)
      #   r.first.first # => 4
      alias []= set_config
    end
  end
end
