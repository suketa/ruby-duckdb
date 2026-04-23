# frozen_string_literal: true

require_relative 'lib/duckdb/version'

Gem::Specification.new do |spec|
  spec.name          = 'duckdb'
  spec.version       = DuckDB::VERSION
  spec.authors       = ['Masaki Suketa']
  spec.email         = ['masaki.suketa@nifty.ne.jp']
  spec.homepage      = 'https://github.com/suketa/ruby-duckdb'
  spec.license       = 'MIT'

  spec.summary = 'Ruby bindings for the DuckDB database engine.'
  spec.description = <<~TEXT
    This gem provides bindings for DuckDB, which is an in-process SQL database optimized for analytical queries on structured data.
    It's lightweight, embeddable, and works directly with files like Parquet and CSV, making it popular for data analysis tasks.
  TEXT

  spec.metadata['rubygems_mfa_required'] = 'true'
  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['changelog_uri'] = "#{spec.homepage}/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      f.match(%r{^(test|spec|features|sample|benchmark)/|^\.(?!rdoc_options)|
                 ^(HACKING|CONTRIBUTION)\.md$|
                 ^(Dockerfile|docker-compose\.yml|getduckdb\.sh|Gemfile(\.lock)?)$}x)
    end
  end

  spec.require_paths = ['lib']
  spec.extensions = ['ext/duckdb/extconf.rb']
  spec.required_ruby_version = '>= 3.2.0'
  spec.add_dependency 'bigdecimal', '>= 3.1.4'
end
