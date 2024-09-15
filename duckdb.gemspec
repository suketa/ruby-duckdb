# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'duckdb/version'

Gem::Specification.new do |spec|
  spec.name          = 'duckdb'
  spec.version       = DuckDB::VERSION
  spec.authors       = ['Masaki Suketa']
  spec.email         = ['masaki.suketa@nifty.ne.jp']

  spec.summary       = 'This module is Ruby binding for DuckDB database engine.'
  spec.description   = 'This module is Ruby binding for DuckDB database engine. You must have the DuckDB engine installed to build/use this module.'
  spec.homepage      = 'https://github.com/suketa/ruby-duckdb'
  spec.license       = 'MIT'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://github.com/suketa/ruby-duckdb'
  spec.metadata['changelog_uri'] = 'https://github.com/suketa/ruby-duckdb/blob/master/CHANGELOG.md'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.require_paths = ['lib']
  spec.extensions    = ['ext/duckdb/extconf.rb']
  spec.required_ruby_version = '>= 3.1.0'
  spec.add_dependency 'bigdecimal', '>= 3.1.4'

  spec.add_development_dependency 'bundler', '~> 2.3'
  spec.add_development_dependency 'minitest', '~> 5.0'
  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'rake-compiler'
end
