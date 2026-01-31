# frozen_string_literal: true

source 'https://rubygems.org'

# Specify your gem's dependencies in duckdb.gemspec
gemspec

gem 'bundler', '~> 4.0'
gem 'minitest', '~> 6.0'
gem 'rake', '~> 13.0'
gem 'rake-compiler'

if /(linux|darwin)/ =~ RUBY_PLATFORM
  gem 'benchmark-ips'
  gem 'stackprof'
end

if /linux/ =~ RUBY_PLATFORM
  gem 'ruby_memcheck'
end

gem 'rubocop', require: false
gem 'rubocop-minitest', require: false
gem 'rubocop-rake', require: false
