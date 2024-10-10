source 'https://rubygems.org'

# Specify your gem's dependencies in duckdb.gemspec
gemspec

if /(linux|darwin)/ =~ RUBY_PLATFORM
  gem 'benchmark-ips'
  gem 'stackprof'
end

if /linux/ =~ RUBY_PLATFORM
  gem 'ruby_memcheck'
end
