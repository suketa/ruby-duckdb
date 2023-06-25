require 'bundler/gem_tasks'
ruby_memcheck_avaiable = begin
                           require 'ruby_memcheck'
                         rescue LoadError
                           false
                         end

require 'rake/testtask'

if ruby_memcheck_avaiable
  RubyMemcheck.config(
    binary_name: 'duckdb/duckdb_native',
    valgrind_options: ['--max-threads=1000']
  )
end

test_config = lambda do |t|
  t.libs << 'test'
  t.libs << 'lib'
  t.test_files = FileList['test/**/*_test.rb']
end

Rake::TestTask.new(test: :compile, &test_config)

if ruby_memcheck_avaiable
  namespace :test do
    RubyMemcheck::TestTask.new(valgrind: :compile, &test_config)
  end
end

require 'rake/extensiontask'

task build: :compile

Rake::ExtensionTask.new('duckdb_native') do |ext|
  ext.ext_dir = 'ext/duckdb'
  ext.lib_dir = 'lib/duckdb'
end

task default: %i[clobber compile test]
