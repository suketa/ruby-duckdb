require 'bundler/gem_tasks'
require 'rake/testtask'

Rake::TestTask.new(:test) do |t|
  t.libs << 'test'
  t.libs << 'lib'
  t.test_files = FileList['test/**/*_test.rb']
end

require 'rake/extensiontask'

task build: :compile

Rake::ExtensionTask.new('duckdb_native') do |ext|
  ext.ext_dir = 'ext/duckdb'
  ext.lib_dir = 'lib/duckdb'
end

task default: %i[clobber compile test]
