require 'rake/testtask'
require 'rake/clean'

rule 'test/CouchbaseMock.jar' do |task|
  download_uri = "http://files.couchbase.com/maven2/org/couchbase/mock/CouchbaseMock/0.5-SNAPSHOT/CouchbaseMock-0.5-20120103.162550-11.jar"
  sh %{wget -q -O test/CouchbaseMock.jar #{download_uri}}
end

CLOBBER << 'test/CouchbaseMock.jar'

Rake::TestTask.new do |test|
  test.libs << "test" << "."
  test.ruby_opts << "-rruby-debug" if ENV['DEBUG']
  test.pattern = 'test/test_*.rb'
  test.verbose = true
end

Rake::Task['test'].prerequisites.unshift('test/CouchbaseMock.jar')
