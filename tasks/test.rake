require 'rake/testtask'
require 'rake/clean'

rule 'test/CouchbaseMock.jar' do |task|
  download_uri = "http://files.couchbase.com/maven2/org/couchbase/mock/CouchbaseMock/0.5-SNAPSHOT/CouchbaseMock-0.5-20120103.162550-11.jar"
  sh %{wget -q -O test/CouchbaseMock.jar #{download_uri}}
end

CLOBBER << 'test/CouchbaseMock.jar'

module FileUtils
  alias :orig_ruby :ruby

  def ruby(*args, &block)
    executable = [ENV['RUBY_PREFIX'], RUBY].flatten.compact.join(' ')
    options = (Hash === args.last) ? args.pop : {}
    if args.length > 1 then
      sh(*([executable] + args + [options]), &block)
    else
      sh("#{executable} #{args.first}", options, &block)
    end
  end
end

Rake::TestTask.new do |test|
  test.libs << "test" << "."
  test.ruby_opts << "-rruby-debug" if ENV['DEBUG']
  test.pattern = 'test/test_*.rb'
  test.options = '--verbose'
end

Rake::Task['test'].prerequisites.unshift('test/CouchbaseMock.jar')

desc "Run the test suite under Valgrind."
task "test:valgrind" do
  ENV['RUBY_PREFIX'] = [
    "valgrind",
    "--num-callers=50",
    "--error-limit=no",
    "--partial-loads-ok=yes",
    "--undef-value-errors=no",
    "--track-fds=yes",
    "--leak-check=full",
    "--leak-resolution=med",
  ].join(' ')
  Rake::Task['test'].invoke
end

desc "Run the test suite under Valgrind with memory-fill."
task "test:valgrind:mem_fill" do
  ENV['RUBY_PREFIX'] = [
    "valgrind",
    "--num-callers=50",
    "--error-limit=no",
    "--partial-loads-ok=yes",
    "--undef-value-errors=no",
    "--freelist-vol=100000000",
    "--malloc-fill=6D",
    "--free-fill=66",
  ].join(' ')
  Rake::Task['test'].invoke
end

desc "Run the test suite under Valgrind with memory-zero."
task "test:valgrind:mem_zero" do
  ENV['RUBY_PREFIX'] = [
    "valgrind",
    "--num-callers=50",
    "--error-limit=no",
    "--partial-loads-ok=yes",
    "--undef-value-errors=no",
    "--freelist-vol=100000000",
    "--malloc-fill=00",
    "--free-fill=00",
  ].join(' ')
  Rake::Task['test'].invoke
end
