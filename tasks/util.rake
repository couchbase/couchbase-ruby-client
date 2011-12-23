desc 'Start an irb session and load the library.'
task :console => :compile do
  exec "irb -I lib -rruby-debug -rcouchbase"
end
