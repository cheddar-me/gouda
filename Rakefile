# frozen_string_literal: true

# require "bundler/setup"

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"

  # root_dir = File.join(File.dirname(__FILE__), '../..')

  ARGV.each { |a| task a.to_sym }

  file_name = ARGV[1]

  if file_name
    t.pattern = file_name
  else
    t.test_files = FileList["test/**/*_test.rb"]
  end

  # t.test_files = FileList["test/**/*_test.rb"]
  # t.verbose = false
end

task default: :test
