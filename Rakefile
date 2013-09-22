# http://data.stackexchange.com/stackoverflow/query/new
# requires ruby >= 2.0

require 'rubygems'
require 'bundler/setup'

require_relative 'importer'
require_relative 'exporter'

desc 'Imports XML files into the database'
task :import do
  puts "Importing"
  puts "------------"
  files = YAML::load_file(File.join(__dir__, 'files.yml'))
  source_directory = ENV['source'] || File.join(__dir__, 'data', 'stackoverflow.com')
  mode = ENV['mode'] || :forked
  importer = Importer.new files: files, source_directory: source_directory, mode: mode
  importer.import
  puts "\n"
end

desc 'Add indices to database columns where needed'
task :index do
  puts "Indexing"
  puts "------------"
  db = Database.new_connection
  indexes = {
    'posts' => ['Id', 'ParentId', 'AcceptedAnswerId', 'export'],
    'comments' => ['Id', 'PostId']
  }
  indexes.each do |table, columns|
    columns.each do |column|
      start = Time.now
      puts "Adding index on #{table}.#{column}..."
      db.exec "CREATE INDEX ON #{table} (#{column})"
      puts "Total time indexing #{table}.#{column}: #{Time.now - start} s"
    end
  end
  puts "\n"
end

desc 'Marks which questions are to be exported'
task :mark_for_export do
  puts "Marking"
  puts "------------"
  db = Database.new_connection
  start = Time.now
  puts "Resetting export flag..."
  db.exec 'UPDATE posts SET export = false'
  puts "Total time resetting export flag: #{Time.now - start}"
  
  tag = ENV['tag'] || 'ios'
  start = Time.now
  puts "Marking questions with tag #{tag}..."
  db.exec "UPDATE posts SET export = true WHERE ParentId IS NULL AND AcceptedAnswerId IS NOT NULL AND Tags LIKE '%#{tag}%'"
  puts "Total time marking #{tag} tagged questions: #{Time.now - start}"
  puts "\n"
end

task :export do
  exporter = Exporter.new name: 'bam'
  exporter.export
end

task :default => [:import, :index, :mark_for_export, :export]