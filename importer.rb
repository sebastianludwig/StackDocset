require 'pg'
require 'cgi'
require 'yaml'
require_relative 'database'

class Importer
  def initialize(options)
    @files = options[:files]
    @source_directory = options[:source_directory]
    @mode = (options[:mode] || :singlethreaded).to_sym
  end
  
  def import
    Dir.chdir @source_directory do
      prepare_database(@files)

      # set values to nil
      @files.each { |filename, columns| @files[filename] = columns.merge!(columns) { nil } }

      case @mode
      when :forked then process_forked(@files)
      when :multithreaded then process_multithreaded(@files)
      else process_singlethreaded(@files)
      end
    end
  end
  
  private
  
  def create_table(db, name, columns)
    db.exec "DROP TABLE IF EXISTS #{name}"
  
    column_definition = columns.map { |k, v| k + " " + v }.join(', ')
    sql = "CREATE TABLE IF NOT EXISTS #{name} (#{column_definition})"
    db.exec sql
  end

  def table_name(filename)
    File.basename(filename, File.extname(filename)).downcase
  end

  def prepare_database(files)
    db = Database.new_connection
  
    files.each do |filename, columns|
      create_table(db, table_name(filename), columns)
      files[filename] = columns.merge!(columns) { nil }   # set values to nil
    end
  
    db.close
  end

  def process_file(filename, columns)
    key_value_pattern = /([\w]+)="([^"]*)"\s/     # TODO make this a private_constant, once it in a class http://stackoverflow.com/questions/2873903/how-to-i-make-private-class-constants-in-ruby
  
    puts "Processing #{filename}..."
  
    start = Time.now
    time = start
  
    db = Database.new_connection
  
    output_filename = File.basename(filename, File.extname(filename))
  
    File.open(output_filename, 'w') do |output|
      File.foreach(filename).with_index do |line, line_num|
        next unless line.start_with? '  <row'
    
        col_values = columns.dup
    
        line.scan(key_value_pattern) do |name, value|
          raise "Unknown column #{name}" unless col_values.has_key? name
          col_values[name] = '"' + value + '"'
        end
        output << col_values.values.join(',') << "\n"
      end
    end

    puts "Total time processing #{filename}: #{Time.now - start} s"
  
    start = Time.now
    puts "Loading #{output_filename}..."
    columns = columns.keys.join(',')
    sql = "COPY #{table_name(filename)} (#{columns}) FROM '#{File.absolute_path(output_filename)}' WITH csv"
    db.exec sql
    puts "Total time for loading #{output_filename}: #{Time.now - start} s"
  
    db.close
  end

  def process_forked(files)
    pids = []
    files.each do |filename, columns|
      pids << Process.fork { process_file(filename, columns) }
    end

    pids.each { |pid| Process.wait pid }
  end

  def process_multithreaded(files)
    threads = []

    files.each do |filename, columns|
      threads << Thread.new { process_file(filename, columns) }
    end

    threads.each { |t| t.join }  
  end

  def process_singlethreaded(files)
    files.each do |filename, columns|
      process_file(filename, columns)
    end  
  end
end
