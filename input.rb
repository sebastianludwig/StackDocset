# http://data.stackexchange.com/stackoverflow/query/new
# requires ruby >= 1.9

require 'pg'
require 'cgi'
  
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
  db = PG.connect( dbname: 'stackoverflow', user: 'Sebastian', host: 'localhost' )
  
  files.each do |filename, columns|
    create_table(db, table_name(filename), columns)
    files[filename] = columns.merge!(columns) { nil }   # set values to nil
  end
  
  db.close
end

def process_file(filename, columns)
  puts "Processing #{filename}..."
  
  start = Time.now
  time = start
  
  db = PG.connect( dbname: 'stackoverflow', user: 'Sebastian', host: 'localhost' )
  
  output_filename = File.basename(filename, File.extname(filename))
  
  File.open(output_filename, 'w') do |output|
    File.foreach(filename).with_index do |line, line_num|
      next unless line.start_with? '  <row'
    
      col_values = columns.dup
    
      line.scan(KEY_VALUE_PATTERN) do |name, value|
        raise "Unknown column #{name}" unless col_values.has_key? name
        col_values[name] = '"' + value + '"'
      end
      output << col_values.values.join(',') << "\n"
  
      if line_num % TIME_REPORT_INTERVAL == 0
        now = Time.now
        #puts "...time for #{TIME_REPORT_INTERVAL} #{filename} records: #{now - time} s"
        time = now
      end
    end
  end

  puts "Total time processing for #{filename}: #{Time.now - start} s"
  
  start = Time.now
  puts "Loading #{output_filename}..."
  columns = columns.keys.join(',')
  sql = "COPY #{table_name(filename)} (#{columns}) FROM '#{File.absolute_path(output_filename)}' WITH csv"
  db.exec sql
  puts "Total time for loading #{output_filename}: #{Time.now - start} s"
  
  db.close
end

def pro(files)
  pids = []
  files.each do |filename, columns|
    pids << Process.fork { process_file(filename, columns) }
  end

  pids.each { |pid| Process.wait pid }
end

def mt(files)
  threads = []

  files.each do |filename, columns|
    threads << Thread.new { process_file(filename, columns) }
  end

  threads.each { |t| t.join }  
end

def st(files)
  files.each do |filename, columns|
    process_file(filename, columns)
  end  
end

TIME_REPORT_INTERVAL = 1000
KEY_VALUE_PATTERN = /([\w]+)="([^"]*)"\s/

files = { 
  'Posts.xml' => {
      'Id'                    => 'integer',
      'Title'                 => 'varchar(512)',
      'Body'                  => 'text',
      'Tags'                  => 'varchar(512)',
      'Score'                 => 'integer',
      'PostTypeId'            => 'integer',
      'ParentId'              => 'integer',
      'OwnerUserId'           => 'integer',
      'OwnerDisplayName'      => 'varchar(64)',
      'LastEditorUserId'      => 'integer',
      'LastEditorDisplayName' => 'varchar(64)',
      'AcceptedAnswerId'      => 'integer',
      'CreationDate'          => 'timestamp',
      'LastEditDate'          => 'timestamp',
      'LastActivityDate'      => 'timestamp',
      'CommunityOwnedDate'    => 'timestamp',
      'ClosedDate'            => 'timestamp',
      'ViewCount'             => 'integer',
      'AnswerCount'           => 'integer',
      'CommentCount'          => 'integer',
      'FavoriteCount'         => 'integer'
    },
  'Comments.xml' => {
    'Id'                    => 'integer',
    'PostId'                => 'integer',
    'UserId'                => 'integer',
    'Text'                  => 'text',
    'CreationDate'          => 'timestamp',
    'Score'                 => 'integer',
    'UserDisplayName'       => 'varchar(64)'
  },
  'Users.xml' => {
    'Id'                    => 'integer',
    'Reputation'            => 'integer',
    'CreationDate'          => 'timestamp',
    'DisplayName'           => 'varchar(64)',
    'LastAccessDate'        => 'timestamp',
    'WebsiteUrl'            => 'varchar(1024)',
    'ProfileImageUrl'       => 'varchar(1024)',
    'Location'              => 'varchar(128)',
    'AboutMe'               => 'text',
    'Views'                 => 'integer',
    'UpVotes'               => 'integer',
    'DownVotes'             => 'integer',
    'EmailHash'             => 'varchar(64)',
    'Age'                   => 'integer'
  },
  'Votes.xml' => {
    'Id'                    => 'integer',
    'PostId'                => 'integer',
    'UserId'                => 'integer',
    'VoteTypeId'            => 'integer',
    'CreationDate'          => 'timestamp',
    'BountyAmount'          => 'integer'
  }
}

Dir.chdir File.join(File.dirname(__FILE__), ARGV[0])

prepare_database(files)

# set values to nil
files.each { |filename, columns| files[filename] = columns.merge!(columns) { nil } }

# pro(files)
st(files)