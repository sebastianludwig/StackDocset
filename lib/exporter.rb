require 'haml'
require 'erb'
require 'sqlite3'
require_relative 'database'
require_relative 'executor'

class Exporter
  def initialize(options = {})
    @docset_name = options[:name]    # TODO raise error if option is missing
    @output = options[:output]
    @mode = options[:mode]
    @batch_size = options[:batch_size] || 2
    @export_comments = options.has_key?(:export_comments) ? options[:export_comments] : true
    @haml = Haml::Engine.new(File.read File.join(__dir__, 'template', 'template.html.haml'))
  end
  
  def export
    FileUtils.remove_entry_secure(output_directory, true)
    FileUtils.mkdir_p documents_directory
    FileUtils.cp File.join(__dir__, 'template', 'style.css'), documents_directory
    add_plist
    
    db = Database.new_connection
    db.exec "DROP TABLE IF EXISTS searchIndex"
    db.exec "CREATE TABLE searchIndex(name TEXT, path TEXT)"
    
    count = db.query("SELECT COUNT(*) FROM posts WHERE export = true").getvalue(0,0).to_i
    
    db.close
    
    batches = (0..3).map { |i| (i * (count / 4.0).ceil .. [(i+1) * (count / 4.0).ceil, count].min) }
    
    tasks = batches.map do |batch|
      Proc.new { export_range(batch) }
    end
    
    executor = Executor.new @mode, tasks
    executor.execute
        
    copy_to_sqlite
    
    # TODO check if the simple form performs just as good
    # res  = conn.exec('select tablename, tableowner from pg_tables')
    # 
    # res.each do |row|
    #   row.each do |column|
    #    puts column
    #   end
    # end
  end
  
  private
  
  def fetch_in_batches(cursor_name, batch_size, db)
    batch = []
    
    begin
      if batch.size == 0
        result = db.exec("FETCH #{batch_size} FROM #{cursor_name}")
        batch = result.collect { |row| row }
      end

      row = batch.shift
      
      yield row, db if row
    end until row.nil?
  end
  
  def export_range(range)
    puts "Exporting range #{range}..."
    
    start = Time.new
    
    db = Database.new_connection
    db.prepare 'insert_statement', "INSERT INTO searchIndex(name, path) VALUES ($1, $2);"
    
    db.transaction do
      db.exec("DECLARE questions CURSOR FOR SELECT * FROM posts WHERE export = true LIMIT #{range.max - range.min} OFFSET #{range.min}")
      
      fetch_in_batches('questions', @batch_size, db) { |row, db| export_question(row, 'insert_statement', db) }
      
      db.exec("CLOSE questions")
    end
    
    puts "Total time exporting range #{range}: #{Time.now - start} s"
    
  ensure
    db.close if db
  end
  
  def docset_name
    @docset_name
  end
  
  def add_plist
    plist = ERB.new(File.read File.join(__dir__, 'template', 'info.plist.erb')).result binding
    filename = File.join contents_directory, "info.plist"
    File.open(filename, 'w') { |file| file.write(plist) }
  end
  
  def output_directory
    @output_directory ||= "#{@output}.docset"
  end
  
  def contents_directory
    @contents_directory ||= File.join output_directory, 'Contents'
  end
  
  def resources_directory
    @resources_directory ||= File.join contents_directory, 'Resources'
  end
  
  def documents_directory
    @documents_directory ||= File.join resources_directory, 'Documents'
  end
  
  def export_question(data, insert_statement_name, db)
    data['answers'] = answers_for_question(data, db)
    data['comments'] = @export_comments ? comments_for_post(data, db) : []
    
    output = @haml.render(Object.new, { :@question => data })
    filename = "#{data['id']}.html"
    path = File.join documents_directory, filename
    File.open(path, 'w') { |file| file.write(output) }
    
    db.exec_prepared insert_statement_name, [CGI.unescape_html(data['title']), filename]
  end
  
  def answers_for_question(question, db)
    result = db.query "SELECT * FROM posts WHERE ParentId = #{question['id']} ORDER BY score DESC NULLS LAST"
    all = result.map { |row| row }
    accepted, other = all.partition { |answer| answer['id'] == question['acceptedanswerid'] }
    accepted.first['accepted'] = true if accepted.size > 0
    other.each { |answer| answer['accepted'] = false }
    all = accepted + other
    all.each { |answer| answer['comments'] = @export_comments ? comments_for_post(answer, db) : [] }
  end
  
  def comments_for_post(post, db)
    result = db.query "SELECT * FROM comments WHERE PostId = #{post['id']} ORDER BY score DESC NULLS LAST, creationdate ASC"
    result.map { |row| row }
  end
  
  def copy_to_sqlite
    sqlite_path = File.join(resources_directory, 'docSet.dsidx')
    FileUtils.remove_entry_secure sqlite_path, true
    sqlite = SQLite3::Database.new(sqlite_path)
    sqlite.execute "CREATE TABLE searchIndex(id INTEGER PRIMARY KEY, name TEXT, type TEXT, path TEXT)"
    insert = sqlite.prepare "INSERT OR IGNORE INTO searchIndex(name, type, path) VALUES (?, 'Guide', ?);"
    
    puts "Copying docset database..."
    
    start = Time.new
    
    db = Database.new_connection
    begin
      db.transaction do
        db.exec("DECLARE export CURSOR FOR SELECT * FROM searchIndex")
      
        fetch_in_batches('export', @batch_size, db) do |row, db| 
          insert.bind_params(row['name'], row['path'])
          insert.execute
          insert.reset!
        end
      
        db.exec("CLOSE export")
      end
    ensure
      db.close
    end
    
    puts "Total time copying docset database: #{Time.now - start} s"
    
    start = Time.new
    puts "Adding index to docset database..."
    sqlite.execute "CREATE UNIQUE INDEX anchor ON searchIndex (name, type, path);"
    puts "Total adding index to docset database: #{Time.now - start}"
    
  ensure
    insert.close if insert
    sqlite.close if sqlite
  end
end