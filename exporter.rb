require 'haml'
require 'erb'
require 'sqlite3'
require_relative 'database'

class Exporter
  def initialize(options = {})
    @docset_name = options[:name]    # TODO raise error if option is missing
    @output = options[:output]
    @batch_size = options[:batch_size] || 2
    @export_comments = options.has_key?(:export_comments) ? options[:export_comments] : true
    @haml = Haml::Engine.new(File.read File.join('template', 'template.html.haml'))
  end
  
  # TODO parallelize
  #   pump it into postgres table first, after finish, copy to sqlite (singlethreaded)
  #   query count, divide by 4, spawn processes
  #   batches = (0..3).map { |i| (i * (count / 4.0).ceil ... [(i+1) * (count / 4.0).ceil, count].min) }
  def export
    FileUtils.remove_entry_secure(output_directory, true)
    FileUtils.mkdir_p documents_directory
    FileUtils.cp File.join('template', 'style.css'), documents_directory
    add_plist
    
    db = Database.new_connection
    db.exec "DROP TABLE IF EXISTS searchIndex"
    db.exec "CREATE TABLE searchIndex(name TEXT, path TEXT)"
    db.close
    
    export_range 1..2
        
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
    db = Database.new_connection
    db.prepare 'insert_statement', "INSERT INTO searchIndex(name, path) VALUES ($1, $2);"
    
    db.transaction do
      db.exec("DECLARE questions CURSOR FOR SELECT * FROM posts WHERE export = true")
      
      fetch_in_batches('questions', @batch_size, db) { |row, db| export_question(row, 'insert_statement', db) }
      
      db.exec("CLOSE questions")
    end
  ensure
    db.close
  end
  
  def docset_name
    @docset_name
  end
  
  def add_plist
    plist = ERB.new(File.read File.join('template', 'info.plist.erb')).result binding
    filename = File.join contents_directory, "info.plist"
    File.open(filename, 'w') { |file| file.write(plist) }
  end
  
  def output_directory
    @output_directory ||= "#{@output}_docset"   # TODO change to .docset
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
    sqlite = SQLite3::Database.new(File.join(resources_directory, 'docSet.dsidx'))
    sqlite.execute "CREATE TABLE searchIndex(id INTEGER PRIMARY KEY, name TEXT, type TEXT, path TEXT)"
    insert = sqlite.prepare "INSERT OR IGNORE INTO searchIndex(name, type, path) VALUES (?, 'Guide', ?);"
    
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
    
    start = Time.new
    puts "Adding index to docset database..."
    sqlite.execute "CREATE UNIQUE INDEX anchor ON searchIndex (name, type, path);"
    puts "Total adding index to docset database: #{Time.now - start}"
    
  ensure
    insert.close
    sqlite.close
  end
end