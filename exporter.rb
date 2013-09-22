require 'haml'
require 'erb'
require_relative 'database'

class Exporter
  def initialize(options = {})
    @name = options[:name]
    @batch_size = options[:batch_size] || 2
    @export_comments = options.has_key?(:export_comments) ? options[:export_comments] : true
    @haml = Haml::Engine.new(File.read File.join('template', 'template.html.haml'))
  end
  
  # parallelize:
  #   query count, divide by 4, spawn processes
  #   batches = (0..3).map { |i| (i * (count / 4.0).ceil ... [(i+1) * (count / 4.0).ceil, count].min) }
  def export
    FileUtils.remove_entry_secure(documents_directory, true)
    FileUtils.mkdir_p documents_directory
    FileUtils.cp File.join('template', 'style.css'), File.join(documents_directory, 'style.css')
    add_plist
    
    db = Database.new_connection
    
    db.transaction do
      db.exec("DECLARE questions CURSOR FOR SELECT * FROM posts WHERE export = true")
      batch = []
      
      begin
        if batch.size == 0
          result = db.exec("FETCH #{@batch_size} FROM questions")
          batch = result.collect { |row| row }
        end

        row = batch.shift
            
        export_question(row, db) if row
      end until row.nil?
      
      db.exec("CLOSE questions")
    end
    
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
  
  def docset_name
    'TODO docset name'
  end
  
  def add_plist
    plist = ERB.new(File.read File.join('template', 'info.plist.erb')).result binding
    filename = File.join contents_directory, "info.plist"
    File.open(filename, 'w') { |file| file.write(plist) }
  end
  
  def contents_directory
    @contents_directory ||= "#{@name}/Contents/"
  end
  
  def documents_directory
    @documents_directory ||= File.join contents_directory, "Resources/Documents/"
  end
  
  def export_question(data, db)
    data['answers'] = answers_for_question(data, db)
    data['comments'] = @export_comments ? comments_for_post(data, db) : []
    
    output = @haml.render(Object.new, { :@question => data })
    filename = File.join documents_directory, "#{data['id']}.html"
    File.open(filename, 'w') { |file| file.write(output) }
    
    # add entry to sqlitedb # be aware of multithreading access to sqlite db
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
end