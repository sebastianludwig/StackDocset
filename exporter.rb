class Exporter
  def initialize
  end
  
  def export
    db = Database.new_connection
    
    db.transaction do
      db.exec( "DECLARE questions CURSOR FOR SELECT * FROM posts WHERE export = true" )
      result = db.exec( "FETCH ALL IN questions" )

      result.values.collect do |row|
        puts row.collect { |col| "%-15s" % [col] }.join('')
      end
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
  
end