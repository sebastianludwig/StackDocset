class Database
  def self.new_connection
    config = YAML::load_file(File.join(__dir__, '..', 'config', 'database.yml'))
    PG.connect(dbname: config['database'], user: config['user'], host: config['host'])
  end
end