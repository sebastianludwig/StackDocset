class Executor
  def initialize(mode, tasks)
    @mode = mode
    @tasks = tasks
  end
  
  def execute
    case @mode
    when :forked then execute_forked
    when :multithreaded then execute_multithreaded
    when :singlethreaded then execute_singlethreaded
    else
      raise "Unknown mode #{@mode}"
    end
  end
  
  private
  
  def execute_singlethreaded
    @tasks.each { |task| task.call }
  end
  
  def execute_multithreaded
    threads = []

    @tasks.each do |task|
      threads << Thread.new { task.call }
    end

    threads.each { |t| t.join }
  end
  
  def execute_forked
    pids = []
    @tasks.each do |task|
      pids << Process.fork { task.call }
    end

    pids.each { |pid| Process.wait pid }
  end
end