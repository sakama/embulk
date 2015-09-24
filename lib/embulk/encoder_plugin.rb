module Embulk

  class EncoderPlugin
    def self.transaction(config, &control)
      yield(config)
      return {}
    end

    def initialize(task, file_output)
      @task = task
      @file_output = file_output
      init
    end

    attr_reader :task, :file_output

    def init
    end

    def add(buffer)
      raise NotImplementedError, "EncoderPlugin#add(buffer) must be implemented"
    end

    def finish
    end

    def close
    end

    def next_file
    end

    def self.new_java
      JavaAdapter.new(self)
    end

    class JavaAdapter
      include Java::EncoderPlugin

      def initialize(ruby_class)
        @ruby_class = ruby_class
      end

      def transaction(java_config, java_control)
        config = DataSource.from_java(java_config)
        @ruby_class.transaction(config) do |task_source_hash|
          java_task_source = DataSource.from_ruby_hash(task_source_hash).to_java
          java_control.run(java_task_source)
        end
        nil
      end

      def open(java_task_source, java_file_output)
        task_source = DataSource.from_java(java_task_source)
        file_output = FileOutput.new(java_file_output)
        ruby_object = @ruby_class.new(task_source, file_output)
        return OutputAdapter.new(ruby_object, file_output)
      end

      class OutputAdapter
        def initialize(ruby_object, file_output)
          @ruby_object = ruby_object
          @file_output = file_output
        end

        def add(java_buffer)
          # TODO this code causes TypeError
          @file_output.add(java_buffer)
        end

        def next_file
          @file_output.flush
          @file_output.next_file
          self
        end

        def finish
          @ruby_object.finish
        end

        def close
          @ruby_object.close
        ensure
          @file_output.close
        end
      end
    end

    def self.from_java(java_class)
      JavaPlugin.ruby_adapter_class(java_class, EncoderPlugin, RubyAdapter)
    end

    module RubyAdapter
      module ClassMethods
      end
      # TODO
    end
  end

end
