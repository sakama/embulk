module Embulk

  require 'embulk/data_source'

  class FileInputPlugin
    def self.transaction(config, &control)
      raise NotImplementedError, "FileInputPlugin.transaction(config, &control) must be implemented"
    end

    def self.resume(task, count, &control)
      raise NotImplementedError, "#{self}.resume(task, count, &control) is not implemented. This plugin is not resumable"
    end

    def self.cleanup(task, count, task_reports)
      # do nothing by default
    end

    def initialize(task, index)
      @task = task
      @file_input = file_input
      @index = index
      init
    end

    attr_reader :task, :file_input, :index

    def init
    end

    def close
    end

    def next_file
    end

    def abort
    end

    def commit
      {}
    end

    def self.new_java
      JavaAdapter.new(self)
    end

    class JavaAdapter
      include Java::FileInputPlugin

      def initialize(ruby_class)
        @ruby_class = ruby_class
      end

      def transaction(java_config, task_count, java_control)
        config = DataSource.from_java(java_config)
        config_diff_hash = @ruby_class.transaction(config, task_count) do |task_source_hash|
          java_task_source = DataSource.from_ruby_hash(task_source_hash).to_java
          java_task_reports = java_control.run(java_task_source)
          java_task_reports.map {|java_task_report|
            DataSource.from_java(java_task_report)
          }
        end
        # TODO check return type of #transaction
        return DataSource.from_ruby_hash(config_diff_hash).to_java
      end

      def resume(java_task_source, task_count, java_control)
        task_source = DataSource.from_java(java_task_source)
        config_diff_hash = @ruby_class.resume(task_source, task_count) do |task_source_hash,task_count|
          java_task_source = DataSource.from_ruby_hash(task_source_hash).to_java
          java_task_reports = java_control.run(java_task_source, task_count)
          java_task_reports.map {|java_task_report|
            DataSource.from_java(java_task_report)
          }
        end
        # TODO check return type of #resume
        return DataSource.from_ruby_hash(config_diff_hash).to_java
      end

      def cleanup(java_task_source, task_count, java_task_reports)
        task_source = DataSource.from_java(java_task_source)
        task_reports = java_task_reports.map {|c| DataSource.from_java(c) }
        @ruby_class.cleanup(task_source, task_count, task_reports)
        return nil
      end

      def open(java_task_source, processor_index, java_file_input)
        task_source = DataSource.from_java(java_task_source)
        file_input = FileInput.new(java_file_input)
        ruby_object = @ruby_class.new(task_source, file_input, processor_index)
        return OutputAdapter.new(ruby_object, file_input)
      end

      class OutputAdapter
        include Java::TransactionalFileInput

        def initialize(ruby_object, file_input)
          @ruby_object = ruby_object
          @file_input = file_input
        end

        def next_file
          @ruby_object.next_file
          self
        end

        def close
          @ruby_object.close
        ensure
          @file_input.close
        end

        def abort
          @ruby_object.abort
        end

        def commit
          task_report_hash = @ruby_object.commit
          return DataSource.from_ruby_hash(task_report_hash).to_java
        end
      end
    end

    # TODO to_java

    def self.from_java(java_class)
      JavaPlugin.ruby_adapter_class(java_class, FileInputPlugin, RubyAdapter)
    end

    module RubyAdapter
      module ClassMethods
        def new_java
          Java::FileInputRunner.new(Java.injector.getInstance(java_class))
        end
        # TODO transaction, resume, cleanup
      end

      # TODO run
    end
  end

end
