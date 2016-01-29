module Kiba
  class Control
    def initialize
      @use_concurrent_row_transform = false
    end

    def use_concurrent_row_transform
      @use_concurrent_row_transform = true
    end

    def process_rows_concurrently?
      @use_concurrent_row_transform
    end

    def pre_processes
      @pre_processes ||= []
    end

    def sources
      @sources ||= []
    end

    def transforms
      @transforms ||= []
    end

    def destinations
      @destinations ||= []
    end

    def post_processes
      @post_processes ||= []
    end
  end
end
