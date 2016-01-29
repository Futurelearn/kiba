require 'celluloid/current'

module Kiba
  module Runner
    # allow to handle a block form just like a regular transform
    class AliasingProc < Proc
      alias_method :process, :call
    end

    def run(control)
      # TODO: add a dry-run (not instantiating mode) to_instances call
      # that will validate the job definition from a syntax pov before
      # going any further. This could be shared with the parser.
      run_pre_processes(control)
      if control.process_rows_concurrently?
        process_rows_concurrently(
          to_instances(control.sources),
          to_instances(control.transforms, true),
          to_instances(control.destinations)
        )
      else
        process_rows(
          to_instances(control.sources),
          to_instances(control.transforms, true),
          to_instances(control.destinations)
        )
      end

      # TODO: when I add post processes as class, I'll have to add a test to
      # make sure instantiation occurs after the main processing is done (#16)
      run_post_processes(control)
    end

    def run_pre_processes(control)
      to_instances(control.pre_processes, true, false).each(&:call)
    end

    def run_post_processes(control)
      to_instances(control.post_processes, true, false).each(&:call)
    end

    class DestinationWriter
      include Celluloid

      def initialize(destinations)
        @destinations = destinations
      end

      def write(row)
        destinations.each do |destination|
          destination.write(row)
        end
      end

      def done
        destinations.each(&:close)
        self.terminate
      end

      attr_reader :destinations
    end

    class Transformer
      include Celluloid

      def initialize(transforms, destination)
        @transforms = transforms
        @destination = destination
      end

      def transform(row)
        transforms.each do |transform|
          row = transform.process(row)
          break unless row
        end

        if row
          destination.write(row)
        end
      end

      attr_reader :transforms, :destination
    end

    def process_rows_concurrently(sources, transforms, destinations)
      writer = DestinationWriter.new(destinations)
      transformer_pool = Transformer.pool(args: [ transforms, writer ])

      futures = []
      sources.each do |source|
        source.each do |row|
          futures << transformer_pool.future.transform(row)
        end
      end

      futures.map(&:value)
      writer.done
    end

    def process_rows(sources, transforms, destinations)
      sources.each do |source|
        source.each do |row|
          transforms.each do |transform|
            row = transform.process(row)
            break unless row
          end
          next unless row
          destinations.each do |destination|
            destination.write(row)
          end
        end
      end
      destinations.each(&:close)
    end

    # not using keyword args because JRuby defaults to 1.9 syntax currently
    def to_instances(definitions, allow_block = false, allow_class = true)
      definitions.map do |definition|
        to_instance(
          *definition.values_at(:klass, :args, :block),
          allow_block, allow_class
        )
      end
    end

    def to_instance(klass, args, block, allow_block, allow_class)
      if klass
        fail 'Class form is not allowed here' unless allow_class
        klass.new(*args)
      elsif block
        fail 'Block form is not allowed here' unless allow_block
        AliasingProc.new(&block)
      else
        # TODO: support block passing to a class form definition?
        fail 'Class and block form cannot be used together at the moment'
      end
    end
  end
end
