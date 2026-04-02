# frozen_string_literal: true

require "delegate"
require "omq"

module OMQ
  # Bridges OMQ sockets into a Ruby Ractor for true parallel processing.
  #
  # Sockets stay in the main Ractor (Async context). Bridge fibers/threads
  # shuttle messages to/from a worker Ractor. Per-connection serialization
  # converts between Ruby objects and ZMQ byte frames transparently:
  # inproc uses Ractor.make_shareable, ipc/tcp use Marshal.
  #
  # @example Simple pipeline
  #   Async do
  #     pull = OMQ::PULL.new
  #     pull.bind("tcp://127.0.0.1:5555")
  #     push = OMQ::PUSH.new
  #     push.connect("tcp://127.0.0.1:5556")
  #
  #     worker = OMQ::Ractor.new(pull, push) do |omq|
  #       pull_p, push_p = omq.sockets
  #       loop do
  #         msg = pull_p.receive
  #         push_p << transform(msg)
  #       end
  #     end
  #
  #     worker.join
  #   end
  #
  class Ractor

    HANDSHAKE_TIMEOUT = 0.1

    # Socket types that use topic/group-based routing.
    # These get topic-aware connection wrappers that preserve
    # the first frame (topic/group) as a plain string for matching.
    TOPIC_SOCKET_TYPES = %i[PUB SUB XPUB XSUB RADIO DISH].freeze


    # -- Connection wrappers -------------------------------------------

    # Mixed into all connection wrappers so is_a? checks against
    # the wrapped class (e.g. DirectPipe) still work.
    #
    module TransparentDelegator
      def is_a?(klass)
        super || __getobj__.is_a?(klass)
      end
      alias_method :kind_of?, :is_a?
    end


    # Shared cache for Marshal.dump so fan-out serializes once.
    # The send pump is single-threaded, so identity check suffices.
    #
    class SerializeCache
      def initialize
        @last  = nil
        @bytes = nil
      end

      def marshal(obj)
        return @bytes if obj.equal?(@last)
        @last  = obj
        @bytes = Marshal.dump(obj).freeze
      end
    end


    # Wraps a tcp/ipc Connection with transparent Marshal serialization.
    # Serializes/deserializes the entire parts array.
    #
    class MarshalConnection < SimpleDelegator
      include TransparentDelegator

      def initialize(conn, cache)
        super(conn)
        @cache = cache
      end

      def send_message(parts)
        super([@cache.marshal(parts)])
      end

      def write_message(parts)
        super([@cache.marshal(parts)])
      end

      def receive_message
        Marshal.load(super.first)
      end
    end


    # Wraps an inproc DirectPipe with Ractor.make_shareable.
    #
    class ShareableConnection < SimpleDelegator
      include TransparentDelegator

      def send_message(obj)
        super(::Ractor.make_shareable(obj))
      end
    end


    # Topic-aware Marshal wrapper for PUB/XPUB/RADIO (send side).
    # Preserves parts[0] (topic/group) as a plain string for
    # subscription matching; serializes parts[1..] (payload).
    #
    class TopicMarshalConnection < SimpleDelegator
      include TransparentDelegator

      def initialize(conn, cache)
        super(conn)
        @cache = cache
      end

      def send_message(parts)
        super([parts[0], @cache.marshal(parts[1..])])
      end

      def write_message(parts)
        super([parts[0], @cache.marshal(parts[1..])])
      end

      def receive_message
        parts = super
        [parts[0], *Marshal.load(parts[1])]
      end
    end


    # Topic-aware shareable wrapper for inproc PUB/SUB.
    #
    class TopicShareableConnection < SimpleDelegator
      include TransparentDelegator

      def send_message(parts)
        super(::Ractor.make_shareable(parts))
      end
    end


    # -- SocketProxy ---------------------------------------------------

    # Raised by SocketProxy#receive after the socket has been closed.
    # The first receive after closure returns nil; subsequent calls raise.
    #
    class SocketClosedError < IOError; end


    class SocketProxy
      def initialize(input_port, output_port, topic_type)
        @in         = input_port
        @out        = output_port
        @topic_type = topic_type
        @closed     = false
      end

      # Receives the next message from this socket.
      # Returns nil once when the socket closes, then raises
      # SocketClosedError on subsequent calls.
      #
      # @return [Object, nil] deserialized message, or nil on close
      #
      def receive
        raise ::Ractor::ClosedError, "not readable" unless @in
        raise SocketClosedError, "socket closed" if @closed
        msg = @in.receive
        if msg.nil?
          @closed = true
          return nil
        end
        @topic_type ? msg.last : msg
      end

      # Receives the next message with its topic (PUB/SUB, RADIO/DISH).
      #
      # @return [Array(String, Object), nil] [topic, payload], or nil on close
      #
      def receive_with_topic
        raise ::Ractor::ClosedError, "not readable" unless @in
        raise SocketClosedError, "socket closed" if @closed
        msg = @in.receive
        if msg.nil?
          @closed = true
          return nil
        end
        [msg.first, msg.last]
      end

      # Sends a message through this socket.
      # For topic-based sockets, wraps as ["", obj] (all subscribers).
      #
      # @param msg [Object] message
      # @return [self]
      #
      def <<(msg)
        raise ::Ractor::ClosedError, "not writable" unless @out
        if @topic_type
          @out.send(["".b.freeze, msg])
        else
          @out.send(msg)
        end
        self
      end

      # Publishes a message with an explicit topic (PUB/SUB, RADIO/DISH).
      #
      # @param msg [Object] payload
      # @param topic [String] topic string for subscription matching
      # @return [self]
      #
      def publish(msg, topic:)
        raise ::Ractor::ClosedError, "not writable" unless @out
        @out.send([topic.b.freeze, msg])
        self
      end

      # Returns the input port for use with Ractor.select.
      #
      # @return [Ractor::Port]
      #
      def to_port
        raise ::Ractor::ClosedError, "not readable" unless @in
        @in
      end
    end


    # -- Context -------------------------------------------------------

    # Frozen, shareable context passed to the worker Ractor.
    # The user calls #sockets to trigger the setup handshake.
    #
    class Context
      def initialize(setup_port, output_ports, socket_configs)
        @setup_port     = setup_port
        @output_ports   = output_ports
        @socket_configs = socket_configs
        ::Ractor.make_shareable(self)
      end

      # Performs the setup handshake and returns SocketProxy objects.
      #
      # @return [Array<SocketProxy>]
      #
      def sockets
        input_ports = @socket_configs.map { |cfg| cfg[:readable] ? ::Ractor::Port.new : nil }

        @setup_port.send(input_ports)

        @socket_configs.each_with_index.map do |cfg, i|
          SocketProxy.new(input_ports[i], @output_ports[i], cfg[:topic_type])
        end
      end
    end


    # -- Constructor ---------------------------------------------------

    # Creates a new OMQ::Ractor that bridges the given sockets into a worker Ractor.
    #
    # @param sockets [Array<Socket>] sockets to bridge
    # @param serialize [Boolean] whether to auto-serialize per connection (default: true)
    # @yield [Context] block executes inside the worker Ractor;
    #   must call omq.sockets immediately
    #
    def initialize(*sockets, serialize: true, &block)
      raise ArgumentError, "no sockets given"  if sockets.empty?
      raise ArgumentError, "no block given"    unless block

      @sockets   = sockets
      @serialize = serialize

      # Categorize sockets
      socket_configs = sockets.map do |s|
        type_sym   = s.class.name.split("::").last.to_sym
        topic_type = TOPIC_SOCKET_TYPES.include?(type_sym)
        { readable: s.is_a?(Readable), writable: s.is_a?(Writable),
          serialize: serialize, topic_type: topic_type }
      end

      # Main Ractor creates output ports (one per writable socket)
      @output_ports = socket_configs.map { |cfg| cfg[:writable] ? ::Ractor::Port.new : nil }
      output_ports  = @output_ports

      # Setup port for the handshake (main-owned, main receives)
      setup_port = ::Ractor::Port.new

      # Build frozen context for the worker
      frozen_configs = ::Ractor.make_shareable(socket_configs)
      frozen_outputs = ::Ractor.make_shareable(output_ports)
      ctx = Context.new(setup_port, frozen_outputs, frozen_configs)

      # Install connection wrappers for per-connection serialization
      install_connection_wrappers(socket_configs) if serialize

      # Start the worker Ractor
      @ractor = ::Ractor.new(ctx, &block)

      # Wait for the handshake with timeout
      @input_ports = await_handshake(setup_port)
      input_ports  = @input_ports

      # Start bridges on the correct task.
      # Inside Async: spawn under current task.
      # Outside Async: dispatch to the IO thread via Reactor.run.
      @input_tasks    = []
      @output_threads = []
      @output_pipes   = []
      if Async::Task.current?
        @parent_task = Async::Task.current
        start_input_bridges(input_ports, socket_configs)
        start_output_bridges(output_ports, socket_configs)
      else
        @parent_task = Reactor.root_task
        Reactor.run do
          start_input_bridges(input_ports, socket_configs)
          start_output_bridges(output_ports, socket_configs)
        end
      end
    end


    # Waits for the worker Ractor to finish naturally.
    # The worker must return from its block on its own.
    #
    def join
      await_ractor { @ractor.join }
    ensure
      cleanup_bridges
    end


    # Returns the worker Ractor's return value.
    # The worker must return from its block on its own.
    #
    def value
      await_ractor { @ractor.value }
    ensure
      cleanup_bridges
    end


    # Signals the worker to stop, then waits for it to finish.
    # Sends nil through all input ports, causing proxy.receive
    # to return nil (first time) or raise SocketClosedError.
    #
    def close
      @input_ports.each { |p| p&.send(nil) rescue nil }
      await_ractor { @ractor.join } rescue nil
      cleanup_bridges
    end


    private


    # Waits for the worker to call omq.sockets (handshake).
    # Times out after HANDSHAKE_TIMEOUT seconds.
    #
    def await_handshake(setup_port)
      rd, wr = IO.pipe
      input_ports = nil
      Thread.new do
        input_ports = setup_port.receive
      ensure
        wr.close
      end
      unless rd.wait_readable(HANDSHAKE_TIMEOUT)
        rd.close
        @ractor.close rescue nil
        raise ArgumentError, "worker Ractor must call omq.sockets before doing anything else"
      end
      rd.close
      input_ports
    end


    # Runs a block in a Thread, returning the result via an
    # IO.pipe so the Async reactor stays responsive.
    #
    def await_ractor
      rd, wr = IO.pipe
      result = nil
      error  = nil
      Thread.new do
        result = yield
      rescue => e
        error = e
      ensure
        wr.close
      end
      rd.wait_readable
      rd.close
      raise error if error
      result
    end


    def install_connection_wrappers(socket_configs)
      @sockets.each_with_index do |socket, i|
        cache      = SerializeCache.new
        topic_type = socket_configs[i][:topic_type]
        engine     = socket.instance_variable_get(:@engine)

        engine.connection_wrapper = ->(conn) do
          inproc = conn.is_a?(Transport::Inproc::DirectPipe)
          if topic_type
            inproc ? TopicShareableConnection.new(conn) : TopicMarshalConnection.new(conn, cache)
          else
            inproc ? ShareableConnection.new(conn) : MarshalConnection.new(conn, cache)
          end
        end
      end
    end


    # Input bridges: socket -> Ractor (Async fibers).
    #
    def start_input_bridges(input_ports, socket_configs)
      @sockets.each_with_index do |socket, i|
        port = input_ports[i]
        next unless port

        do_serialize = socket_configs[i][:serialize]
        topic_type   = socket_configs[i][:topic_type]

        @input_tasks << @parent_task.async(transient: true, annotation: "ractor input bridge") do
          loop do
            msg = socket.receive
            if do_serialize && !topic_type
              msg = msg.first
            end
            port.send(msg)
          rescue IOError, Async::Stop
            port.send(nil) rescue nil
            break
          rescue ::Ractor::ClosedError
            break
          end
        end
      end
    end


    # Output bridges: Ractor -> socket.
    #
    # A Thread reads from the Ractor port (blocking, can't run in Async)
    # and pushes messages to a Thread::Queue. An Async task waits on an
    # IO.pipe signal and drains the queue directly into the engine --
    # avoiding the Reactor.run synchronization round-trip per message.
    #
    def start_output_bridges(output_ports, socket_configs)
      @sockets.each_with_index do |socket, i|
        port = output_ports[i]
        next unless port

        do_serialize = socket_configs[i][:serialize]
        topic_type   = socket_configs[i][:topic_type]
        engine       = socket.instance_variable_get(:@engine)
        queue        = Thread::Queue.new
        rd, wr       = IO.pipe
        @output_pipes << rd << wr

        # Thread: port.receive -> queue + pipe signal
        @output_threads << Thread.new do
          loop do
            msg = port.receive
            break if msg.equal?(SHUTDOWN)
            queue << msg
            wr.write_nonblock("x") rescue nil
          rescue ::Ractor::ClosedError
            break
          end
          wr.close rescue nil
        end

        # Async task: wait on pipe, drain queue, enqueue to engine
        @input_tasks << @parent_task.async(transient: true, annotation: "ractor output bridge") do
          loop do
            rd.wait_readable
            rd.read_nonblock(4096) rescue nil

            while (msg = queue.pop(true) rescue nil)
              if do_serialize
                parts = topic_type && msg.is_a?(Array) ? msg : [msg]
                engine.enqueue_send(parts)
              else
                parts = socket.__send__(:freeze_message, msg)
                engine.enqueue_send(parts)
              end
            end
          rescue IOError, Async::Stop
            break
          end
        end
      end
    end


    SHUTDOWN = :__omq_ractor_shutdown__

    def cleanup_bridges
      @input_tasks.each { |t| t.stop rescue nil }
      # Unblock output bridge Threads waiting on port.receive
      # (port.close does NOT unblock a waiting receive)
      @output_ports.each { |p| p&.send(SHUTDOWN) rescue nil }
      @output_pipes.each { |io| io.close rescue nil }
      @output_threads.each { |t| t.join(1) }
    end
  end
end
