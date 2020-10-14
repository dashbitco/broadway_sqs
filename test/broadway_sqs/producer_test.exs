defmodule BroadwaySQS.BroadwaySQS.ProducerTest do
  use ExUnit.Case

  alias Broadway.Message

  defmodule MessageServer do
    def start_link() do
      Agent.start_link(fn -> [] end)
    end

    def push_messages(server, messages) do
      Agent.update(server, fn queue -> queue ++ messages end)
    end

    def take_messages(server, amount) do
      Agent.get_and_update(server, &Enum.split(&1, amount))
    end
  end

  defmodule FakeSQSClient do
    @behaviour BroadwaySQS.SQSClient
    @behaviour Broadway.Acknowledger

    @impl true
    def init(opts) do
      {:ok, opts}
    end

    @impl true
    def receive_messages(amount, opts) do
      messages = MessageServer.take_messages(opts[:message_server], amount)
      send(opts[:test_pid], {:messages_received, length(messages)})

      for msg <- messages do
        ack_data = %{
          receipt: %{id: "Id_#{msg}", receipt_handle: "ReceiptHandle_#{msg}"},
          test_pid: opts[:test_pid]
        }

        metadata = %{fake: "FAKE"}
        %Message{data: msg, metadata: metadata, acknowledger: {__MODULE__, :ack_ref, ack_data}}
      end
    end

    @impl true
    def ack(_ack_ref, successful, _failed) do
      [%Message{acknowledger: {_, _, %{test_pid: test_pid}}} | _] = successful
      send(test_pid, {:messages_deleted, length(successful)})
    end
  end

  defmodule Forwarder do
    use Broadway

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    def init(opts) do
      {:ok, opts}
    end

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data, message.metadata})
      message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  defp prepare_for_start_module_opts(module_opts) do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    try do
      BroadwaySQS.Producer.prepare_for_start(Forwarder,
        producer: [
          module: {BroadwaySQS.Producer, module_opts},
          concurrency: 1
        ]
      )
    after
      stop_broadway(pid)
    end
  end

  describe "prepare_for_start/2 validation" do
    test "when the queue url is not present" do
      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, required option :queue_url not found, received options: []",
        fn ->
          prepare_for_start_module_opts([])
        end
      )
    end

    test "when the queue url is nil" do
      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :queue_url to be an string, got: nil",
        fn ->
          prepare_for_start_module_opts(queue_url: nil)
        end
      )
    end

    test "when the queue url is an empty string" do
      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :queue_url to be an string, got: \"\"",
        fn ->
          prepare_for_start_module_opts(queue_url: "")
        end
      )
    end

    test "when the queue url is an atom" do
      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :queue_url to be an string, got: :my_queue_url_atom",
        fn ->
          prepare_for_start_module_opts(queue_url: :my_queue_url_atom)
        end
      )
    end

    test "when the queue url is a string" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue"
               )

      assert result_module_opts[:queue_url] == "https://sqs.amazonaws.com/0000000000/my_queue"
    end

    test ":attribute_names is optional without default value" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue"
               )

      refute Keyword.has_key?(result_module_opts, :attribute_names)
    end

    test "when :attribute_names is a list containing any of the supported attributes" do
      all_attribute_names = [
        :sender_id,
        :sent_timestamp,
        :approximate_receive_count,
        :approximate_first_receive_timestamp,
        :sequence_number,
        :message_deduplication_id,
        :message_group_id,
        :aws_trace_header
      ]

      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 attribute_names: all_attribute_names
               )

      assert result_module_opts[:attribute_names] == all_attribute_names

      bad_attribute_names = [:approximate_receive_count, :unsupported]

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :attribute_names to be a list with possible members [:sender_id, :sent_timestamp, :approximate_receive_count, :approximate_first_receive_timestamp, :sequence_number, :message_deduplication_id, :message_group_id, :aws_trace_header], got: [:approximate_receive_count, :unsupported]",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            attribute_names: bad_attribute_names
          )
        end
      )
    end

    test "when :attribute_names is :all" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 attribute_names: :all
               )

      assert result_module_opts[:attribute_names] == :all
    end

    test ":message_attribute_names is optional without default value" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue"
               )

      refute Keyword.has_key?(result_module_opts, :message_attribute_names)
    end

    test "when :message_attribute_names is a list of non empty strings" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 message_attribute_names: ["attr_1", "attr_2"]
               )

      assert result_module_opts[:message_attribute_names] == ["attr_1", "attr_2"]

      bad_message_attribute_names = ["attr_1", :not_a_string]

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :queue_url to be a list with non-empty strings, got: [:not_a_string]",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            message_attribute_names: bad_message_attribute_names
          )
        end
      )
    end

    test "when :message_attribute_names is :all" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 message_attribute_names: :all
               )

      assert result_module_opts[:message_attribute_names] == :all
    end

    test ":wait_time_seconds is optional without default value" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue"
               )

      refute Keyword.has_key?(result_module_opts, :wait_time_seconds)
    end

    test ":wait_time_seconds should be a non negative integer" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 wait_time_seconds: 0
               )

      assert result_module_opts[:wait_time_seconds] == 0

      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 wait_time_seconds: 10
               )

      assert result_module_opts[:wait_time_seconds] == 10

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :wait_time_seconds to be a non negative integer, got: -1",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            wait_time_seconds: -1
          )
        end
      )

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :wait_time_seconds to be a non negative integer, got: :an_atom",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            wait_time_seconds: :an_atom
          )
        end
      )
    end

    test ":max_number_of_messages is optional with default value 10" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue"
               )

      assert result_module_opts[:max_number_of_messages] == 10
    end

    test ":max_number_of_messages should be an integer between 1 and 10" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 max_number_of_messages: 1
               )

      assert result_module_opts[:max_number_of_messages] == 1

      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 max_number_of_messages: 10
               )

      assert result_module_opts[:max_number_of_messages] == 10

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :max_number_of_messages to be an integer between 1 and 10, got: 0",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            max_number_of_messages: 0
          )
        end
      )

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :max_number_of_messages to be an integer between 1 and 10, got: 11",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            max_number_of_messages: 11
          )
        end
      )
    end

    test ":config is optional with default value []" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue"
               )

      assert result_module_opts[:config] == []
    end

    test ":config should be a keyword list" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 config: [scheme: "https://", region: "us-east-1"]
               )

      assert result_module_opts[:config] == [scheme: "https://", region: "us-east-1"]

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :config to be a keyword list, got: :an_atom",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            config: :an_atom
          )
        end
      )
    end

    test ":visibility_timeout is optional without default value" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue"
               )

      refute Keyword.has_key?(result_module_opts, :visibility_timeout)
    end

    test ":visibility_timeout should be a non negative integer" do
      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 visibility_timeout: 0
               )

      assert result_module_opts[:visibility_timeout] == 0

      assert {[BroadwaySQS.BroadwaySQS.ProducerTest.Forwarder],
              [
                producer: [
                  module: {BroadwaySQS.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
                 visibility_timeout: 43200
               )

      assert result_module_opts[:visibility_timeout] == 43200

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :visibility_timeout to be an integer between 0 and 43200, got: -1",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            visibility_timeout: -1
          )
        end
      )

      assert_raise(
        ArgumentError,
        "invalid configuration given to SQSBroadway.prepare_for_start/2, expected :visibility_timeout to be an integer between 0 and 43200, got: 142857",
        fn ->
          prepare_for_start_module_opts(
            queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
            visibility_timeout: 142_857
          )
        end
      )
    end
  end

  test "receive messages when the queue has less than the demand" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    MessageServer.push_messages(message_server, 1..5)

    assert_receive {:messages_received, 5}

    for msg <- 1..5 do
      assert_receive {:message_handled, ^msg, _}
    end

    stop_broadway(pid)
  end

  test "receive messages with the metadata defined by the SQS client" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)
    MessageServer.push_messages(message_server, 1..5)

    assert_receive {:message_handled, _, %{fake: "FAKE"}}

    stop_broadway(pid)
  end

  test "keep receiving messages when the queue has more than the demand" do
    {:ok, message_server} = MessageServer.start_link()
    MessageServer.push_messages(message_server, 1..20)
    {:ok, pid} = start_broadway(message_server)

    assert_receive {:messages_received, 10}

    for msg <- 1..10 do
      assert_receive {:message_handled, ^msg, _}
    end

    assert_receive {:messages_received, 5}

    for msg <- 11..15 do
      assert_receive {:message_handled, ^msg, _}
    end

    assert_receive {:messages_received, 5}

    for msg <- 16..20 do
      assert_receive {:message_handled, ^msg, _}
    end

    assert_receive {:messages_received, 0}

    stop_broadway(pid)
  end

  test "keep trying to receive new messages when the queue is empty" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    MessageServer.push_messages(message_server, [13])
    assert_receive {:messages_received, 1}
    assert_receive {:message_handled, 13, _}

    assert_receive {:messages_received, 0}
    refute_receive {:message_handled, _, _}

    MessageServer.push_messages(message_server, [14, 15])
    assert_receive {:messages_received, 2}
    assert_receive {:message_handled, 14, _}
    assert_receive {:message_handled, 15, _}

    stop_broadway(pid)
  end

  test "stop trying to receive new messages after start draining" do
    {:ok, message_server} = MessageServer.start_link()
    broadway_name = new_unique_name()
    {:ok, pid} = start_broadway(broadway_name, message_server)

    [producer] = Broadway.producer_names(broadway_name)

    assert_receive {:messages_received, 0}

    :sys.suspend(producer)
    flush_messages_received()
    task = Task.async(fn -> Broadway.Topology.ProducerStage.drain(producer) end)
    :sys.resume(producer)
    Task.await(task)

    refute_receive {:messages_received, _}, 10

    stop_broadway(pid)
  end

  test "delete acknowledged messages" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    MessageServer.push_messages(message_server, 1..20)

    assert_receive {:messages_deleted, 10}
    assert_receive {:messages_deleted, 10}

    stop_broadway(pid)
  end

  defp start_broadway(broadway_name, message_server) do
    Broadway.start_link(
      Forwarder,
      build_broadway_opts(broadway_name,
        sqs_client: FakeSQSClient,
        queue_url: "https://sqs.amazonaws.com/0000000000/my_queue",
        receive_interval: 0,
        test_pid: self(),
        message_server: message_server
      )
    )
  end

  defp start_broadway(message_server), do: start_broadway(new_unique_name(), message_server)

  defp build_broadway_opts(broadway_name, producer_opts) do
    [
      name: broadway_name,
      context: %{test_pid: self()},
      producer: [
        module: {BroadwaySQS.Producer, producer_opts},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        default: [
          batch_size: 10,
          batch_timeout: 50,
          concurrency: 1
        ]
      ]
    ]
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  defp flush_messages_received() do
    receive do
      {:messages_received, 0} -> flush_messages_received()
    after
      0 -> :ok
    end
  end
end
