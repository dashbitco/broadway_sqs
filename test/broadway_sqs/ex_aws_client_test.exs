defmodule BroadwaySQS.ExAwsClientTest do
  use ExUnit.Case

  alias BroadwaySQS.ExAwsClient
  alias Broadway.Message

  defmodule FakeHttpClient do
    @behaviour ExAws.Request.HttpClient

    def request(:post, url, "Action=ReceiveMessage" <> _ = body, _, _) do
      send(self(), {:http_request_called, %{url: url, body: body}})

      response_body = """
      <ReceiveMessageResponse>
        <ReceiveMessageResult>
          <Message>
            <MessageId>Id_1</MessageId>
            <ReceiptHandle>ReceiptHandle_1</ReceiptHandle>
            <Body>Message 1</Body>
          </Message>
          <Message>
            <MessageId>Id_2</MessageId>
            <ReceiptHandle>ReceiptHandle_2</ReceiptHandle>
            <Body>Message 2</Body>
          </Message>
        </ReceiveMessageResult>
      </ReceiveMessageResponse>
      """

      {:ok, %{status_code: 200, body: response_body}}
    end

    def request(:post, url, "Action=DeleteMessageBatch" <> _ = body, _, _) do
      send(self(), {:http_request_called, %{url: url, body: body}})

      {:ok, %{status_code: 200, body: "<DeleteMessageBatchResponse />"}}
    end
  end

  describe "validate init options" do
    test ":queue_name is required" do
      assert ExAwsClient.init([]) ==
               {:error, "expected :queue_name to be a non empty string, got: nil"}

      assert ExAwsClient.init(queue_name: nil) ==
               {:error, "expected :queue_name to be a non empty string, got: nil"}
    end

    test ":queue_name should be a non empty string" do
      assert ExAwsClient.init(queue_name: "") ==
               {:error, "expected :queue_name to be a non empty string, got: \"\""}

      assert ExAwsClient.init(queue_name: :an_atom) ==
               {:error, "expected :queue_name to be a non empty string, got: :an_atom"}

      {:ok, %{queue_name: queue_name}} = ExAwsClient.init(queue_name: "my_queue")
      assert queue_name == "my_queue"
    end

    test ":wait_time_seconds is optional without default value" do
      {:ok, result} = ExAwsClient.init(queue_name: "my_queue")

      refute Keyword.has_key?(result.receive_messages_opts, :wait_time_seconds)
    end

    test ":wait_time_seconds should be a non negative integer" do
      opts = [queue_name: "my_queue"]

      {:ok, result} = opts |> Keyword.put(:wait_time_seconds, 0) |> ExAwsClient.init()
      assert result.receive_messages_opts[:wait_time_seconds] == 0

      {:ok, result} = opts |> Keyword.put(:wait_time_seconds, 10) |> ExAwsClient.init()
      assert result.receive_messages_opts[:wait_time_seconds] == 10

      {:error, message} = opts |> Keyword.put(:wait_time_seconds, -1) |> ExAwsClient.init()
      assert message == "expected :wait_time_seconds to be a non negative integer, got: -1"

      {:error, message} = opts |> Keyword.put(:wait_time_seconds, :an_atom) |> ExAwsClient.init()
      assert message == "expected :wait_time_seconds to be a non negative integer, got: :an_atom"
    end

    test ":max_number_of_messages is optional with default value 10" do
      {:ok, result} = ExAwsClient.init(queue_name: "my_queue")

      assert result.receive_messages_opts[:max_number_of_messages] == 10
    end

    test ":max_number_of_messages should be an integer between 1 and 10" do
      opts = [queue_name: "my_queue"]

      {:ok, result} = opts |> Keyword.put(:max_number_of_messages, 1) |> ExAwsClient.init()
      assert result.receive_messages_opts[:max_number_of_messages] == 1

      {:ok, result} = opts |> Keyword.put(:max_number_of_messages, 10) |> ExAwsClient.init()
      assert result.receive_messages_opts[:max_number_of_messages] == 10

      {:error, message} = opts |> Keyword.put(:max_number_of_messages, 0) |> ExAwsClient.init()

      assert message ==
               "expected :max_number_of_messages to be a integer between 1 and 10, got: 0"

      {:error, message} = opts |> Keyword.put(:max_number_of_messages, 11) |> ExAwsClient.init()

      assert message ==
               "expected :max_number_of_messages to be a integer between 1 and 10, got: 11"

      {:error, message} =
        opts |> Keyword.put(:max_number_of_messages, :an_atom) |> ExAwsClient.init()

      assert message ==
               "expected :max_number_of_messages to be a integer between 1 and 10, got: :an_atom"
    end

    test ":config is optional with default value []" do
      {:ok, result} = ExAwsClient.init(queue_name: "my_queue")
      assert result.config == []
    end

    test ":config should be a keyword list" do
      opts = [queue_name: "my_queue"]

      config = [scheme: "https://", region: "us-east-1"]
      {:ok, result} = opts |> Keyword.put(:config, config) |> ExAwsClient.init()
      assert result.config == [scheme: "https://", region: "us-east-1"]

      config = :an_atom
      {:error, message} = opts |> Keyword.put(:config, config) |> ExAwsClient.init()
      assert message == "expected :config to be a keyword list, got: :an_atom"
    end
  end

  describe "receive_messages/2" do
    setup do
      %{
        opts: [
          queue_name: "my_queue",
          config: [
            http_client: FakeHttpClient,
            access_key_id: "FAKE_ID",
            secret_access_key: "FAKE_KEY"
          ]
        ]
      }
    end

    test "returns a list of Broadway.Message with :data and :acknowledger set", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      [message1, message2] = ExAwsClient.receive_messages(10, opts, :a_module)

      assert message1 == %Message{
               acknowledger:
                 {:a_module,
                  %{
                    receipt: %{id: "Id_1", receipt_handle: "ReceiptHandle_1"},
                    sqs_client:
                      {ExAwsClient,
                       %{
                         config: [
                           http_client: FakeHttpClient,
                           access_key_id: "FAKE_ID",
                           secret_access_key: "FAKE_KEY"
                         ],
                         queue_name: "my_queue",
                         receive_messages_opts: [max_number_of_messages: 10]
                       }}
                  }},
               data: "Message 1",
               processor_pid: nil,
               publisher: :default
             }

      assert message2.data == "Message 2"
    end

    test "send a SQS/ReceiveMessage request with default options", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      ExAwsClient.receive_messages(10, opts, :a_module)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == "Action=ReceiveMessage&MaxNumberOfMessages=10"
      assert url == "https://sqs.us-east-1.amazonaws.com/my_queue"
    end

    test "request with custom :wait_time_seconds", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:wait_time_seconds, 0) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts, :a_module)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body =~ "WaitTimeSeconds=0"
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts, :a_module)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body =~ "MaxNumberOfMessages=5"
    end

    test "request with custom :config options", %{opts: base_opts} do
      config =
        Keyword.merge(base_opts[:config],
          scheme: "http://",
          host: "localhost",
          port: 9324
        )

      {:ok, opts} = Keyword.put(base_opts, :config, config) |> ExAwsClient.init()

      ExAwsClient.receive_messages(10, opts, :a_module)

      assert_received {:http_request_called, %{url: url}}
      assert url == "http://localhost:9324/my_queue"
    end
  end

  describe "delete_messages/2" do
    setup do
      %{
        opts: [
          queue_name: "my_queue",
          config: [
            http_client: FakeHttpClient,
            access_key_id: "FAKE_ID",
            secret_access_key: "FAKE_KEY"
          ]
        ]
      }
    end

    test "send a SQS/DeleteMessageBatch request", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)

      ExAwsClient.delete_messages(
        [
          %{id: "1", receipt_handle: "abc"},
          %{id: "2", receipt_handle: "def"}
        ],
        opts
      )

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body ==
               "Action=DeleteMessageBatch" <>
                 "&DeleteMessageBatchRequestEntry.1.Id=1&DeleteMessageBatchRequestEntry.1.ReceiptHandle=abc" <>
                 "&DeleteMessageBatchRequestEntry.2.Id=2&DeleteMessageBatchRequestEntry.2.ReceiptHandle=def"

      assert url == "https://sqs.us-east-1.amazonaws.com/my_queue"
    end

    test "request with custom :config options", %{opts: base_opts} do
      config =
        Keyword.merge(base_opts[:config],
          scheme: "http://",
          host: "localhost",
          port: 9324
        )

      {:ok, opts} = Keyword.put(base_opts, :config, config) |> ExAwsClient.init()

      ExAwsClient.delete_messages([%{id: "1", receipt_handle: "abc"}], opts)

      assert_received {:http_request_called, %{url: url}}
      assert url == "http://localhost:9324/my_queue"
    end
  end
end
