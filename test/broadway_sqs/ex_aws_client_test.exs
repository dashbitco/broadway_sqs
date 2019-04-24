defmodule BroadwaySQS.ExAwsClientTest do
  use ExUnit.Case

  alias BroadwaySQS.ExAwsClient
  alias Broadway.Message
  import ExUnit.CaptureLog

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
            <MD5OfBody>fake_md5</MD5OfBody>
            <Body>Message 1</Body>
            <Attribute>
              <Name>SenderId</Name>
              <Value>13</Value>
            </Attribute>
            <Attribute>
              <Name>ApproximateReceiveCount</Name>
              <Value>5</Value>
            </Attribute>
            <MessageAttribute>
              <Name>TestStringAttribute</Name>
              <Value>
                <StringValue>Test</StringValue>
                <DataType>String</DataType>
              </Value>
            </MessageAttribute>
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

  defmodule FakeHttpClientWithError do
    @behaviour ExAws.Request.HttpClient

    def request(:post, _url, "Action=ReceiveMessage" <> _, _, _) do
      {:error, %{reason: "Fake error"}}
    end
  end

  describe "validate init options" do
    test ":queue_name is required" do
      assert ExAwsClient.init([]) ==
               {:error, ":queue_name is required"}

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

    test ":attribute_names is optional without default value" do
      {:ok, result} = ExAwsClient.init(queue_name: "my_queue")

      refute Keyword.has_key?(result.receive_messages_opts, :attribute_names)
    end

    test ":attribute_names should be a list containing any of the supported attributes" do
      all_attribute_names = [
        :sender_id,
        :sent_timestamp,
        :approximate_receive_count,
        :approximate_first_receive_timestamp,
        :wait_time_seconds,
        :receive_message_wait_time_seconds
      ]

      {:ok, result} =
        ExAwsClient.init(queue_name: "my_queue", attribute_names: all_attribute_names)

      assert result.receive_messages_opts[:attribute_names] == all_attribute_names

      attribute_names = [:approximate_receive_count, :unsupported]

      {:error, message} =
        ExAwsClient.init(queue_name: "my_queue", attribute_names: attribute_names)

      assert message ==
               "expected :attribute_names to be :all or a list containing any of " <>
                 inspect(all_attribute_names) <>
                 ", got: [:approximate_receive_count, :unsupported]"
    end

    test ":message_attribute_names is optional without default value" do
      {:ok, result} = ExAwsClient.init(queue_name: "my_queue")

      refute Keyword.has_key?(result.receive_messages_opts, :message_attribute_names)
    end

    test ":message_attribute_names should be a list of non empty strings" do
      {:ok, result} =
        ExAwsClient.init(queue_name: "my_queue", message_attribute_names: ["attr_1", "attr_2"])

      assert result.receive_messages_opts[:message_attribute_names] == ["attr_1", "attr_2"]

      {:error, message} =
        ExAwsClient.init(
          queue_name: "my_queue",
          message_attribute_names: ["attr_1", :not_a_string]
        )

      assert message ==
               "expected :message_attribute_names to be :all or a list of non empty strings" <>
                 ", got: [\"attr_1\", :not_a_string]"
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
               "expected :max_number_of_messages to be an integer between 1 and 10, got: 0"

      {:error, message} = opts |> Keyword.put(:max_number_of_messages, 11) |> ExAwsClient.init()

      assert message ==
               "expected :max_number_of_messages to be an integer between 1 and 10, got: 11"

      {:error, message} =
        opts |> Keyword.put(:max_number_of_messages, :an_atom) |> ExAwsClient.init()

      assert message ==
               "expected :max_number_of_messages to be an integer between 1 and 10, got: :an_atom"
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

    test ":visibility_timeout should be a non negative integer" do
      opts = [queue_name: "my_queue"]

      {:ok, result} = opts |> Keyword.put(:visibility_timeout, 0) |> ExAwsClient.init()
      assert result.receive_messages_opts[:visibility_timeout] == 0

      {:ok, result} = opts |> Keyword.put(:visibility_timeout, 256) |> ExAwsClient.init()
      assert result.receive_messages_opts[:visibility_timeout] == 256

      {:error, message} = opts |> Keyword.put(:visibility_timeout, -1) |> ExAwsClient.init()

      assert message ==
               "expected :visibility_timeout to be an integer between 0 and 43200, got: -1"

      {:error, message} = opts |> Keyword.put(:visibility_timeout, :an_atom) |> ExAwsClient.init()

      assert message ==
               "expected :visibility_timeout to be an integer between 0 and 43200, got: :an_atom"
    end

    test ":visibility_timeout is optional without default value" do
      {:ok, result} = ExAwsClient.init(queue_name: "my_queue")

      refute Keyword.has_key?(result.receive_messages_opts, :visibility_timeout)
    end

    test ":visibility_timeout should be an integer between 0 seconds and 12 hours" do
      opts = [queue_name: "my_queue"]

      {:ok, result} = opts |> Keyword.put(:visibility_timeout, 0) |> ExAwsClient.init()
      assert result.receive_messages_opts[:visibility_timeout] == 0

      max_visibility_timeout = 12 * 60 * 60

      {:ok, result} =
        opts |> Keyword.put(:visibility_timeout, max_visibility_timeout) |> ExAwsClient.init()

      assert result.receive_messages_opts[:visibility_timeout] == max_visibility_timeout

      {:error, message} = opts |> Keyword.put(:visibility_timeout, -1) |> ExAwsClient.init()

      assert message ==
               "expected :visibility_timeout to be an integer between 0 and 43200, got: -1"

      one_day_in_seconds = 24 * 60 * 60

      {:error, message} =
        opts |> Keyword.put(:visibility_timeout, one_day_in_seconds) |> ExAwsClient.init()

      assert message ==
               "expected :visibility_timeout to be an integer between 0 and 43200, got: 86400"

      {:error, message} = opts |> Keyword.put(:visibility_timeout, :an_atom) |> ExAwsClient.init()

      assert message ==
               "expected :visibility_timeout to be an integer between 0 and 43200, got: :an_atom"
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
            secret_access_key: "FAKE_KEY",
            retries: [max_attempts: 0]
          ]
        ]
      }
    end

    test "returns a list of Broadway.Message with :data and :acknowledger set", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      [message1, message2] = ExAwsClient.receive_messages(10, opts)

      assert message1.data == "Message 1"
      assert message2.data == "Message 2"

      assert message1.acknowledger ==
               {ExAwsClient, opts.ack_ref,
                %{receipt: %{id: "Id_1", receipt_handle: "ReceiptHandle_1"}}}
    end

    test "add message_id, receipt_handle and md5_of_body to metadata", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      [%{metadata: metadata} | _] = ExAwsClient.receive_messages(10, opts)

      assert metadata.message_id == "Id_1"
      assert metadata.receipt_handle == "ReceiptHandle_1"
      assert metadata.md5_of_body == "fake_md5"
    end

    test "add attributes to metadata", %{opts: base_opts} do
      {:ok, opts} = Keyword.put(base_opts, :attribute_names, :all) |> ExAwsClient.init()

      [%{metadata: metadata_1}, %{metadata: metadata_2} | _] =
        ExAwsClient.receive_messages(10, opts)

      assert metadata_1.attributes == %{"sender_id" => 13, "approximate_receive_count" => 5}
      assert metadata_2.attributes == []
    end

    test "add message_attributes to metadata", %{opts: base_opts} do
      {:ok, opts} = Keyword.put(base_opts, :message_attribute_names, :all) |> ExAwsClient.init()

      [%{metadata: metadata_1}, %{metadata: metadata_2} | _] =
        ExAwsClient.receive_messages(10, opts)

      assert metadata_1.message_attributes == %{
               "TestStringAttribute" => %{
                 name: "TestStringAttribute",
                 data_type: "String",
                 string_value: "Test",
                 binary_value: "",
                 value: "Test"
               }
             }

      assert metadata_2.message_attributes == []
    end

    test "if the request fails, returns an empty list and log the error", %{opts: base_opts} do
      {:ok, opts} =
        base_opts
        |> put_in([:config, :http_client], FakeHttpClientWithError)
        |> ExAwsClient.init()

      assert capture_log(fn ->
               assert ExAwsClient.receive_messages(10, opts) == []
             end) =~ "[error] Unable to fetch events from AWS. Reason: \"Fake error\""
    end

    test "send a SQS/ReceiveMessage request with default options", %{opts: base_opts} do
      {:ok, opts} = ExAwsClient.init(base_opts)
      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == "Action=ReceiveMessage&MaxNumberOfMessages=10"
      assert url == "https://sqs.us-east-1.amazonaws.com/my_queue"
    end

    test "request with custom :wait_time_seconds", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:wait_time_seconds, 0) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body =~ "WaitTimeSeconds=0"
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> ExAwsClient.init()
      ExAwsClient.receive_messages(10, opts)

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

      ExAwsClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{url: url}}
      assert url == "http://localhost:9324/my_queue"
    end
  end

  describe "ack/2" do
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
      ack_data_1 = %{sqs_client_opts: opts, receipt: %{id: "1", receipt_handle: "abc"}}
      ack_data_2 = %{sqs_client_opts: opts, receipt: %{id: "2", receipt_handle: "def"}}

      ExAwsClient.ack(
        opts.ack_ref,
        [
          %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_1}, data: nil},
          %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data_2}, data: nil}
        ],
        []
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

      ack_data = %{receipt: %{id: "1", receipt_handle: "abc"}}
      message = %Message{acknowledger: {ExAwsClient, opts.ack_ref, ack_data}, data: nil}

      ExAwsClient.ack(opts.ack_ref, [message], [])

      assert_received {:http_request_called, %{url: url}}
      assert url == "http://localhost:9324/my_queue"
    end
  end
end
