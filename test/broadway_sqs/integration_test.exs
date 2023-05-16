defmodule BroadwaySQS.BroadwaySQS.IntegrationTest do
  use ExUnit.Case, async: false

  alias Plug.Conn

  defmodule MyConsumer do
    use Broadway

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    def init(opts) do
      {:ok, opts}
    end

    def handle_message(_, message, %{my_pid: my_pid}) do
      send(my_pid, {:message_handled, message.data, message.metadata})
      message
    end

    def handle_batch(_, messages, _, %{my_pid: my_pid}) do
      send(my_pid, {:batch_handled, messages})
      messages
    end
  end

  @receive_message_response """
  <?xml version="1.0"?>
  <ReceiveMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
    <ReceiveMessageResult>
      <Message>
        <MessageId>7cd4d61a-2d9a-4922-9738-308af6126fea</MessageId>
        <ReceiptHandle>receipt-handle-1</ReceiptHandle>
        <MD5OfBody>8cd6cfc2639481fee178bd04dd3628a7</MD5OfBody>
        <Body>hello world</Body>
      </Message>
      <Message>
        <MessageId>c431bcb8-3275-4cbb-a4a1-7bcbbc5773d1</MessageId>
        <ReceiptHandle>receipt-handle-2</ReceiptHandle>
        <MD5OfBody>35179a54ea587953021400eb0cd23201</MD5OfBody>
        <Body>how are you?</Body>
      </Message>
    </ReceiveMessageResult>
    <ResponseMetadata>
      <RequestId>251d6374-3eac-5128-b100-3b06e7db493a</RequestId>
    </ResponseMetadata>
  </ReceiveMessageResponse>
  """

  @receive_message_empty_response """
  <?xml version="1.0"?>
  <ReceiveMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
    <ReceiveMessageResult>
    </ReceiveMessageResult>
    <ResponseMetadata>
      <RequestId>251d6374-3eac-5128-b100-3b06e7db493a</RequestId>
    </ResponseMetadata>
  </ReceiveMessageResponse>
  """

  @delete_message_response """
  <?xml version="1.0"?>
  <DeleteMessageBatchResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
    <DeleteMessageBatchResult>
      <DeleteMessageBatchResultEntry>
        <Id>my-delete-message-batch-id-1</Id>
      </DeleteMessageBatchResultEntry>
      <DeleteMessageBatchResultEntry>
        <Id>my-delete-message-batch-id-2</Id>
      </DeleteMessageBatchResultEntry>
    </DeleteMessageBatchResult>
    <ResponseMetadata>
      <RequestId>1164da49-dd83-5d9d-acaa-823b38f2d2f8</RequestId>
    </ResponseMetadata>
  </DeleteMessageBatchResponse>
  """

  defmodule RequestConter do
    use Agent

    def start_link(counters) do
      Agent.start_link(fn -> counters end, name: __MODULE__)
    end

    def count_for(request_name) do
      if Process.whereis(__MODULE__) do
        Agent.get(__MODULE__, & &1[request_name])
      end
    end

    def increment_for(request_name) do
      if Process.whereis(__MODULE__) do
        Agent.update(__MODULE__, &Map.put(&1, request_name, &1[request_name] + 1))
      end
    end
  end

  setup do
    bypass = Bypass.open()

    Application.put_env(:ex_aws, :sqs,
      scheme: "http",
      host: "localhost",
      port: bypass.port
    )

    on_exit(fn -> Application.delete_env(:ex_aws, :sqs) end)

    {:ok, bypass: bypass}
  end

  test "consume messages from SQS and ack it", %{bypass: bypass} do
    us = self()

    Bypass.expect(bypass, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      response =
        case body do
          "Action=ReceiveMessage" <> _rest ->
            if RequestConter.count_for(:receive_message) > 5 do
              @receive_message_empty_response
            else
              RequestConter.increment_for(:receive_message)
              @receive_message_response
            end

          "Action=DeleteMessageBatch" <> _rest ->
            RequestConter.increment_for(:delete_message_batch)
            send(us, :messages_deleted)
            @delete_message_response
        end

      conn
      |> Conn.put_resp_header("content-type", "text/xml")
      |> Conn.resp(200, response)
    end)

    {:ok, _} = RequestConter.start_link(%{receive_message: 0, delete_message_batch: 0})

    {:ok, _consumer} = start_fake_consumer(bypass)

    assert_receive {:message_handled, "hello world", %{receipt_handle: "receipt-handle-1"}}, 1_000
    assert_receive {:message_handled, "how are you?", %{receipt_handle: "receipt-handle-2"}}

    assert_receive {:batch_handled, _messages}

    assert_receive :messages_deleted
    assert_receive :messages_deleted
    assert_receive :messages_deleted

    assert RequestConter.count_for(:receive_message) == 6
    assert RequestConter.count_for(:delete_message_batch) == 3
  end

  defp start_fake_consumer(bypass) do
    Broadway.start_link(MyConsumer,
      name: MyConsumer,
      producer: [
        module:
          {BroadwaySQS.Producer,
           sqs_client: BroadwaySQS.ExAwsClient,
           max_number_of_messages: 2,
           config: [
             access_key_id: "MY_AWS_ACCESS_KEY_ID",
             secret_access_key: "MY_AWS_SECRET_ACCESS_KEY",
             region: "us-east-2"
           ],
           queue_url: queue_endpoint_url(bypass)},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        default: [batch_size: 4, batch_timeout: 2000]
      ],
      context: %{my_pid: self()}
    )
  end

  defp queue_endpoint_url(bypass) do
    "http://localhost:#{bypass.port}/my_queue"
  end
end
