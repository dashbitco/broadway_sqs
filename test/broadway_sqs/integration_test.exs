defmodule FakeConsumerTwo do
  use Broadway

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def init(opts) do
    {:ok, opts}
  end

  def handle_message(_, message, %{my_pid: my_pid}) do
    send(my_pid, {:message_handled, message.data, message.metadata})
    message
  end

  def handle_batch(_, messages, _, _) do
    messages
  end
end

defmodule BroadwaySQS.BroadwaySQS.IntegrationTest do
  use ExUnit.Case, async: false

  alias Broadway.Message
  alias Plug.Conn

  setup do
    bypass = Bypass.open()

    Application.put_env(:ex_aws, :sqs,
      scheme: "http",
      host: "localhost",
      port: bypass.port,
      region: ""
    )

    on_exit(fn -> Application.delete_env(:ex_aws, :sqs) end)

    {:ok, bypass: bypass}
  end

  test "consume messages from SQS", %{bypass: bypass} do
    # The receive_action
    Bypass.expect(bypass, fn conn ->
      response = """
      <?xml version="1.0"?>
      <ReceiveMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
        <ReceiveMessageResult>
          <Message>
            <MessageId>7cd4d61a-2d9a-4922-9738-308af6126fea</MessageId>
            <ReceiptHandle>receipt-handle-1</ReceiptHandle>
            <MD5OfBody>8cd6cfc2639481fee178bd04dd3628a7</MD5OfBody>
            <Body>another message from queue</Body>
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

      conn
      |> IO.inspect()
      |> Conn.put_resp_header("content-type", "text/xml")
      |> Conn.put_resp_header("x-amzn-RequestId", "251d6374-3eac-5128-b100-3b06e7db493a")
      |> Conn.resp(200, response)
    end)

    {:ok, consumer} = start_fake_consumer(bypass)

    assert_receive {:message_handled, _, _}
  end

  defp queue_endpoint_url(bypass) do
    "http://localhost:#{bypass.port}/my_queue"
  end

  defp start_fake_consumer(bypass) do
    Broadway.start_link(FakeConsumerTwo,
      name: FakeConsumerTwo,
      producer: [
        module:
          {BroadwaySQS.Producer,
           sqs_client: BroadwaySQS.ExAwsClient,
           max_number_of_messages: 2,
           config: [
             access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
             secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY",
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
end
