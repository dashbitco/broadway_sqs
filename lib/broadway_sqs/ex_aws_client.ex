defmodule BroadwaySQS.ExAwsClient do
  @moduledoc """
  Default SQS client used by `BroadwaySQS.SQSProducer` to communicate with AWS
  SQS service. This client implements the `BroadwaySQS.SQSClient` behaviour which
  defines callbacks for receiving and acknowledging messages.

  ## Options

    * `:queue_name` - Required. The name of the queue.
    * `:max_number_of_messages` - Optional. The maximum number of messages to be fetched
      per request. This value must be between `1` and `10`, which is the maximun number
      allowed by AWS. Default is `10`.
    * `:wait_time_seconds` - Optional. The duration (in seconds) for which the call waits
      for a message to arrive in the queue before returning.
    * `:config` - Optional. A set of options that overrides the default ExAws configuration
      options. The most commonly used options are: `:access_key_id`, `:secret_access_key`,
      `:scheme`, `:region` and `:port`. For a complete list of configuration options and
      their default values, please see the `ExAws` documentation.

  ### Example

      Broadway.start_link(MyBroadway, %{},
        name: MyBroadway,
        producers: [
          default: [
            module: BroadwaySQS.SQSProducer,
            arg: [
              sqs_client: {BroadwaySQS.ExAwsClient, [
                queue_name: "queue/my_local_queue",
                wait_time_seconds: 2,
                max_number_of_messages: 5,
                config: [
                  scheme: "http://",
                  host: "localhost",
                  port: 9324
                ]
              ]}
            ],
          ],
        ],
      )

  """

  alias Broadway.{Message, Acknowledger}

  @behaviour BroadwaySQS.SQSClient
  @behaviour Acknowledger

  @default_max_number_of_messages 10
  @max_num_messages_allowed_by_aws 10

  @impl true
  def init(opts) do
    with {:ok, queue_name} <- validate(opts, :queue_name),
         {:ok, receive_messages_opts} <- validate_receive_messages_opts(opts),
         {:ok, config} <- validate(opts, :config, []) do
      {:ok,
       %{
         queue_name: queue_name,
         receive_messages_opts: receive_messages_opts,
         config: config
       }}
    end
  end

  @impl true
  def receive_messages(demand, opts) do
    receive_messages_opts = put_max_number_of_messages(opts.receive_messages_opts, demand)

    opts.queue_name
    |> ExAws.SQS.receive_message(receive_messages_opts)
    |> ExAws.request(opts.config)
    |> wrap_received_messages(opts)
  end

  @impl true
  def ack(successful, _failed) do
    successful
    |> Enum.chunk_every(@max_num_messages_allowed_by_aws)
    |> Enum.each(&delete_messages/1)
  end

  defp delete_messages(messages) do
    [%Message{acknowledger: {_, %{sqs_client_opts: opts}}} | _] = messages
    receipts = Enum.map(messages, &extract_message_receipt/1)

    opts.queue_name
    |> ExAws.SQS.delete_message_batch(receipts)
    |> ExAws.request(opts.config)
  end

  defp wrap_received_messages({:ok, %{body: body}}, opts) do
    Enum.map(body.messages, fn message ->
      ack_data = %{
        receipt: %{id: message.message_id, receipt_handle: message.receipt_handle},
        sqs_client_opts: opts
      }

      %Message{data: message.body, acknowledger: {__MODULE__, ack_data}}
    end)
  end

  defp wrap_received_messages({:error, reason}, _) do
    # TODO: Treat errors properly
    IO.warn("Unable to fetch events from AWS. Reason: #{inspect(reason)}")
  end

  defp put_max_number_of_messages(receive_messages_opts, demand) do
    max_number_of_messages = min(demand, receive_messages_opts[:max_number_of_messages])
    Keyword.put(receive_messages_opts, :max_number_of_messages, max_number_of_messages)
  end

  defp extract_message_receipt(message) do
    {_, %{receipt: receipt}} = message.acknowledger
    receipt
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:config, value) when not is_list(value),
    do: validation_error(:config, "a keyword list", value)

  defp validate_option(:queue_name, value) when not is_binary(value) or value == "",
    do: validation_error(:queue_name, "a non empty string", value)

  defp validate_option(:wait_time_seconds, nil), do: {:ok, nil}

  defp validate_option(:wait_time_seconds, value) when not is_integer(value) or value < 0,
    do: validation_error(:wait_time_seconds, "a non negative integer", value)

  defp validate_option(:max_number_of_messages, value) when value not in 1..10,
    do: validation_error(:max_number_of_messages, "a integer between 1 and 10", value)

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_receive_messages_opts(opts) do
    with {:ok, wait_time_seconds} <- validate(opts, :wait_time_seconds),
         {:ok, max_number_of_messages} <-
           validate(opts, :max_number_of_messages, @default_max_number_of_messages) do
      wait_time_seconds_opt =
        if wait_time_seconds, do: [wait_time_seconds: wait_time_seconds], else: []

      {:ok, [max_number_of_messages: max_number_of_messages] ++ wait_time_seconds_opt}
    end
  end
end
