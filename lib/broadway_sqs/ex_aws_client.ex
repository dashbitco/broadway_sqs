defmodule BroadwaySQS.ExAwsClient do
  @moduledoc """
  Default SQS client used by `BroadwaySQS.Producer` to communicate with AWS
  SQS service. This client implements the `BroadwaySQS.SQSClient` behaviour which
  defines callbacks for receiving and acknowledging messages.
  """

  alias Broadway.{Message, Acknowledger}
  require Logger

  @behaviour BroadwaySQS.SQSClient
  @behaviour Acknowledger

  @default_max_number_of_messages 10
  @max_num_messages_allowed_by_aws 10
  @max_visibility_timeout_allowed_by_aws_in_seconds 12 * 60 * 60

  @impl true
  def init(opts) do
    with {:ok, queue_name} <- validate(opts, :queue_name),
         {:ok, receive_messages_opts} <- validate_receive_messages_opts(opts),
         {:ok, config} <- validate(opts, :config, []) do
      ack_ref = Broadway.TermStorage.put(%{queue_name: queue_name, config: config})

      {:ok,
       %{
         queue_name: queue_name,
         receive_messages_opts: receive_messages_opts,
         config: config,
         ack_ref: ack_ref
       }}
    end
  end

  @impl true
  def receive_messages(demand, opts) do
    receive_messages_opts = put_max_number_of_messages(opts.receive_messages_opts, demand)

    opts.queue_name
    |> ExAws.SQS.receive_message(receive_messages_opts)
    |> ExAws.request(opts.config)
    |> wrap_received_messages(opts.ack_ref)
  end

  @impl true
  def ack(ack_ref, successful, _failed) do
    successful
    |> Enum.chunk_every(@max_num_messages_allowed_by_aws)
    |> Enum.each(fn messages -> delete_messages(messages, ack_ref) end)
  end

  defp delete_messages(messages, ack_ref) do
    receipts = Enum.map(messages, &extract_message_receipt/1)

    opts = Broadway.TermStorage.get!(ack_ref)

    opts.queue_name
    |> ExAws.SQS.delete_message_batch(receipts)
    |> ExAws.request(opts.config)
  end

  defp wrap_received_messages({:ok, %{body: body}}, ack_ref) do
    Enum.map(body.messages, fn message ->
      ack_data = %{
        receipt: %{id: message.message_id, receipt_handle: message.receipt_handle}
      }

      %Message{data: message.body, acknowledger: {__MODULE__, ack_ref, ack_data}}
    end)
  end

  defp wrap_received_messages({:error, reason}, _) do
    Logger.error("Unable to fetch events from AWS. Reason: #{inspect(reason)}")
  end

  defp put_max_number_of_messages(receive_messages_opts, demand) do
    max_number_of_messages = min(demand, receive_messages_opts[:max_number_of_messages])
    Keyword.put(receive_messages_opts, :max_number_of_messages, max_number_of_messages)
  end

  defp extract_message_receipt(message) do
    {_, _, %{receipt: receipt}} = message.acknowledger
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

  defp validate_option(:max_number_of_messages, value)
       when value not in 1..@max_num_messages_allowed_by_aws do
    validation_error(
      :max_number_of_messages,
      "an integer between 1 and #{@max_num_messages_allowed_by_aws}",
      value
    )
  end

  defp validate_option(:visibility_timeout, nil), do: {:ok, nil}

  defp validate_option(:visibility_timeout, value)
       when value not in 0..@max_visibility_timeout_allowed_by_aws_in_seconds do
    validation_error(
      :visibility_timeout,
      "an integer between 0 and #{@max_visibility_timeout_allowed_by_aws_in_seconds}",
      value
    )
  end

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_receive_messages_opts(opts) do
    with {:ok, wait_time_seconds} <- validate(opts, :wait_time_seconds),
         {:ok, max_number_of_messages} <-
           validate(opts, :max_number_of_messages, @default_max_number_of_messages),
         {:ok, visibility_timeout} <- validate(opts, :visibility_timeout) do
      wait_time_seconds_opt =
        if wait_time_seconds, do: [wait_time_seconds: wait_time_seconds], else: []

      visibility_timeout_opt =
        if visibility_timeout, do: [visibility_timeout: visibility_timeout], else: []

      {:ok,
       [max_number_of_messages: max_number_of_messages] ++
         wait_time_seconds_opt ++ visibility_timeout_opt}
    end
  end
end
