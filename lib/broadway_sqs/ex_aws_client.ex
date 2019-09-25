defmodule BroadwaySQS.ExAwsClient do
  @moduledoc """
  Default SQS client used by `BroadwaySQS.Producer` to communicate with AWS
  SQS service. This client uses the `ExAws.SQS` library and implements the
  `BroadwaySQS.SQSClient` and `Broadway.Acknowledger` behaviours which define
  callbacks for receiving and acknowledging messages.
  """

  alias Broadway.{Message, Acknowledger}
  require Logger

  @behaviour BroadwaySQS.SQSClient
  @behaviour Acknowledger

  @default_max_number_of_messages 10
  @max_num_messages_allowed_by_aws 10
  @max_visibility_timeout_allowed_by_aws_in_seconds 12 * 60 * 60
  @supported_attributes [
    :sender_id,
    :sent_timestamp,
    :approximate_receive_count,
    :approximate_first_receive_timestamp,
    :wait_time_seconds,
    :receive_message_wait_time_seconds
  ]

  @impl true
  def init(opts) do
    with {:ok, queue_url} <- validate(opts, :queue_url, required: true),
         {:ok, receive_messages_opts} <- validate_receive_messages_opts(opts),
         {:ok, config} <- validate(opts, :config, default: []) do
      ack_ref = Broadway.TermStorage.put(%{queue_url: queue_url, config: config})

      {:ok,
       %{
         queue_url: queue_url,
         receive_messages_opts: receive_messages_opts,
         config: config,
         ack_ref: ack_ref
       }}
    end
  end

  @impl true
  def receive_messages(demand, opts) do
    receive_messages_opts = put_max_number_of_messages(opts.receive_messages_opts, demand)

    opts.queue_url
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

    opts.queue_url
    |> ExAws.SQS.delete_message_batch(receipts)
    |> ExAws.request(opts.config)
  end

  defp wrap_received_messages({:ok, %{body: body}}, ack_ref) do
    Enum.map(body.messages, fn message ->
      metadata = Map.delete(message, :body)
      acknowledger = build_acknowledger(message, ack_ref)
      %Message{data: message.body, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp wrap_received_messages({:error, reason}, _) do
    Logger.error("Unable to fetch events from AWS. Reason: #{inspect(reason)}")
    []
  end

  defp build_acknowledger(message, ack_ref) do
    receipt = %{id: message.message_id, receipt_handle: message.receipt_handle}
    {__MODULE__, ack_ref, %{receipt: receipt}}
  end

  defp put_max_number_of_messages(receive_messages_opts, demand) do
    max_number_of_messages = min(demand, receive_messages_opts[:max_number_of_messages])
    Keyword.put(receive_messages_opts, :max_number_of_messages, max_number_of_messages)
  end

  defp extract_message_receipt(message) do
    {_, _, %{receipt: receipt}} = message.acknowledger
    receipt
  end

  defp validate(opts, key, options \\ []) when is_list(opts) do
    has_key = Keyword.has_key?(opts, key)
    required = Keyword.get(options, :required, false)
    default = Keyword.get(options, :default)

    cond do
      has_key ->
        validate_option(key, opts[key])

      required ->
        {:error, "#{inspect(key)} is required"}

      default != nil ->
        validate_option(key, default)

      true ->
        {:ok, nil}
    end
  end

  defp validate_option(:config, value) when not is_list(value),
    do: validation_error(:config, "a keyword list", value)

  defp validate_option(:queue_url, value) when not is_binary(value) or value == "",
    do: validation_error(:queue_url, "a non empty string", value)

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

  defp validate_option(:visibility_timeout, value)
       when value not in 0..@max_visibility_timeout_allowed_by_aws_in_seconds do
    validation_error(
      :visibility_timeout,
      "an integer between 0 and #{@max_visibility_timeout_allowed_by_aws_in_seconds}",
      value
    )
  end

  defp validate_option(:attribute_names, value) do
    supported? = fn name -> name in @supported_attributes end

    if value == :all || (is_list(value) && Enum.all?(value, supported?)) do
      {:ok, value}
    else
      validation_error(
        :attribute_names,
        ":all or a list containing any of #{inspect(@supported_attributes)}",
        value
      )
    end
  end

  defp validate_option(:message_attribute_names, value) do
    non_empty_string? = fn name -> is_binary(name) && name != "" end

    if value == :all || (is_list(value) && Enum.all?(value, non_empty_string?)) do
      {:ok, value}
    else
      validation_error(:message_attribute_names, ":all or a list of non empty strings", value)
    end
  end

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_receive_messages_opts(opts) do
    with {:ok, wait_time_seconds} <- validate(opts, :wait_time_seconds),
         {:ok, attribute_names} <- validate(opts, :attribute_names),
         {:ok, message_attribute_names} <- validate(opts, :message_attribute_names),
         {:ok, max_number_of_messages} <-
           validate(opts, :max_number_of_messages, default: @default_max_number_of_messages),
         {:ok, visibility_timeout} <- validate(opts, :visibility_timeout) do
      validated_opts = [
        max_number_of_messages: max_number_of_messages,
        wait_time_seconds: wait_time_seconds,
        visibility_timeout: visibility_timeout,
        attribute_names: attribute_names,
        message_attribute_names: message_attribute_names
      ]

      {:ok, Enum.filter(validated_opts, fn {_, value} -> value end)}
    end
  end
end
