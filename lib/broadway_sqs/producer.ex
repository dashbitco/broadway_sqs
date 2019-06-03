defmodule BroadwaySQS.Producer do
  @moduledoc """
  A GenStage producer that continuously polls messages from a SQS queue and
  acknowledge them after being successfully processed.

  ## Options using ExAwsClient (Default)

    * `:queue_name` - Required. The name of the queue.
    * `:max_number_of_messages` - Optional. The maximum number of messages to be fetched
      per request. This value must be between `1` and `10`, which is the maximun number
      allowed by AWS. Default is `10`.
    * `:wait_time_seconds` - Optional. The duration (in seconds) for which the call waits
      for a message to arrive in the queue before returning.
    * `:visibility_timeout` - Optional. The time period (in seconds) that a message will
      remain _invisible_ to other consumers whilst still on the queue and not acknowledged.
      This is passed to SQS when the message (or messages) are read.
      This value must be between 0 and 43200 (12 hours).
    * `:attribute_names` - A list containing the names of attributes that should be
      attached to the response and appended to the `metadata` field of the message.
      Supported values are `:sender_id`, `:sent_timestamp`, `:approximate_receive_count`,
      `:approximate_first_receive_timestamp`, `:wait_time_seconds` and
      `:receive_message_wait_time_seconds`. You can also use `:all` instead of the list
      if you want to retrieve all attributes.
    * `:message_attribute_names` - A list containing the names of custom message attributes
      that should be attached to the response and appended to the `metadata` field of the
      message. You can also use `:all` instead of the list if you want to retrieve all
      attributes.
    * `:config` - Optional. A set of options that overrides the default ExAws configuration
      options. The most commonly used options are: `:access_key_id`, `:secret_access_key`,
      `:scheme`, `:region` and `:port`. For a complete list of configuration options and
      their default values, please see the `ExAws` documentation.

  ## Additional options

    * `:sqs_client` - Optional. A module that implements the `BroadwaySQS.SQSClient`
      behaviour. This module is responsible for fetching and acknowledging the
      messages. Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs. Default
      is `ExAwsClient`.
    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 5000.

  ### Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producers: [
          default: [
            module: {BroadwaySQS.Producer,
              queue_name: "my_queue",
              config: [
                access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
                secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY",
                region: "us-east-2"
              ]
            }
          ]
        ]
      )

  The above configuration will set up a producer that continuously receives
  messages from `"my_queue"` and sends them downstream.

  For a complete guide on using Broadway with Amazon SQS, please see the
  [Amazon SQS Guide](https://hexdocs.pm/broadway/amazon-sqs.html).
  """

  use GenStage

  @default_receive_interval 5000

  @impl true
  def init(opts) do
    client = opts[:sqs_client] || BroadwaySQS.ExAwsClient
    receive_interval = opts[:receive_interval] || @default_receive_interval

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, opts} ->
        {:producer,
         %{
           demand: 0,
           receive_timer: nil,
           receive_interval: receive_interval,
           sqs_client: {client, opts}
         }}
    end
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    messages = receive_messages_from_sqs(state, demand)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_messages_from_sqs(state, total_demand) do
    %{sqs_client: {client, opts}} = state
    client.receive_messages(total_demand, opts)
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
