defmodule BroadwaySQS.Producer do
  @moduledoc """
  A GenStage producer that continuously polls messages from a SQS queue and
  acknowledge them after being successfully processed.

  By default this producer uses `BroadwaySQS.ExAwsClient` to talk to SQS but
  you can provide your client by implementing the `BroadwaySQS.SQSClient`
  behaviour.

  For a quick getting started on using Broadway with Amazon SQS, please see
  the [Amazon SQS Guide](https://hexdocs.pm/broadway/amazon-sqs.html).

  ## Options for `BroadwaySQS.ExAwsClient`

    * `:queue_url` - Required. The full URL of the queue.

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

  ## Producer Options

  These options applies to all producers, regardless of client implementation:

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 5000.

    * `:sqs_client` - Optional. A module that implements the `BroadwaySQS.SQSClient`
      behaviour. This module is responsible for fetching and acknowledging the
      messages. Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs. Default
      is `BroadwaySQS.ExAwsClient`.

  ## Acknowledgments

  In case of successful processing, the message is properly acknowledge to SQS.
  In case of failures, no message is acknowledged, which means Amazon SQS will
  eventually redeliver the message on remove it based on the "Visibility Timeout"
  and "Max Receive Count" configurations. For more information, see:

    * ["Visibility Timeout" page on Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
    * ["Dead Letter Queue" page on Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)

  ### Batching

  Even if you are not interested in working with Broadway batches via the
  `handle_batch/3` callback, we recommend all Broadway pipelines with SQS
  producers to define a default batcher with `batch_size` set to 10, so
  messages can be acknowledged in batches, which improves the performance
  and reduce the costs of integrating with SQS.

  ## Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producers: [
          default: [
            module: {BroadwaySQS.Producer,
              queue_url: "https://us-east-2.queue.amazonaws.com/100000000001/my_queue",
              config: [
                access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
                secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY",
                region: "us-east-2"
              ]
            }
          ]
        ],
        processors: [
          default: []
        ],
        batchers: [
          default: [
            batch_size: 10,
            batch_timeout: 2000
          ]
        ]
      )

  The above configuration will set up a producer that continuously receives
  messages from `"my_queue"` and sends them downstream.

  ## Retrieving Metadata

  By default the following information is added to the `metadata` field in the
  `%Message{}` struct:

    * `message_id` - The message id received when the message was sent to the queue
    * `receipt_handle` - The receipt handle
    * `md5_of_body` - An MD5 digest of the message body

  You can access any of that information directly while processing the message:

      def handle_message(_, message, _) do
        receipt = %{
          id: message.metadata.message_id,
          receipt_handle: message.metadata.receipt_handle
        }

        # Do something with the receipt
      end

  If you want to retrieve `attributes` or `message_attributes`, you need to
  configure the `:attributes_names` and `:message_attributes_names` options
  accordingly, otherwise, attributes will not be attached to the response and
  will not be available in the `metadata` field

      producers: [
        default: [
          module: {BroadwaySQS.Producer,
            queue_url: "https://us-east-2.queue.amazonaws.com/100000000001/my_queue",
            # Define which attributes/message_attributes you want to be attached
            attribute_names: [:approximate_receive_count],
            message_attribute_names: ["SomeAttribute"],
          }
        ]
      ]

  and then in `handle_message`:

      def handle_message(_, message, _) do
        approximate_receive_count = message.metadata.attributes["approximate_receive_count"]
        some_attribute = message.metadata.message_attributes["SomeAttribute"]

        # Do something with the attributes
      end

  For more information on the `:attributes_names` and `:message_attributes_names`
  options.
  """

  use GenStage

  require Logger

  @default_receive_interval 5000

  @impl true
  def init(opts) do
    client = opts[:sqs_client] || BroadwaySQS.ExAwsClient
    receive_interval = opts[:receive_interval] || @default_receive_interval

    if Keyword.has_key?(opts, :queue_name) do
      Logger.error(
        "The option :queue_name has been removed in order to keep compatibility with " <>
          "ex_aws_sqs >= v3.0.0. Please set the full queue URL using the new :queue_url option."
      )
    end

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
