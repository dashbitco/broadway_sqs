defmodule BroadwaySQS.SQSClient do
  @moduledoc """
  A generic behaviour to implement SQS CLients for `BroadwaySQS.SQSProducer`.
  This module defines callbacks to normalize options, receive message batches and
  acknowledge/delete messages from a SQS queue. Modules that implement this behaviour
  should be passed as the `:sqs_client` option from `BroadwaySQS.SQSProducer`.

  """

  alias Broadway.Message

  @type messages :: [Message.t()]

  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, message :: binary}
  @callback receive_messages(total_demand :: pos_integer, opts :: any, ack_module :: module) ::
              messages
  @callback delete_messages(messages, opts :: any) :: no_return
end
