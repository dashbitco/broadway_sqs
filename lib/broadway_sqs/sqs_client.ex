defmodule BroadwaySQS.SQSClient do
  @moduledoc """
  A generic behaviour to implement SQS CLients for `BroadwaySQS.SQSProducer`.
  This module defines callbacks to normalize options, receive message batches and
  acknowledge/delete messages from a SQS queue. Modules that implement this behaviour
  should be passed as the `:sqs_client` option from `BroadwaySQS.SQSProducer`.

  """

  @type received_messages :: [Broadway.Message.t()]

  @type message_receipt :: %{id: binary, receipt_handle: binary}

  @type message_receipts :: [message_receipt]

  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, message :: binary}
  @callback receive_messages(total_demand :: pos_integer, opts :: any, ack_module :: module) ::
              received_messages
  @callback delete_messages(message_receipts, opts :: any) :: no_return
end
