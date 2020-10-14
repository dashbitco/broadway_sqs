defmodule BroadwaySQS.SQSClient do
  @moduledoc """
  A generic behaviour to implement SQS Clients for `BroadwaySQS.Producer`.
  This module defines callbacks to normalize options and receive message
  from a SQS queue. Modules that implement this behaviour should be passed
  as the `:sqs_client` option from `BroadwaySQS.Producer`.
  """

  alias Broadway.Message

  @type messages :: [Message.t()]

  @callback receive_messages(demand :: pos_integer, opts :: any) :: messages
end
