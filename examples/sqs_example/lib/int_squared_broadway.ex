defmodule BroadwaySQSExample.IntSquared do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    {module, opts} = Application.get_env(:broadway_sqs_example, :producer_module)
    opts = opts ++ [queue_url: Application.get_env(:broadway_sqs_example, :int_queue)]

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {module, opts}
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
  end

  def handle_message(_, %Message{data: data} = message, _) do
    IO.inspect(data)

    message
    |> Message.update_data(fn data -> String.to_integer(data) * String.to_integer(data) end)
  end

  def handle_batch(_, messages, _, _) do
    list = messages |> Enum.map(fn e -> e.data end)
    IO.inspect(list, label: "Got batch, sending acks to SQS")
    messages
  end
end
