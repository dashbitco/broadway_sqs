defmodule BroadwaySQS.Options do
  @moduledoc false

  def definition() do
    [
      queue_url: [
        required: true,
        type: :string,
        doc: """
        The SQS queue url.
        """
      ],
      sqs_client: [],
      receive_interval: [
        type: :non_neg_integer
      ],
      test_pid: [
        type: :pid
      ],
      message_server: [
        type: :pid
      ],
      on_success: [
        type: :atom,
        default: :ack
      ],
      on_failure: [
        type: :atom,
        default: :noop
      ],
      config: [
        type: :keyword_list,
        default: []
      ],
      max_number_of_messages: [
        type: :pos_integer,
        default: 10
      ],
      wait_time_seconds: [
        type: :non_neg_integer
      ],
      visibility_timeout: [
        type: :non_neg_integer
      ],
      attribute_names: [],
      message_attribute_names: []
    ]
  end
end
