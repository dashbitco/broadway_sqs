defmodule BroadwaySQS.Options do
  @moduledoc false

  def definition() do
    [
      queue_url: [
        required: true,
        type: {
          :custom,
          __MODULE__,
          :type_non_empty_string,
          [[{:name, :queue_url}]]
        },
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
        type: {
          :custom,
          __MODULE__,
          :type_bounded_integer,
          [[{:name, :max_number_of_messages}, {:min, 1}, {:max, 10}]]
        },
        default: 10
      ],
      wait_time_seconds: [
        type: :non_neg_integer
      ],
      visibility_timeout: [
        type: {
          :custom,
          __MODULE__,
          :type_bounded_integer,
          [[{:name, :visibility_timeout}, {:min, 0}, {:max, 43200}]]
        }
      ],
      attribute_names: [
        type: {
          :custom,
          __MODULE__,
          :type_list_limited_member,
          [
            [
              {:name, :attribute_names},
              {:allowed_members,
               [
                 :sender_id,
                 :sent_timestamp,
                 :approximate_receive_count,
                 :approximate_first_receive_timestamp,
                 :sequence_number,
                 :message_deduplication_id,
                 :message_group_id,
                 :aws_trace_header
               ]}
            ]
          ]
        }
      ],
      message_attribute_names: [
        type: {
          :custom,
          __MODULE__,
          :type_array_non_empty_string,
          [[{:name, :queue_url}]]
        }
      ]
    ]
  end

  def type_bounded_integer(value, [{:name, _}, {:min, min}, {:max, max}])
      when is_integer(value) and value >= min and value <= max do
    {:ok, value}
  end

  def type_bounded_integer(value, [{:name, name}, {:min, min}, {:max, max}]) do
    {:error,
     "expected :#{name} to be an integer between #{min} and #{max}, got: #{inspect(value)}"}
  end

  def type_array_non_empty_string(value, [{:name, name}]) do
    invalid_members =
      value
      |> Enum.filter(&(is_nil(&1) || &1 == "" || !is_binary(&1)))

    if invalid_members == [] do
      {:ok, value}
    else
      {:error,
       "expected :#{name} to be a list with non-empty strings, got: #{inspect(invalid_members)}"}
    end
  end

  def type_non_empty_string("", [{:name, name}]) do
    {:error, "expected :#{name} to be an string, got: \"\""}
  end

  def type_non_empty_string(value, _) when not is_nil(value) and is_binary(value) do
    {:ok, value}
  end

  def type_non_empty_string(value, [{:name, name}]) do
    {:error, "expected :#{name} to be an string, got: #{inspect(value)}"}
  end

  def type_list_limited_member(value, [{:name, name}, {:allowed_members, allowed_members}]) do
    if value -- allowed_members == [] do
      {:ok, value}
    else
      {:error,
       "expected :#{name} to be a list with possible members #{inspect(allowed_members)}, got: #{
         inspect(value)
       }"}
    end
  end
end
