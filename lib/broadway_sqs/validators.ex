defmodule BroadwaySQS.Validators do
  @moduledoc """
  The Validators used for sqs
  """

  @doc false
  def validate_configure_options!(options) do
    Enum.each(options, fn {option, value} ->
      with true <- option in [:on_success, :on_failure],
           {:ok, _} <- validate_option(option, value) do
        :ok
      else
        _ ->
          raise ArgumentError,
                "unsupported configure option #{inspect(option)} => #{inspect(value)}"
      end
    end)
  end

  @doc false
  def validate(opts, key, options \\ []) when is_list(opts) do
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

  @doc false
  def validate_option(:config, value) when not is_list(value),
    do: validation_error(:config, "a keyword list", value)

  @doc false
  def validate_option(:queue_url, value) when not is_binary(value) or value == "",
    do: validation_error(:queue_url, "a non empty string", value)

  @doc false
  def validate_option(:wait_time_seconds, value) when not is_integer(value) or value < 0,
    do: validation_error(:wait_time_seconds, "a non negative integer", value)

  @doc false
  def validate_option(:max_number_of_messages, value)
      when value not in 1..@max_num_messages_allowed_by_aws do
    validation_error(
      :max_number_of_messages,
      "an integer between 1 and #{@max_num_messages_allowed_by_aws}",
      value
    )
  end

  @doc false
  def validate_option(:visibility_timeout, value)
      when value not in 0..@max_visibility_timeout_allowed_by_aws_in_seconds do
    validation_error(
      :visibility_timeout,
      "an integer between 0 and #{@max_visibility_timeout_allowed_by_aws_in_seconds}",
      value
    )
  end

  @doc false
  def validate_option(:attribute_names, value) do
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

  @doc false
  def validate_option(:message_attribute_names, value) do
    non_empty_string? = fn name -> is_binary(name) && name != "" end

    if value == :all || (is_list(value) && Enum.all?(value, non_empty_string?)) do
      {:ok, value}
    else
      validation_error(:message_attribute_names, ":all or a list of non empty strings", value)
    end
  end

  @doc false
  def validate_option(option, value) when option in [:on_success, :on_failure] do
    if value in [:ack, :noop] do
      {:ok, value}
    else
      validation_error(option, ":ack or :noop", value)
    end
  end

  @doc false
  def validate_option(_, value), do: {:ok, value}

  @doc false
  def validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  @doc false
  def validate_receive_messages_opts(opts) do
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
