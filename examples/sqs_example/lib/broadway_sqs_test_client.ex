defmodule BroadwaySQS.TestClient do
  @moduledoc """
    Dummy client to use during unit tests for BroadwaySQS
     doesn't actually do anything, as we don't need it to.
  """

  @behaviour BroadwaySQS.SQSClient

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  @impl true
  def receive_messages(_demand, _opts) do
    []
  end
end
