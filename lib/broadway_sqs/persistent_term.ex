defmodule BroadwaySQS.Storage.PersistentTerm do
  @moduledoc """
  A simple term storage to avoid passing large amounts of data between processes.

  If you have a large amount of data and you want to avoid passing it between
  processes, you can use the `PersistentTerm`. The `PersistentTerm` creates a persistent term
   for it, allowing you pass the reference around instead of the term.

  If the same term is put multiple times, it is stored only once, avoiding
  generating garbage. However, the stored terms are never removed.
  A common use case for this feature is in Broadway.Producer, which may need
  to pass a lot of information to acknowledge messages. With this module, you
  can store those terms when the producer starts and only pass the reference
  between messages:

      iex> ref = BroadwaySQS.Storage.PersistentTerm.put({:foo, :bar, :baz}) # On init
      iex> BroadwaySQS.Storage.PersistentTerm.get!(ref) # On ack
      {:foo, :bar, :baz}

  """

  use GenServer
  @name __MODULE__

  @doc false
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: @name)
  end

  @doc """
  Gets a previously stored term.
  """
  def get!(term) do
    find_by_term(term)
  end

  @doc """
  Delete a previously stored term.
  """
  def delete!(term) do
    :persistent_term.erase(term)
  end

  @doc """
  Puts a term.
  """
  def put(term) do
    find_by_term(term) || GenServer.call(@name, {:put, term}, :infinity)
  end

  # Callbacks

  @impl true
  def init(:ok) do
    {:ok, []}
  end

  @impl true
  def handle_call({:put, term}, _from, state) do
    case find_by_term(term) do
      {:error, _} -> {:reply, nil, state}
      term -> {:reply, term, state}
    end
  end

  defp find_by_term(term) do
    try do
      :persistent_term.get(term)
    rescue
      {:error, :term_not_found}
    end
  end
end
