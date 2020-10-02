defmodule BroadwaySQS.TermStorage do
  @moduledoc """
  A simple term storage to avoid passing large amounts of data between processes.

  If you have a large amount of data and you want to avoid passing it between
  processes, you can use the `TermStorage`. The `TermStorage` creates a unique
  reference for it, allowing you pass the reference around instead of the term.

  If the same term is put multiple times, it is stored only once, avoiding
  generating garbage. However, the stored terms are never removed.
  A common use case for this feature is in Broadway.Producer, which may need
  to pass a lot of information to acknowledge messages. With this module, you
  can store those terms when the producer starts and only pass the reference
  between messages:

      iex> ref = BroadwaySQS.TermStorage.put({:foo, :bar, :baz}) # On init
      iex> BroadwaySQS.TermStorage.get!(ref) # On ack
      {:foo, :bar, :baz}

  """

  @name __MODULE__

  @doc """
  Gets a previously stored term.
  """
  def get!(ref) when is_reference(ref) do
    # :ets.lookup_element(@name, ref, 2)
    :persistent_term.get({@name, ref})
  end

  @doc """
  Puts a term.
  """
  def put(term) do
    ref = make_ref()
    :persistent_term.put({@name, ref}, term)
    ref
  end
end
