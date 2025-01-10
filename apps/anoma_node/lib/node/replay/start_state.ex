defmodule Anoma.Node.Replay.State do
  @moduledoc """
  I define logic that determines the startup state for the node.

  When a node starts and data is present in the database, specific arguments have to
  be passed to the supervision tree in order for the node to pick up where it left off.

  In particular, the following data must be restored.

   - The pending transactions waiting for an ordering.
   - Consensi in the mempool that have not been turned into a block.
   - The height of the ordering engine.
   - The committed height of the storage engine.

  """

  alias Anoma.Node.Tables

  @doc """
  Given a node id, I will determine the startup arguments for the node
  depending on the data found in the database.
  """
  @spec initial_state(String.t()) :: any()
  def initial_state(node_id) do
  end

  @doc """
  Given a node id, I determine if there is existing data for this node.
  """
  @spec initialize_storage(String.t()) :: {:ok, :existing_node | :new_node}
  def initialize_storage(node_id) do
    # check if the node has existing tables, and initialize them if need be.
    case Tables.initialize_tables_for_node(node_id) do
      {:ok, :created} ->
        {:ok, :new_node}

      {:ok, :existing} ->
        {:ok, :existing_node}

      {:error, _e} ->
        {:error, :failed_to_initialize_storage}
    end
  end
end
