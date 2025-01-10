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

  If no data exists, nil is returned. There is no initial state.
  """

  # node_args: [
  #     tx_args: [
  #       mempool: [transactions: [], round: 0, consensus: []],
  #       ordering: [next_height: 1],
  #       storage: [uncommitted_height: 0]
  #     ],
  #     node_id: "LTU3NjQ2MDc0ODcwMTQ4MzM3NQ=="
  #   ]
  @spec initial_state(String.t()) :: any()
  def initial_state(_node_id) do
    # if Tables.existing_tables?(node_id) do
    #   nil
    # else
    #   nil
    # end
    :ok
  end

  # def storage_arguments(node_id) do
  # end

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

  ############################################################
  #                       Helpers                             #
  ############################################################

  # @type block_info :: {integer(), integer()}
  # @doc """
  # I return all the blocks from the given table.
  # I return a tuple with the latest round and total length of all blocks.
  # """
  # @spec block_info(atom()) :: block_info
  # defp block_info(table) do
  #   case :mnesia.match_object({table, :_, :_}) do
  #     # no blocks found, return default empty block
  #     [] ->
  #       [{:ok, -1, []}]

  #     blocks ->
  #       blocks
  #   end
  #   |> Enum.reduce({nil, 0}, fn {_table, round, block}, {_round, height} ->
  #     {round, height + length(block)}
  #   end)
  # end
end
