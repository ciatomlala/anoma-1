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

  require Logger

  ############################################################
  #                       Types                              #
  ############################################################

  @type storage_args :: [uncommitted_height: non_neg_integer()]

  @type ordering_args :: [next_height: non_neg_integer()]

  @type mempool_args :: [
          transactions: [any()],
          round: non_neg_integer(),
          consensus: [any()]
        ]

  @type startup_args :: [
          mempool: mempool_args,
          storage: storage_args,
          ordering: ordering_args
        ]

  ############################################################
  #                       Public                             #
  ############################################################
  @doc """
  Given a node id, I will determine the startup arguments for the node
  depending on the data found in the database.

  If no data exists, nil is returned. There is no initial state.

  # node_args: [
  #     tx_args: [
  #       mempool: [transactions: [], round: 0, consensus: []],
  #       ordering: [next_height: 1],
  #       storage: [uncommitted_height: 0]
  #     ],
  #     node_id: "LTU3NjQ2MDc0ODcwMTQ4MzM3NQ=="
  #   ]
  """
  @spec startup_arguments(String.t()) :: {:ok, startup_args}
  def(startup_arguments(node_id)) do
    with {:ok, storage} <- storage_arguments(node_id),
         {:ok, ordering} <- ordering_arguments(node_id),
         {:ok, mempool} <- mempool_arguments(node_id) do
      {:ok,
       [
         tx_args: [
           mempool: mempool,
           ordering: ordering,
           storage: storage
         ]
       ]}
    end
  end

  @spec storage_arguments(String.t()) :: {:ok, storage_args}
  def storage_arguments(node_id) do
    # read the blocks table for this node
    blocks_table = Tables.table_blocks(node_id)
    {:ok, blocks_summary} = block_table_summary(blocks_table)

    {:ok, [uncommitted_height: blocks_summary.last_round]}
  end

  @spec ordering_arguments(binary()) :: {:ok, ordering_args}
  def ordering_arguments(node_id) do
    # read the blocks table for this node
    blocks_table = Tables.table_blocks(node_id)
    {:ok, blocks_summary} = block_table_summary(blocks_table)

    {:ok, [next_height: blocks_summary.transaction_count + 1]}
  end

  @spec mempool_arguments(String.t()) :: {:ok, mempool_args}
  def mempool_arguments(node_id) do
    # read the blocks table for this node
    events_table = Tables.table_events(node_id)
    {:ok, events_summary} = events_table_summary(events_table)
    IO.inspect(events_summary)

    {:ok,
     [
       transactions: events_summary.transactions,
       round: events_summary.next_round,
       consensus: events_summary.consensus
     ]}
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

  ############################################################
  #                       Helpers                             #
  ############################################################

  @type block_table_summary :: %{
          last_round: non_neg_integer,
          transaction_count: non_neg_integer
        }

  @doc """
  I return a summary of all the required information from the blocks table.
  """
  @spec block_table_summary(atom()) ::
          {:ok, block_table_summary} | {:error, :failed_to_create_summary}
  def block_table_summary(table) do
    :mnesia.transaction(fn ->
      default_summary = %{last_round: 0, transaction_count: 0}

      case :mnesia.match_object({table, :_, :_}) do
        # no blocks found, return default empty block
        [] ->
          default_summary

        blocks ->
          blocks
          |> Enum.reduce(default_summary, fn {_, round, txs}, summary ->
            summary
            |> Map.update!(:last_round, &max(round, &1))
            |> Map.update!(:transaction_count, &(&1 + Enum.count(txs)))
          end)
      end
    end)
    |> case do
      {:atomic, summary} ->
        {:ok, summary}

      _ ->
        {:error, :failed_to_create_summary}
    end
  end

  def events_table_summary(table) do
    :mnesia.transaction(fn ->
      default_summary = %{transactions: [], next_round: 0, consensus: []}

      # fetch the list of transactions from the events table
      # use guard specs to get all values, except the consensus and round values
      # pattern to destructure every record against
      matchhead = {:"$1", :"$2", :"$3"}

      # guards to filter out objects we're not interested in
      # consensus = {:"=:=", :"$2", :consensus}
      # round = {:"=:=", :"$2", :round}
      # guards = [not: {:orelse, consensus, round}]

      # values of the result we want to get back
      result = [{{:"$2", :"$3"}}]

      case :mnesia.select(table, [{matchhead, [], [result]}]) do
        # no blocks found, return default empty block
        [] ->
          default_summary

        data ->
          data
          |> Enum.reduce(default_summary, fn
            [round: round], summary ->
              Map.put(summary, :round, round + 1)

            [consensus: transaction_ids], summary ->
              Map.put(summary, :consensus, transaction_ids)

            [{tx_id, tr}], summary ->
              Map.update!(summary, :transactions, &[{tx_id, tr} | &1])
          end)
      end
    end)
    |> case do
      {:atomic, summary} ->
        {:ok, summary}

      e ->
        Logger.error(inspect(e))
        {:error, :failed_to_create_summary}
    end
  end
end
