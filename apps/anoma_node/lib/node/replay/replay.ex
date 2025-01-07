defmodule Anoma.Node.Replay do
  @moduledoc """
  I contain logic to replay storage.

  When a node starts, it can start with an empty database, a "fresh" node.
  If there was pre-existing data present, it's called an an "existing node".

  In case of a fresh node, the node can start up without any ceremony. The database
  is created and the node starts up.

  In case of an existing node, the exising data needs to be verified for correctness.
  The replay mechanism assures that the present data is correct.

  Correct data means that the data in the values and events table is correct and does not lead to
  errors.

  A replay uses the previous data to start up a temporary node with the existing data.
  If that node starts up succesfully, the data is considered valid.
  The real node can continue starting up using the old data.
  The mock node is removed from the system.
  """
  alias Anoma.Node.Transaction.Backends
  alias Anoma.Node.Tables

  require Logger

  ############################################################
  #                       Types                              #
  ############################################################

  @type block_info :: {integer(), integer()}
  @type round :: integer()
  @type consensi :: [[binary()]]
  @type transactions :: [Backends.transaction()]

  @type replay_data :: {consensi, round, block_info, transactions}

  ############################################################
  #                       Public                             #
  ############################################################

  def wait_for_confirm(_temp_node_id, nil) do
    {:ok, :confirmed}
  end

  def wait_for_confirm(temp_node_id, final_consensus) do
    result =
      receive do
        %{body: %{node_id: ^temp_node_id, body: %{order: ^final_consensus}}} ->
          {:ok, :confirmed}
      after
        1000 ->
          {:error, :confirm_failed}
      end

    result
  end

  @doc """
  I attempt to replay the data for a given node id.
  """
  @spec replay_for(String.t()) :: {:ok, :succeeded} | {:error, :replay_failed}
  def replay_for(node_id) do
    temp_node_id = temporary_node_id()

    with {:ok, _} <- init_tables_node(node_id, temp_node_id),
         {:ok, args} <- supervisor_args(node_id, temp_node_id) do
      EventBroker.subscribe_me([])

      Anoma.Supervisor.start_node(
        node_id: temp_node_id,
        tx_args: args,
        replay: false
      )

      final_consensus = List.last(args[:mempool][:consensus])

      case wait_for_confirm(temp_node_id, final_consensus) do
        {:ok, :confirmed} ->
          EventBroker.unsubscribe_me([])
          {:ok, args}

        _ ->
          EventBroker.unsubscribe_me([])
          :fail
      end
    end
  end

  @doc """
  Given a node id, I will create the tables for the node and initialize them with
  the data used to replay.
  I do not start a node, I only set up its tables.
  """
  def init_tables_node(from_node_id, to_node_id) do
    with {:ok, :created} <- Tables.initialize_tables_for_node(to_node_id),
         {:ok, :copied} <- duplicate_tables(from_node_id, to_node_id) do
      {:ok, :data_initialized}
    else
      {:ok, :existing} ->
        Logger.error("data found for replay node #{inspect(to_node_id)}")
        {:error, :target_node_existed}

      {:error, :failed_to_initialize_tables} ->
        Logger.error("failed to create replay node #{inspect(to_node_id)}")
        {:error, :failed_to_create_replay_node}

      {:error, :failed_to_read_replay_data} ->
        Logger.error("failed to read replay data from node '#{from_node_id}'")
        {:error, :failed_to_create_replay_node}
    end
  end

  def supervisor_args(from_node_id, to_node_id) do
    with {:ok, replay_data} <- replay_data(from_node_id),
         {:ok, mempool} <- mempool_args(replay_data),
         {:ok, ordering} <- ordering_args(replay_data),
         {:ok, storage} <- storage_args(replay_data) do
      {:ok,
       [
         mempool: mempool,
         ordering: ordering,
         storage: storage
       ]}
    else
      {:ok, :existing} ->
        Logger.error("data found for replay node #{inspect(to_node_id)}")
        {:error, :target_node_existed}

      {:error, :failed_to_initialize_tables} ->
        Logger.error("failed to create replay node #{inspect(to_node_id)}")
        {:error, :failed_to_create_replay_node}

      {:error, :failed_to_read_replay_data} ->
        Logger.error("failed to read replay data from node '#{from_node_id}'")
        {:error, :failed_to_create_replay_node}
    end
  end

  @doc """
  I create a temporary node and return its node id.
  Ensure that the tables have been copied before creating the temporary node.
  """
  @spec temporary_node_id() :: String.t()
  def temporary_node_id() do
    Base.encode64("#{System.monotonic_time()}")
  end

  ############################################################
  #                       Private Helpers                    #
  ############################################################

  # @doc """
  # I retrieve all necessary data from the given node to replay a different node.

  # The replay data is the data necessary to create the arguments to start up a new node.

  # The replay data returns the consensi, last round, block information (height and round),
  # and transactions.
  # """
  @spec replay_data(String.t()) ::
          {:ok, replay_data} | {:error, :failed_to_read_replay_data}
  defp replay_data(node_id) do
    # table names for the given node
    events_table = Tables.table_events(node_id)
    blocks_table = Tables.table_blocks(node_id)

    # fetch the data from the mnesia tables of this node
    :mnesia.transaction(fn ->
      consensi = consensi(events_table)
      round = current_round(events_table)
      block_info = block_info(blocks_table)
      transactions = transactions(blocks_table)

      {:ok, consensi} =
        drop_executed_consensi(block_info, round, consensi, events_table)

      {consensi, round, block_info, transactions}
    end)
    |> case do
      {:atomic, {consensi, round, block_info, transactions}} ->
        {:ok, {consensi, round, block_info, transactions}}

      e ->
        Logger.error(inspect(e))
        {:error, :failed_to_read_replay_data}
    end
  end

  # @doc """
  # Given the replay data, I return the arguments to be passed to the
  # mempool in the temporary node.
  # """
  @spec mempool_args(replay_data) :: [
          transactions: [any()],
          round: non_neg_integer(),
          consensus: [any()]
        ]
  defp mempool_args(replay_data) do
    {consensi, round, _, transactions} = replay_data

    {:ok,
     [
       transactions: transactions,
       round: round + 1,
       consensus: consensi
     ]}
  end

  # @doc """
  # Given the replay data, I return the arguments to be passed to the
  # ordering engine in the temporary node.
  # """
  @spec ordering_args(replay_data) :: [next_height: non_neg_integer()]
  defp ordering_args(replay_data) do
    {_, _, {_committed_round, height}, _} = replay_data
    {:ok, [next_height: height + 1]}
  end

  # @doc """
  # Given the replay data, I return the arguments to be passed to the
  # storage engine in the temporary node.
  # """
  @spec storage_args(replay_data) :: [uncommitted_height: non_neg_integer()]
  defp storage_args(replay_data) do
    {_, _, {_committed_round, height}, _} = replay_data
    {:ok, [uncommitted_height: height]}
  end

  # @doc """
  # I copy the values and updates table from one node to another.
  # """
  @spec duplicate_tables(String.t(), String.t()) :: {:ok, :copied}
  defp duplicate_tables(from_node_id, to_node_id) do
    # copy the entire values table
    from_table = Tables.table_values(from_node_id)
    to_table = Tables.table_values(to_node_id)
    {:ok, :table_copied} = Tables.duplicate_table(from_table, to_table)

    # copy the entire updates table
    from_table = Tables.table_updates(from_node_id)
    to_table = Tables.table_updates(to_node_id)
    {:ok, :table_copied} = Tables.duplicate_table(from_table, to_table)

    {:ok, :copied}
  end

  ############################################################
  #                       Mnesia Helpers                     #
  ############################################################

  # @doc """
  # When a node starts up, and there were consensi in the mempool that
  # made it into a block, remove them.
  # """
  defp drop_executed_consensi(block_info, round, consensi, events_table) do
    {committed_round, _height} = block_info

    if committed_round == round do
      {:ok, consensi}
    else
      # keep only the consensi that have not been committed yet
      cutoff = committed_round - round + 1
      {in_block, remaining} = Enum.split(consensi, cutoff)

      # remove the transactions after committed_round
      for id <- Enum.concat(in_block) do
        :mnesia.delete({events_table, id})
      end

      {:ok, remaining}
    end
  end

  # @doc """
  # I return the list of consensi from the mnesia table.
  # If no list if found, I return the empty list.
  # """
  @spec consensi(atom()) :: consensi
  defp consensi(table) do
    case :mnesia.read({table, :consensus}) do
      [] ->
        []

      [{_, _, consensi}] ->
        consensi
    end
  end

  # @doc """
  # I return the round from the events table.
  # """
  @spec current_round(atom()) :: round()
  defp current_round(table) do
    case :mnesia.read({table, :round}) do
      [] ->
        -1

      [{_, _, round}] ->
        round
    end
  end

  # @doc """
  # I return all the blocks from the given table.
  # I return a tuple with the latest round and total length of all blocks.
  # """
  @spec block_info(atom()) :: block_info
  defp block_info(table) do
    case :mnesia.match_object({table, :_, :_}) do
      # no blocks found, return default empty block
      [] ->
        [{:ok, -1, []}]

      blocks ->
        blocks
    end
    |> Enum.reduce({nil, 0}, fn {_table, round, block}, {_round, height} ->
      {round, height + length(block)}
    end)
  end

  # @doc """
  # I return a list of all transactions in the events table.
  # """
  @spec transactions(atom()) :: transactions
  defp transactions(table) do
    # use guard specs to get all values, except the consensus and round values
    # pattern to destructure every record against
    matchhead = {:"$1", :"$2", :"$3"}

    # guards to filter out objects we're not interested in
    consensus = {:"=:=", :"$2", :consensus}
    round = {:"=:=", :"$2", :round}
    guards = [not: {:orelse, consensus, round}]

    # values of the result we want to get back
    result = [{{:"$2", :"$3"}}]

    # :mnesia.dirty_select(table, [
    #   {{:"$1", :"$2", :"$3"},
    #    [not: {:orelse, {:"=:=", :"$2", :consensus}, {:"=:=", :"$2", :round}}],
    #    [{{:"$1", :"$2"}}]}
    # ])

    :mnesia.select(table, [{matchhead, guards, [result]}])
  end
end
