defmodule Anoma.Node.Examples.EReplay.StartState do
  @moduledoc """
  I define examples on how the start state of a node is computed.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Examples.Mempool, as: EMempool
  alias Anoma.Node.Replay.State
  alias Anoma.Node.Tables
  alias Anoma.Node.Transaction.Executor
  alias Anoma.Node.Registry

  import ExUnit.Assertions
  import Mock

  # -----------------------------------------------------------
  # Table states

  @doc """
  I assert that a node that does not exist does not have any tables present.
  """
  @spec no_node_no_tables() :: :ok
  def no_node_no_tables() do
    non_existing_node_id =
      "ENode.random_node_id() is not available because of dependency issues this sucks fix this"

    has_tables? = Tables.has_data?(non_existing_node_id)
    assert has_tables? == {:error, :none_exist}
    :ok
  end

  @doc """
  I check whether a fresh node has all its tables created.
  """
  @spec new_node_has_tables(ENode.t()) :: ENode.t()
  def new_node_has_tables(enode \\ ENode.start_node()) do
    has_tables? = Tables.has_data?(enode.node_id)
    assert has_tables? == {:ok, :exists}

    enode
  end

  @spec partial_state_if_table_deleted() :: Anoma.Node.Examples.ENode.t()
  @doc """
  I check whether a node with some missing tables is marked as partial.
  """
  @spec partial_state_if_table_deleted(ENode.t()) :: ENode.t()
  def partial_state_if_table_deleted(enode \\ ENode.start_node()) do
    # delete a table for the given node
    table_to_delete = Tables.table_blocks(enode.node_id)

    {:atomic, :ok} = :mnesia.delete_table(table_to_delete)

    # there are not partial tables left
    has_tables? = Tables.has_data?(enode.node_id)
    assert has_tables? == {:error, :partial_exist}

    enode
  end

  # -----------------------------------------------------------
  # Mempool

  @doc """
  I start up a new node, or I assume that the given node is empty.

  I compute the startup arguments for this node and verify that they are the default arguments.
  """
  @spec mempool_args_fresh_node(ENode.t()) :: ENode.t()
  def mempool_args_fresh_node(enode \\ ENode.start_node()) do
    # there should be 0 transactions
    {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    # assert values in the arguments
    assert mempool_start_args[:transactions] == []
    assert mempool_start_args[:round] == 0
    assert mempool_start_args[:consensus] == []

    enode
  end

  @doc """
  I start up a new node, or assume the given node is empty.
  I add ten transactions to the mempool and complete all of them so that they are in a block.
  When I compute the startup arguments for this node's mempool, I expect to have the default arguments.
  """
  @spec mempool_args_non_fresh_node(ENode.t()) :: ENode.t()
  def mempool_args_non_fresh_node(enode \\ ENode.start_node()) do
    # run ten separate transactions in a block through the node.
    EMempool.complete_ten_transactions(enode)

    {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    # assert values in the arguments
    assert mempool_start_args[:transactions] == []
    assert mempool_start_args[:round] == 10
    assert mempool_start_args[:consensus] == []

    enode
  end

  @doc """
  I start up a new node, or assume the given node is empty.
  I add a transaction to the mempool.
  The startup arguments for this mempool should contain the transaction I added.
  """
  @spec mempool_args_non_block_transaction(ENode.t()) :: ENode.t()
  def mempool_args_non_block_transaction(enode \\ ENode.start_node()) do
    # run a transaction, but do not create a block
    # this will make sure the transaction is still present in the mempool's tables
    # and it should be restored.
    {_node, transaction} = EMempool.add_transaction(enode)

    # compute the mempool startup arguments
    {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    # assert the transaction I just added is in the list of the startup arguments.
    assert mempool_start_args[:transactions] == [
             {transaction.id, {transaction.backend, transaction.noun}}
           ]

    assert mempool_start_args[:round] == 0
    assert mempool_start_args[:consensus] == []

    enode
  end

  @doc """
  I start up a new node, or assume the given node is empty.
  I add a bunch of transactions to the mempool.
  The startup arguments for this mempool should contain the transactions I added.
  """
  @spec mempool_args_non_block_transactions(ENode.t()) :: ENode.t()
  def mempool_args_non_block_transactions(enode \\ ENode.start_node()) do
    # run 10 transactions, but do not create a block
    # this will make sure the transactions are still present in the mempool's tables
    # and they should be restored.
    transaction_list =
      for _ <- 1..10 do
        {_node, transaction} = EMempool.add_transaction(enode)
        {transaction.id, {transaction.backend, transaction.noun}}
      end

    # compute the mempool startup arguments
    {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    # assert the transaction I just added is in the list of the startup arguments.
    assert mempool_start_args[:transactions] -- transaction_list == []
    assert transaction_list -- mempool_start_args[:transactions] == []
    assert mempool_start_args[:round] == 0
    assert mempool_start_args[:consensus] == []

    enode
  end

  @doc """
  I start up a new node, or assume the given node is empty.

  I add a number of transactions to the mempool.

  When a transaction is added to the mempool, and a consensus process
  calls the Mempool.execute([transactions]) function, the following happens.
  - Mempool will fire a consensus event
  - Mempool will call Executor.execute for the given transactions.
  - The logging engine will write the order into the events table.

  This example does not want the Executor.execute call to happen, so it
  is mocked out.
  Removing that function effectively causes the transactions to never be executed.

  The logging engine might still be writing after the consensus event has been fired,
  so I wait for an mnesia event to be sure the table has been written.
  """
  def mempool_consensi_present(enode \\ ENode.start_node()) do
    with_mock Executor, [:passthrough], execute: fn _, _ -> :ok end do
      EventBroker.subscribe_me([])

      # subscribe to mnesia events as well. see below.
      events_table = Tables.table_events(enode.node_id)
      :mnesia.subscribe({:table, events_table, :simple})

      # start creating a block with a single transaction.
      {_enode, transaction} = EMempool.make_block(enode)

      # wait for the mnesia table to be written fully
      EMempool.wait_for_consensus_write(enode, transaction)

      # compute the mempool arguments.
      # expect that the consensus contains one element
      {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

      # assert values in the arguments
      expected_transactions = [
        {transaction.id, {transaction.backend, transaction.noun}}
      ]

      assert mempool_start_args[:transactions] == expected_transactions
      assert mempool_start_args[:round] == 0

      expected_consensus = [[transaction.id]]

      assert mempool_start_args[:consensus] == expected_consensus
    end

    enode
  end

  @doc """
  I start up a new node, or assume the given node is empty.
  I add a number of transactions to the mempool but do not let them form into a block.

  When a block is created, the Logging engine does the following:
  - It removes the transactions from the events table.
  - It removes the consensus for these transactions from the events table
  - It writes the round into the events table.

  When the node stops before this ceremony has happened, the following is true.
  - There is a block in the blocks table that holds a list of transactions
  - That same list of transactions is still present in the events table
  - The round of the events table is outdated.

  The startup arguments do not take into account transactions and consensi that are
  in an actual block, these are dropped. This test asserts that this is the case.

  It does so by doing the following.

  If a block event is fired, the Logging engine will remove these values from the database.
  We make sure that this not happen by mocking this behaviour.
  """
  def mempool_obsolete_consensi(enode \\ ENode.start_node()) do
    logging_engine = Registry.whereis(enode.node_id, Logging)

    # Logging.update(enode.node_id, vsn)
    # EventBroker.subscribe_me([])

    # create a block from a transaction
    # {_enode, _transaction} = EMempool.complete_transaction(enode)

    # # compute the mempool arguments.
    # # expect that the consensus contains one element
    # {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    # # assert values in the arguments
    # assert mempool_start_args[:transactions] == []
    # assert mempool_start_args[:round] == 1
    # assert mempool_start_args[:consensus] == []

    enode
  end

  # -----------------------------------------------------------
  # Storage

  @spec storage_args_fresh_node() :: Anoma.Node.Examples.ENode.t()
  @doc """
  I check whether the storage arguments for a fresh node are the default arguments.
  """
  @spec storage_args_fresh_node(ENode.t()) :: ENode.t()
  def storage_args_fresh_node(enode \\ ENode.start_node()) do
    # there should be 0 transactions, and the committed height should be 0.
    {:ok, storage_start_args} = State.storage_arguments(enode.node_id)

    assert storage_start_args == [uncommitted_height: 0]

    enode
  end

  @doc """
  I check whether the storage arguments for a fresh node are the default arguments.
  """
  @spec storage_args_non_fresh_node(ENode.t()) :: ENode.t()
  def storage_args_non_fresh_node(enode \\ ENode.start_node()) do
    # run ten separate transactions in a block through the node.
    EMempool.complete_ten_transactions(enode)

    # there should be 10 transactions, and the committed height should be 9.
    {:ok, storage_start_args} = State.storage_arguments(enode.node_id)

    assert storage_start_args == [uncommitted_height: 9]

    enode
  end

  @doc """
  I check whether the storage arguments are default when a transaction is added
  but not executed.
  """
  @spec storage_args_non_block_transaction(ENode.t()) :: ENode.t()
  def storage_args_non_block_transaction(enode \\ ENode.start_node()) do
    # run ten separate transactions in a block through the node.
    EMempool.add_transaction(enode)

    # there should be 10 transactions, and the committed height should be 0.
    {:ok, storage_start_args} = State.storage_arguments(enode.node_id)

    assert storage_start_args == [uncommitted_height: 0]

    enode
  end

  # -----------------------------------------------------------
  # Ordering

  @doc """
  I check whether the ordering arguments for a fresh node are the default arguments.
  """
  @spec ordering_args_fresh_node(ENode.t()) :: ENode.t()
  def ordering_args_fresh_node(enode \\ ENode.start_node()) do
    # there should be 0 transactions, and the committed height should be 0.
    {:ok, ordering_start_args} = State.ordering_arguments(enode.node_id)

    assert ordering_start_args == [next_height: 1]

    enode
  end

  @doc """
  I check whether the ordering arguments for a fresh node are the default arguments.
  """
  @spec ordering_args_non_fresh_node(ENode.t()) :: ENode.t()
  def ordering_args_non_fresh_node(enode \\ ENode.start_node()) do
    # run ten separate transactions in a block through the node.
    EMempool.complete_ten_transactions(enode)

    # there should be 10 transactions, and the committed height should be 9.
    {:ok, ordering_start_args} = State.ordering_arguments(enode.node_id)

    assert ordering_start_args == [next_height: 11]

    enode
  end
end
