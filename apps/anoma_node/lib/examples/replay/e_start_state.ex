defmodule Anoma.Node.Examples.EReplay.StartState do
  @moduledoc """
  I define examples on how the start state of a node is computed.
  """

  alias Anoma.Node.Examples.Mempool, as: EMempool
  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Tables
  alias Anoma.Node.Replay.State

  import ExUnit.Assertions

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

  # @spec mempool_args_empty_node(ENode.t()) :: ENode.t()
  def mempool_args_fresh_node(enode \\ ENode.start_node()) do
    # there should be 0 transactions
    {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    # assert values in the arguments
    assert mempool_start_args[:transactions] == []
    assert mempool_start_args[:round] == 0
    assert mempool_start_args[:consensus] == []

    enode
  end

  # @spec mempool_args_empty_node(ENode.t()) :: ENode.t()
  def mempool_args_non_fresh_node(enode \\ ENode.start_node()) do
    # run ten separate transactions in a block through the node.
    EMempool.complete_ten_transactions(enode)

    Process.sleep(1000)

    {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    # assert values in the arguments
    assert mempool_start_args[:transactions] == []
    assert mempool_start_args[:round] == 0
    assert mempool_start_args[:consensus] == []

    enode
  end

  @doc """
  I check whether the mempool arguments for a fresh node are the default arguments.
  """
  @spec mempool_args_non_block_transaction(ENode.t()) :: ENode.t()
  def mempool_args_non_block_transaction(enode \\ ENode.start_node()) do
    # run a transaction, but do not create a block
    # this will make sure the transaction is still present in the mempool's tables
    # and it should be restored.
    {_node, transaction} = EMempool.add_transaction(enode)

    Process.sleep(1000)

    {:ok, mempool_start_args} = State.mempool_arguments(enode.node_id)

    assert mempool_start_args[:transactions] == [
             {transaction.id, {transaction.backend, transaction.noun}}
           ]

    assert mempool_start_args[:round] == 0
    assert mempool_start_args[:consensus] == []

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
  I check whether the storage arguments for a fresh node are the default arguments.
  """
  @spec storage_args_non_block_transaction(ENode.t()) :: ENode.t()
  def storage_args_non_block_transaction(enode \\ ENode.start_node()) do
    # run ten separate transactions in a block through the node.
    EMempool.add_transaction(enode)

    Process.sleep(1000)

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
