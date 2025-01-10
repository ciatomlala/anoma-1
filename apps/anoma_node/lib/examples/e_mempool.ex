defmodule Anoma.Node.Examples.Mempool do
  @moduledoc """
  I contain examples on how to interact with the mempool.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Examples.ETransaction
  alias Anoma.Node.Transaction.Backends
  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Registry
  alias Anoma.Node.Examples.EEvent

  import ExUnit.Assertions

  @type transaction :: {Backends.backend(), Noun.t()}
  @type transaction_id :: String.t()

  # -----------------------------------------------------------
  # Adding transactions

  @doc """
  I add a transaction to the mempool.
  """
  @spec add_transaction(ENode.t()) ::
          {ENode.t(), {transaction, transaction_id}}
  def add_transaction(enode \\ ENode.start_node()) do
    # create a transaction
    transaction_id = ETransaction.random_transaction_id()
    transaction = ETransaction.trivial_transparent_transaction()

    # submit the transaction to the mempool.
    Mempool.tx(enode.node_id, transaction, transaction_id)

    # assert that the transaction is in the mempool.
    # note: we cannot assert that it is the only transaction, because
    # this example is reused below.
    transactions = Mempool.tx_dump(enode.node_id)

    assert transaction_id in transactions

    {enode, {transaction, transaction_id}}
  end

  @doc """
  I add a transaction to the mempool that errors when executed.

  """
  @spec add_error_transaction(ENode.t()) ::
          {ENode.t(), {transaction, transaction_id}}
  def add_error_transaction(enode \\ ENode.start_node()) do
    # create a transaction
    transaction_id = ETransaction.random_transaction_id()
    transaction = ETransaction.faulty_transaction()

    # submit the transaction to the mempool.
    Mempool.tx(enode.node_id, transaction, transaction_id)

    # assert that the transaction is in the mempool.
    # note: we cannot assert that it is the only transaction, because
    # this example is reused below.
    transactions = Mempool.tx_dump(enode.node_id)

    assert transaction_id in transactions

    {enode, {transaction, transaction_id}}
  end

  @doc """
  I add multiple transactions to the mempool.
  """
  @spec add_multiple_transactions(ENode.t()) ::
          {ENode.t(), [{transaction, transaction_id}]}
  def add_multiple_transactions(enode \\ ENode.start_node()) do
    transaction_count = 10

    # insert `transaction_count` transactions in the mempool
    transactions =
      1..transaction_count
      |> Enum.reduce([], fn _, transactions ->
        {_, {transaction, transaction_id}} = add_transaction(enode)
        [{transaction, transaction_id} | transactions]
      end)

    # assert all the transactions are in the mempool.
    mempool_transactions = Mempool.tx_dump(enode.node_id)

    for {_, transaction_id} <- transactions do
      assert transaction_id in mempool_transactions
    end

    {enode, transactions}
  end

  # -----------------------------------------------------------
  # Executing transactions

  @doc """
  I add a transaction to the mempool that fails when executed.
  I execute this transaction.
  """
  @spec execute_error_transaction(ENode.t()) ::
          {ENode.t(), {transaction, transaction_id}}
  def execute_error_transaction(enode \\ ENode.start_node()) do
    # subscribe to events here to be sure the tx events are caught
    EventBroker.subscribe_me([])

    # count the current launched transactions
    tx_count = launched_transactions_count(enode)

    # add the transaction to the mempool
    {enode, {transaction, transaction_id}} = add_error_transaction(enode)

    # adding a transaction to the mempool has two observable effects.
    # - a transaction event should be fired
    # - there should be a new transaction task running in the dynanamic observer.

    # check that the event has been fired
    event = EEvent.transaction_event(enode, transaction, transaction_id)
    EEvent.wait_for_transaction_event(enode, event)

    # assert there is a task running for this transaction
    assert launched_transactions_count(enode) == tx_count + 1

    {enode, {transaction, transaction_id}}
  end

  @doc """
  I add a transaction to the mempool that fails when executed.
  I execute this transaction.
  """
  @spec execute_multiple_transactions(ENode.t()) ::
          {ENode.t(), [{transaction, transaction_id}]}
  def execute_multiple_transactions(enode \\ ENode.start_node()) do
    # subscribe to events here to be sure the tx events are caught
    EventBroker.subscribe_me([])

    # count the current launched transactions
    tx_count = launched_transactions_count(enode)

    # add the transaction to the mempool
    {enode, transactions} = add_multiple_transactions(enode)

    # adding a transaction to the mempool has two observable effects.
    # - a transaction event should be fired
    # - there should be a new transaction task running in the dynanamic observer.

    # check that the event has been fired for each transaction
    for {transaction, transaction_id} <- transactions do
      event = EEvent.transaction_event(enode, transaction, transaction_id)
      EEvent.wait_for_transaction_event(enode, event)
    end

    # assert there is a task running for each running transaction
    assert launched_transactions_count(enode) ==
             tx_count + Enum.count(transactions)

    {enode, transactions}
  end

  # -----------------------------------------------------------
  # Blocks

  def complete_transaction(enode \\ ENode.start_node()) do
    # fire a transaction
    {_node, transaction} = execute_error_transaction(enode)
  end

  ############################################################
  #                       Helpers                            #
  ############################################################

  # @doc """
  # I return the amount of transactions currently running.
  # """
  @spec launched_transactions_count(ENode.t()) :: non_neg_integer()
  defp launched_transactions_count(enode) do
    tx_supervisor = Registry.via(enode.node_id, TxSupervisor)
    Enum.count(Task.Supervisor.children(tx_supervisor))
  end
end
