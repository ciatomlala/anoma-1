defmodule Anoma.Node.Examples.Mempool do
  @moduledoc """
  I contain examples on how to interact with the mempool.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Examples.ETransaction
  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Examples.EEvent

  import ExUnit.Assertions

  # -----------------------------------------------------------
  # Adding transactions

  @doc """
  I add a transaction to the mempool.
  """
  @spec add_transaction(ENode.t()) ::
          {ENode.t(), ETransaction.t()}
  @spec add_transaction(ENode.t(), ETransaction.t()) ::
          {ENode.t(), ETransaction.t()}
  # default arguments
  def add_transaction(enode \\ ENode.start_node()) do
    transaction = ETransaction.simple_transaction()
    add_transaction(enode, transaction)
  end

  def add_transaction(enode, transaction) do
    # submit the transaction to the mempool.
    Mempool.tx(
      enode.node_id,
      {transaction.backend, transaction.noun},
      transaction.id
    )

    # assert that the transaction is in the mempool.
    # note: we cannot assert that it is the only transaction, because
    # this example is reused below.
    transactions = Mempool.tx_dump(enode.node_id)

    assert transaction.id in transactions

    {enode, transaction}
  end

  @doc """
  I add a transaction to the mempool that errors when executed.
  """
  @spec add_error_transaction(ENode.t()) ::
          {ENode.t(), ETransaction.t()}

  @spec add_error_transaction(ENode.t(), ETransaction.t()) ::
          {ENode.t(), ETransaction.t()}
  def add_error_transaction(enode \\ ENode.start_node()) do
    transaction = ETransaction.faulty_transaction()
    add_error_transaction(enode, transaction)
  end

  def add_error_transaction(enode, transaction) do
    add_transaction(enode, transaction)
  end

  @doc """
  I add multiple transactions to the mempool.
  """
  @spec add_multiple_transactions(ENode.t()) ::
          {ENode.t(), [ETransaction.t()]}
  @spec add_multiple_transactions(ENode.t(), [ETransaction.t()]) ::
          {ENode.t(), [ETransaction.t()]}

  def add_multiple_transactions(enode \\ ENode.start_node()) do
    transactions =
      Enum.map(1..10, fn _ -> ETransaction.simple_transaction() end)

    add_multiple_transactions(enode, transactions)
  end

  def add_multiple_transactions(enode, transactions) do
    # insert `transaction_count` transactions in the mempool
    Enum.each(transactions, &add_transaction(enode, &1))

    # assert all the transactions are in the mempool.
    mempool_transactions = Mempool.tx_dump(enode.node_id)

    for transaction <- transactions do
      assert transaction.id in mempool_transactions
    end

    {enode, transactions}
  end

  # -----------------------------------------------------------
  # Executing transactions

  @doc """
  I add a transaction to the mempool that executes properly.
  I execute this transaction.
  """
  @spec execute_transaction(ENode.t(), ETransaction.t()) ::
          {ENode.t(), ETransaction.t()}

  def execute_transaction(enode \\ ENode.start_node()) do
    transaction = ETransaction.faulty_transaction()
    execute_transaction(enode, transaction)
  end

  def execute_transaction(enode, transaction) do
    # subscribe to events here to be sure the tx events are caught
    EventBroker.subscribe_me([])

    # add the transaction to the mempool
    {_enode, _transaction} = add_transaction(enode, transaction)

    # adding a transaction to the mempool has two observable effects.
    # - a transaction event should be fired
    # - there should be a new transaction task running in the dynanamic observer.

    # check that the event has been fired
    event = EEvent.transaction_event(enode, transaction)
    EEvent.wait_for_transaction_event(enode, event)

    {enode, transaction}
  end

  @doc """
  I add a transaction to the mempool that fails when executed.
  I execute this transaction.
  """
  @spec execute_multiple_transactions(ENode.t()) ::
          {ENode.t(), [ETransaction.t()]}

  def execute_multiple_transactions(enode \\ ENode.start_node()) do
    transactions =
      Enum.map(1..10, fn _ -> ETransaction.simple_transaction() end)

    execute_multiple_transactions(enode, transactions)
  end

  def execute_multiple_transactions(enode, transactions) do
    # subscribe to events here to be sure the tx events are caught
    EventBroker.subscribe_me([])

    # add the transaction to the mempool
    {enode, transactions} = add_multiple_transactions(enode, transactions)

    # adding a transaction to the mempool has two observable effects.
    # - a transaction event should be fired
    # - there should be a new transaction task running in the dynanamic observer.

    # check that the event has been fired for each transaction
    for transaction <- transactions do
      event = EEvent.transaction_event(enode, transaction)
      EEvent.wait_for_transaction_event(enode, event)
    end

    {enode, transactions}
  end

  # -----------------------------------------------------------
  # Blocks

  @doc """
  I run a transaction and let it complete.
  I expect a transaction description with the following values:
   - {backend, noun}: the transaction and noun
   - The expected result of executing the transaction
     E.g., {:ok, {:read_value, [["key" | 0] | 0]}}
   - The id of the transaction
  """
  @spec complete_transaction(
          ENode.t(),
          ETransaction.t()
        ) :: {ENode.t(), ETransaction.t()}
  def complete_transaction(enode \\ ENode.start_node()) do
    transaction = ETransaction.faulty_transaction()
    complete_transaction(enode, transaction)
  end

  def complete_transaction(enode, transaction) do
    # subscribe to events here to be sure the events are caught
    EventBroker.subscribe_me([])

    # fire a transaction
    {_node, _transaction} = execute_transaction(enode, transaction)

    # the transaction is currently waiting for an ordering
    # or it has already executed if it did not scry.
    #
    # to ensure that the transaction completes, a consensus event
    # must be fired. This is done by the consensus engine
    # by calling Mempool.execute(node, transaction_ids)
    # there is no consensus in the current branch, so the call is done manually
    #
    # The Mempool.execute call will fire a consensus event
    # and then call the executor to execute the transactions.
    #
    # The executor will order the transactions in the consensus
    # and then wait for all transactions to complete.
    # After this, an execution event is sent.
    Mempool.execute(enode.node_id, [transaction.id])

    # to verify that the transaction completed, n observable effects
    # must be assertd.
    # - consensus event is fired
    # - order event is fired
    # - execution event is fired

    # wait for the consensus event
    consensus_event = EEvent.consensus_event(enode, [transaction.id])
    EEvent.wait_for_consensus_event(enode, consensus_event)

    # wait for the order event
    order_event = EEvent.order_event(enode, transaction.id)
    EEvent.wait_for_order_event(enode, order_event)

    # wait for the execution event

    execution_event = EEvent.execution_event(enode, transaction)
    EEvent.wait_for_execution_event(enode, execution_event)

    {enode, transaction}
  end
end
