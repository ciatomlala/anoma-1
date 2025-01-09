defmodule Anoma.Node.Examples.Mempool do
  @moduledoc """
  I contain examples on how to interact with the mempool.
  """

  alias Anoma.Node.Examples.ETransaction
  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Examples.ENode

  import ExUnit.Assertions

  @type transaction :: {Backends.backend(), Noun.t()}
  @type transaction_id :: String.t()

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
  I add a transaction to the mempool that fails when executed.
  """
  @spec add_faulty_transaction(ENode.t()) ::
          {ENode.t(), {transaction, transaction_id}}
  def add_faulty_transaction(enode \\ ENode.start_node()) do
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
end
