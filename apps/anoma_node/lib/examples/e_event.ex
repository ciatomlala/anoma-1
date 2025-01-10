defmodule Anoma.Node.Examples.EEvent do
  @moduledoc """
  I contain logic to send node events and wait for node events.

  These events are not generic events such as apps/event_broker/lib/examples/e_event_broker.ex.
  Rather, these are specific node events.
  """

  alias Anoma.Node.Examples.ENode

  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Event
  alias Anoma.Node.Examples.ETransaction
  alias Anoma.Node.Transaction.Backends
  alias Anoma.Node.Transaction.Ordering
  alias Anoma.Node.Transaction.Executor

  import ExUnit.Assertions

  require Anoma.Node.Event

  ############################################################
  #                       Events                             #
  ############################################################
  # @doc """
  # I create a transaction event
  # """
  @spec transaction_event(
          ENode.t(),
          {Backends.backend(), Noun.t()} | nil,
          String.t() | nil
        ) ::
          EventBroker.Event.t()
  def transaction_event(enode \\ ENode.start_node(), tx \\ nil, id \\ nil) do
    # create a random transaction if none was given.
    {backend, noun} =
      if tx, do: tx, else: ETransaction.trivial_transparent_transaction()

    # create a random id if none was given.
    id = if id, do: id, else: ETransaction.random_transaction_id()

    # create a transaction event
    event = new_tx_event({backend, noun}, id)

    Event.new_with_body(enode.node_id, event)
  end

  @doc """
  I create a consensus event for the given transaction ids.
  """
  @spec consensus_event(ENode.t(), [String.t()]) :: EventBroker.Event.t()
  def consensus_event(enode \\ ENode.start_node(), transaction_ids \\ []) do
    # create a transaction event
    event = new_consensus_event(transaction_ids)

    Event.new_with_body(enode.node_id, event)
  end

  @doc """
  I create an order event for the given transaction id.
  """
  @spec order_event(ENode.t(), String.t() | nil) :: EventBroker.Event.t()
  def order_event(enode \\ ENode.start_node(), transaction_id \\ nil) do
    transaction_id =
      if transaction_id do
        transaction_id
      else
        ETransaction.random_transaction_id()
      end

    # create a transaction event
    event = new_order_event(transaction_id)

    Event.new_with_body(enode.node_id, event)
  end

  @doc """
  I create an execution event for the given transaction id and the given result.
  The transaction should be a tuple with an id and an expected result.
  E.g., {{:ok, [["key" | 0]]}, "id 1"}
        {[error: "id 1"], "id 1"}
  """
  @spec execution_event(ENode.t(), {any(), String.t()} | nil) ::
          EventBroker.Event.t()
  def execution_event(enode \\ ENode.start_node(), transaction \\ nil) do
    {transaction_id, transaction_result} =
      if transaction do
        transaction
      else
        {{:ok, [["key" | 0]]}, ETransaction.random_transaction_id()}
      end

    # create a transaction event
    event = new_execution_event([{transaction_id, transaction_result}])

    Event.new_with_body(enode.node_id, event)
  end

  ############################################################
  #                       Send Events                        #
  ############################################################
  @spec send_transaction_event(Anoma.Node.Examples.ENode.t()) ::
          {Anoma.Node.Examples.ENode.t(), EventBroker.Event.t()}
  @doc """
  I send the given transaction event.
  If no event was given, I send a default event.
  """
  @spec send_transaction_event(ENode.t(), EventBroker.Event.t() | nil) ::
          {ENode.t(), EventBroker.Event.t()}
  def send_transaction_event(enode \\ ENode.start_node(), event \\ nil) do
    # if no event was given, create a default event
    event = if event, do: event, else: transaction_event(enode)

    # send the event
    EventBroker.event(event)

    {enode, event}
  end

  @doc """
  I send the consensus event.
  If no event was given, I send a default event.
  """
  @spec send_consensus_event(ENode.t(), EventBroker.Event.t() | nil) ::
          {ENode.t(), EventBroker.Event.t()}
  def send_consensus_event(enode \\ ENode.start_node(), event \\ nil) do
    # if no event was given, create a default event
    event = if event, do: event, else: consensus_event(enode)

    # send the event
    EventBroker.event(event)

    {enode, event}
  end

  @doc """
  I send the order event.
  If no event was given, I send a default event.
  """
  @spec send_order_event(ENode.t(), EventBroker.Event.t() | nil) ::
          {ENode.t(), EventBroker.Event.t()}
  def send_order_event(enode \\ ENode.start_node(), event \\ nil) do
    # if no event was given, create a default event
    event = if event, do: event, else: order_event(enode)

    # send the event
    EventBroker.event(event)

    {enode, event}
  end

  @doc """
  I send the execution event.
  If no event was given, I send a default event.
  """
  @spec send_execution_event(ENode.t(), EventBroker.Event.t() | nil) ::
          {ENode.t(), EventBroker.Event.t()}
  def send_execution_event(enode \\ ENode.start_node(), event \\ nil) do
    # if no event was given, create a default event
    event = if event, do: event, else: execution_event(enode)

    # send the event
    EventBroker.event(event)

    {enode, event}
  end

  ############################################################
  #                       Wait For Events                    #
  ############################################################
  @doc """
  I wait for a specific event.
  """
  def wait_for_transaction_event(enode \\ ENode.start_node(), event \\ nil) do
    # subscribe to transaction events
    EventBroker.subscribe_me([])

    # if no event was given, create and send one now.
    event =
      if event == nil do
        {_node, event} = send_transaction_event(enode)
        event
      else
        event
      end

    # the event will be fired from another module,
    # so the source_mdoule attribute has to be ignored.
    expected_body = event.body

    assert_receive %EventBroker.Event{
                     body: ^expected_body,
                     source_module: _
                   },
                   1000
  end

  @doc """
  I wait for a specific consensus event.
  """
  def wait_for_consensus_event(enode \\ ENode.start_node(), event \\ nil) do
    # subscribe to transaction events
    EventBroker.subscribe_me([])

    # if no event was given, create and send one now.
    event =
      if event == nil do
        {_node, event} = send_consensus_event(enode)
        event
      else
        event
      end

    # the event will be fired from another module,
    # so the source_mdoule attribute has to be ignored.
    expected_body = event.body

    assert_receive %EventBroker.Event{
                     body: ^expected_body,
                     source_module: _
                   },
                   1000
  end

  @doc """
  I wait for a specific order event.
  """
  def wait_for_order_event(enode \\ ENode.start_node(), event \\ nil) do
    # subscribe to all events
    EventBroker.subscribe_me([])

    # if no event was given, create and send one now.
    event =
      if event == nil do
        {_node, event} = send_order_event(enode)
        event
      else
        event
      end

    # the event will be fired from another module,
    # so the source_mdoule attribute has to be ignored.
    expected_body = event.body

    assert_receive %EventBroker.Event{
                     body: ^expected_body,
                     source_module: _
                   },
                   1000
  end

  @doc """
  I wait for a specific execution event.
  """
  def wait_for_execution_event(enode \\ ENode.start_node(), event \\ nil) do
    # subscribe to all events
    EventBroker.subscribe_me([])

    # if no event was given, create and send one now.
    event =
      if event == nil do
        {_node, event} = send_execution_event(enode)
        event
      else
        event
      end

    # the event will be fired from another module,
    # so the source_mdoule attribute has to be ignored.
    expected_body = event.body

    assert_receive %EventBroker.Event{
                     body: ^expected_body,
                     source_module: _
                   },
                   1000
  end

  ############################################################
  #                       Helpers                            #
  ############################################################

  @doc """
  Given a transaction and an id, I create a transaction event.
  """
  @spec new_tx_event({Backends.backend(), Noun.t()}, String.t()) ::
          Mempool.TxEvent.t()
  def new_tx_event({backend, noun}, id) do
    %Mempool.TxEvent{
      id: id,
      tx: %Mempool.Tx{backend: backend, code: noun}
    }
  end

  @doc """
  I create a new consensus event.
  """
  @spec new_consensus_event([String.t()]) :: Mempool.ConsensusEvent.t()
  def new_consensus_event(transaction_ids) do
    %Mempool.ConsensusEvent{
      order: transaction_ids
    }
  end

  @doc """
  I create a new order event.
  """
  @spec new_order_event(String.t()) :: Ordering.OrderEvent.t()
  def new_order_event(transaction_id) do
    %Ordering.OrderEvent{
      tx_id: transaction_id
    }
  end

  @doc """
  I create a new execution event.
  I expect a list of tuples that contain a transaction id and a result of the execution
  eof that transaction.

  E.g., {"id 1", {:ok, [["key" | 0]]}}
        {"id 1", [error: "id 1"]}
  """
  @spec new_execution_event([{any(), String.t()}]) ::
          Executor.ExecutionEvent.t()
  def new_execution_event(results) do
    %Executor.ExecutionEvent{
      result: results
    }
  end
end
