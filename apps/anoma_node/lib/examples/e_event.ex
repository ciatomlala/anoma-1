defmodule Anoma.Node.Examples.EEvent do
  @moduledoc """
  I contain logic to send node events and wait for node events.

  These events are not generic events such as apps/event_broker/lib/examples/e_event_broker.ex.
  Rather, these are specific node events.
  """

  alias Anoma.Node.Examples.ENode

  alias Anoma.Node.Event
  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Event, as: NodeEvent
  alias Anoma.Node.Examples.ETransaction
  alias Anoma.Node.Transaction.Backends
  alias EventBroker.Event

  require Anoma.Node.Event, as: NodeEvent

  import ExUnit.Assertions

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
          Event.t()
  def transaction_event(enode \\ ENode.start_node(), tx \\ nil, id \\ nil) do
    # create a random transaction if none was given.
    {backend, noun} =
      if tx, do: tx, else: ETransaction.trivial_transparent_transaction()

    # create a random id if none was given.
    id = if id, do: id, else: ETransaction.random_transaction_id()

    # create a transaction event
    event = new_tx_event({backend, noun}, id)

    NodeEvent.new_with_body(enode.node_id, event)
  end

  ############################################################
  #                       Send Events                        #
  ############################################################
  @doc """
  I send the given transaction event.
  If no event was given, I send a default event.
  """
  @spec send_transaction_event(ENode.t(), Event.t() | nil) ::
          {ENode.t(), Event.t()}
  def send_transaction_event(enode \\ ENode.start_node(), event \\ nil) do
    # if no event was given, create a default event
    event = if event, do: event, else: transaction_event(enode)

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
end
