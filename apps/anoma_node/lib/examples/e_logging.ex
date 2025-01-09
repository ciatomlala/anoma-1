defmodule Anoma.Node.Examples.ELogging do
  @moduledoc """
  I define examples that test the behavior of the logging engine.

  These examples do not use the actual system of transactions.
  They only send events, and test the events that are a result of them.
  """

  alias Anoma.Node.Event
  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Tables
  alias Anoma.Node.Transaction.Mempool

  require Anoma.Node.Event
  require ExUnit.Assertions

  import ExUnit.Assertions

  use EventBroker.WithSubscription

  @doc """
  I test the events from the mnesia tables by creating an event
  and asserting I receive it.
  """
  @spec check_tx_event(ENode.t()) :: {ENode.t(), String.t()}
  def check_tx_event(enode \\ ENode.start_node()) do
    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    # create and fire a random event
    {event_1, id_1, code_1, backend_1} = random_tx_event(enode.node_id)
    EventBroker.event(event_1)

    assert_receive(
      {:mnesia_table_event,
       {:write, {^events_table, ^id_1, {^backend_1, ^code_1}}, _}},
      5000
    )

    assert {:atomic, [{^events_table, ^id_1, {^backend_1, ^code_1}}]} =
             :mnesia.transaction(fn ->
               :mnesia.read(events_table, id_1)
             end)

    :mnesia.unsubscribe({:table, events_table, :simple})
    flush_mailbox!()

    {enode, id_1}
  end

  @doc """
  I test whether multiple transactions events are received properly.
  """
  @spec check_multiple_tx_events(ENode.t()) :: {ENode.t(), [String.t()]}
  def check_multiple_tx_events(enode \\ ENode.start_node()) do
    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    ids =
      for i <- 1..5 do
        {event, id, code, backend} = random_tx_event(enode.node_id)
        EventBroker.event(event)

        # assert that the event is received
        assert_receive(
          {:mnesia_table_event,
           {:write, {^events_table, ^id, {^backend, ^code}}, _}},
          5000
        )

        id
      end

    # unsubscribe from mnesia
    :mnesia.unsubscribe({:table, events_table, :simple})
    flush_mailbox!()

    {enode, ids}
  end

  ############################################################
  #                      Consensus event                     #
  ############################################################

  @doc """
  I fire a consensus event and test whether a notification is sent
  of that exact event.
  """
  @spec check_consensus_event(ENode.t()) :: {ENode.t(), String.t()}
  def check_consensus_event(enode \\ ENode.start_node()) do
    # fire events using a previous example
    {_node, event_id} = check_tx_event(enode)

    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    # fire a consensus event
    consensus_event = consensus_event(enode.node_id, [event_id])
    EventBroker.event(consensus_event)

    # assert that a mnesia table event is fired
    assert_receive(
      {:mnesia_table_event,
       {:write, {^events_table, :consensus, [[^event_id]]}, _}},
      5000
    )

    # unsubscribe from mnesia, test is done
    :mnesia.unsubscribe({:table, events_table, :simple})
    flush_mailbox!()

    # assert that the consensus in the events table contains the order
    assert {:atomic, [{^events_table, :consensus, [[^event_id]]}]} =
             :mnesia.transaction(fn ->
               :mnesia.read(events_table, :consensus)
             end)

    {enode, event_id}
  end

  @doc """
  I fire multiple consensus events and test whether a notification is sent
  of those exact events.
  """
  @spec check_consensus_event_multiple(ENode.t()) :: {ENode.t(), [String.t()]}
  def check_consensus_event_multiple(enode \\ ENode.start_node()) do
    {_node, event_ids} = check_multiple_tx_events(enode)

    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    # fire two consensus events
    for event_id <- event_ids do
      consensus_event = consensus_event(enode.node_id, [event_id])
      EventBroker.event(consensus_event)
    end

    # assert that a table event is fired for each consesus event
    # turn [a, b] into [[a], [b]]
    consensi =
      Enum.map(event_ids, &List.wrap/1)

    # event for the first consensus
    first_consensus =
      Enum.take(consensi, 1)

    assert_receive(
      {:mnesia_table_event,
       {:write, {^events_table, :consensus, ^first_consensus}, _}},
      5000
    )

    # event after second consensus
    assert_receive(
      {:mnesia_table_event,
       {:write, {^events_table, :consensus, ^consensi}, _}},
      5000
    )

    # unsubscribe from mnesia events
    :mnesia.unsubscribe({:table, events_table, :simple})
    flush_mailbox!()

    assert {:atomic, [{^events_table, :consensus, ^consensi}]} =
             :mnesia.transaction(fn ->
               :mnesia.read(events_table, :consensus)
             end)

    {enode, event_ids}
  end

  ############################################################
  #                         Block event                      #
  ############################################################

  @doc """
  I create a transaction, and then mint it into a block.
  I check that there is no consensus left, and that there are no
  transactions left.
  """
  @spec check_block_event(ENode.t()) :: {ENode.t(), [String.t()]}
  def check_block_event(enode \\ ENode.start_node()) do
    {_node, event_id} = check_consensus_event(enode)

    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    # fire a block event
    block_event = block_event(enode.node_id, [event_id], 0)
    EventBroker.event(block_event)

    # assert that mnesia tells us the event has been deleted
    assert_receive(
      {:mnesia_table_event, {:delete, {^events_table, ^event_id}, _}},
      5000
    )

    # unsubscribe from mnesia events
    :mnesia.unsubscribe({:table, events_table, :simple})

    # assert that there are no waiting consensi in the tables
    assert {:atomic, [{^events_table, :consensus, []}]} =
             :mnesia.transaction(fn ->
               :mnesia.read(events_table, :consensus)
             end)

    # assert that there are no pending transactions
    assert {:atomic, []} =
             :mnesia.transaction(fn ->
               :mnesia.read(events_table, event_id)
             end)

    # unsubscribe from mnesia events
    :mnesia.unsubscribe({:table, events_table, :simple})
    flush_mailbox!()

    {enode, event_id}
  end

  @doc """
  I create a few transactions, and then mint them into a block.
  I check that there is no consensus left, and that there are no
  transactions left.
  """
  @spec check_block_event_all(ENode.t()) :: {ENode.t(), [String.t()]}
  def check_block_event_all(enode \\ ENode.start_node()) do
    {_node, event_ids} = check_consensus_event_multiple(enode)

    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    # iterate over the list of events with a scan
    # e.g., ["id1", "id2"] => [[["id1"], ["id2"]], [["id2"]]]
    consensi =
      event_ids
      |> Enum.reverse()
      |> Enum.scan([], &([[&1]] ++ &2))
      |> Enum.reverse()

    for consensi <- consensi do
      [[event_id] | event_ids] = consensi

      # fire a block event
      block_event = block_event(enode.node_id, [event_id], 0)
      EventBroker.event(block_event)

      # assert that the transaction is removed from the table
      assert_receive(
        {:mnesia_table_event, {:delete, {^events_table, ^event_id}, _}},
        5000
      )

      # assert only the remainder of the transactions is in a consensus
      assert {:atomic, [{^events_table, :consensus, ^event_ids}]} =
               :mnesia.transaction(fn ->
                 :mnesia.read(events_table, :consensus)
               end)
    end

    # unsubscribe from mnesia events
    :mnesia.unsubscribe({:table, events_table, :simple})
    flush_mailbox!()

    {enode, event_ids}
  end

  @doc """
  I create a few transactions, and then mint them all but one into a block.
  I check that there is a single consensus left, and that there is a single
  transaction left.
  """
  @spec check_block_event_subset(ENode.t()) ::
          {ENode.t(), [String.t()], [String.t()]}
  def check_block_event_subset(enode \\ ENode.start_node()) do
    {_node, event_ids} = check_consensus_event_multiple(enode)

    # the last transaction is not put into a block
    # this should keep it in consensus after this test
    keep_event_ids = Enum.take(event_ids, -1)
    event_ids = Enum.drop(event_ids, -1)

    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    # iterate over the list of events with a scan
    # e.g., [id1], [[id1], [id2]], ..
    consensi =
      event_ids
      |> Enum.reverse()
      |> Enum.scan([keep_event_ids], &([[&1]] ++ &2))
      |> Enum.reverse()

    for consensi <- consensi do
      [[event_id] | event_ids] = consensi

      # fire a block event
      block_event = block_event(enode.node_id, [event_id], 0)
      EventBroker.event(block_event)

      # assert that the transaction is removed from the table
      assert_receive(
        {:mnesia_table_event, {:delete, {^events_table, ^event_id}, _}},
        5000
      )

      # assert only the remainder of the transactions is in a consensus
      assert {:atomic, [{^events_table, :consensus, ^event_ids}]} =
               :mnesia.transaction(fn ->
                 :mnesia.read(events_table, :consensus)
               end)
    end

    # unsubscribe from mnesia events
    :mnesia.unsubscribe({:table, events_table, :simple})
    flush_mailbox!()

    {enode, keep_event_ids, event_ids}
  end

  # @spec replay_corrects_result(String.t()) :: String.t()
  # def replay_corrects_result(node_id \\ Node.example_random_id()) do
  #   replay_ensure_created_tables(node_id)
  #   table = Storage.blocks_table(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write(
  #       {table, 0, [%Mempool.Tx{backend: :debug_bloblike, code: "code 1"}]}
  #     )
  #   end)

  #   write_consensus_leave_one_out(node_id)
  #   filter = [%Mempool.TxFilter{}]

  #   with_subscription [filter] do
  #     Logging.restart_with_replay(node_id)

  #     :ok =
  #       wait_for_tx(node_id, "id 2", "code 2")

  #     :error_tx =
  #       wait_for_tx(node_id, "id 1", "code 1")
  #   end

  #   state = Anoma.Node.Registry.whereis(node_id, Mempool) |> :sys.get_state()
  #   nil = Map.get(state.transactions, "id 1")
  #   1 = state.round

  #   node_id
  # end

  # @spec replay_consensus_leave_one_out(String.t()) :: String.t()
  # def replay_consensus_leave_one_out(node_id \\ Node.example_random_id()) do
  #   write_consensus_leave_one_out(node_id)
  #   replay_ensure_created_tables(node_id)

  #   filter = [%Mempool.TxFilter{}]

  #   with_subscription [filter] do
  #     Logging.restart_with_replay(node_id)

  #     :ok =
  #       wait_for_tx(node_id, "id 1", "code 1")

  #     :ok =
  #       wait_for_tx(node_id, "id 2", "code 2")

  #     :ok =
  #       wait_for_consensus(node_id, ["id 1"])

  #     Mempool.execute(node_id, ["id 2"])

  #     :ok =
  #       wait_for_consensus(node_id, ["id 2"])

  #     node_id
  #   end
  # end

  # @spec replay_several_consensus(String.t()) :: String.t()
  # def replay_several_consensus(node_id \\ Node.example_random_id()) do
  #   write_several_consensus(node_id)
  #   replay_ensure_created_tables(node_id)

  #   txfilter = [%Mempool.TxFilter{}]
  #   consensus_filter = [%Mempool.ConsensusFilter{}]

  #   with_subscription [txfilter, consensus_filter] do
  #     Logging.restart_with_replay(node_id)

  #     :ok =
  #       wait_for_tx(node_id, "id 1", "code 1")

  #     :ok =
  #       wait_for_tx(node_id, "id 2", "code 2")

  #     :ok =
  #       wait_for_consensus(node_id, ["id 1"])

  #     :ok =
  #       wait_for_consensus(node_id, ["id 2"])

  #     node_id
  #   end
  # end

  # @spec replay_consensus_with_several_txs(String.t()) :: String.t()
  # def replay_consensus_with_several_txs(node_id \\ Node.example_random_id()) do
  #   write_consensus_with_several_tx(node_id)
  #   replay_ensure_created_tables(node_id)

  #   txfilter = [%Mempool.TxFilter{}]
  #   consensus_filter = [%Mempool.ConsensusFilter{}]

  #   with_subscription [txfilter, consensus_filter] do
  #     Logging.restart_with_replay(node_id)

  #     :ok =
  #       wait_for_tx(node_id, "id 1", "code 1")

  #     :ok =
  #       wait_for_tx(node_id, "id 2", "code 2")

  #     :ok =
  #       wait_for_consensus(node_id, ["id 1", "id 2"])

  #     node_id
  #   end
  # end

  # @spec replay_consensus(String.t()) :: String.t()
  # def replay_consensus(node_id \\ Node.example_random_id()) do
  #   write_consensus(node_id)
  #   replay_ensure_created_tables(node_id)

  #   txfilter = [%Mempool.TxFilter{}]
  #   consensus_filter = [%Mempool.ConsensusFilter{}]

  #   with_subscription [txfilter, consensus_filter] do
  #     Logging.restart_with_replay(node_id)

  #     :ok =
  #       wait_for_tx(node_id, "id 1", "code 1")

  #     :ok =
  #       wait_for_consensus(node_id, ["id 1"])

  #     node_id
  #   end
  # end

  # @spec replay_several_txs(String.t()) :: String.t()
  # def replay_several_txs(node_id \\ Node.example_random_id()) do
  #   write_several_tx(node_id)
  #   replay_ensure_created_tables(node_id)

  #   txfilter = [%Mempool.TxFilter{}]

  #   with_subscription [txfilter] do
  #     Logging.restart_with_replay(node_id)

  #     :ok =
  #       wait_for_tx(node_id, "id 1", "code 1")

  #     :ok =
  #       wait_for_tx(node_id, "id 2", "code 2")

  #     node_id
  #   end
  # end

  # @spec replay_tx(String.t()) :: String.t()
  # def replay_tx(node_id \\ Node.example_random_id()) do
  #   write_tx(node_id)
  #   replay_ensure_created_tables(node_id)

  #   txfilter = [%Mempool.TxFilter{}]

  #   with_subscription [txfilter] do
  #     {:ok, _pid} = Logging.restart_with_replay(node_id)

  #     :ok =
  #       wait_for_tx(node_id, "id 1", "code 1")

  #     node_id
  #   end
  # end

  # @spec write_consensus_leave_one_out(String.t()) :: atom()
  # defp write_consensus_leave_one_out(node_id) do
  #   table = write_several_tx(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write({table, :consensus, [["id 1"]]})
  #   end)

  #   table
  # end

  # @spec write_several_consensus(String.t()) :: atom()
  # defp write_several_consensus(node_id) do
  #   table = write_several_tx(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write({table, :consensus, [["id 1"], ["id 2"]]})
  #   end)

  #   table
  # end

  # @spec write_consensus_with_several_tx(String.t()) :: atom()
  # defp write_consensus_with_several_tx(node_id) do
  #   table = write_several_tx(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write({table, :consensus, [["id 1", "id 2"]]})
  #   end)

  #   table
  # end

  # @spec write_consensus(String.t()) :: atom()
  # def write_consensus(node_id) do
  #   table = write_tx(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write({table, :consensus, [["id 1"]]})
  #   end)

  #   table
  # end

  # @spec write_several_tx(String.t()) :: atom()
  # defp write_several_tx(node_id) do
  #   table = create_event_table(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write({table, "id 1", {:debug_bloblike, "code 1"}})
  #     :mnesia.write({table, "id 2", {:debug_bloblike, "code 2"}})
  #   end)

  #   table
  # end

  # @spec write_tx(String.t()) :: atom()
  # defp write_tx(node_id) do
  #   table = create_event_table(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write({table, "id 1", {:debug_bloblike, "code 1"}})
  #   end)

  #   table
  # end

  # @spec wait_for_consensus(String.t(), list(binary())) ::
  #         :ok | :error_consensus
  # defp wait_for_consensus(node_id, consensus) do
  #   receive do
  #     %EventBroker.Event{
  #       body: %Node.Event{
  #         node_id: ^node_id,
  #         body: %Mempool.ConsensusEvent{
  #           order: ^consensus
  #         }
  #       }
  #     } ->
  #       :ok
  #   after
  #     1000 -> :error_consensus
  #   end
  # end

  # @spec wait_for_tx(String.t(), binary(), Noun.t()) :: :ok | :error_tx
  # defp wait_for_tx(node_id, id, code) do
  #   receive do
  #     %EventBroker.Event{
  #       body: %Node.Event{
  #         node_id: ^node_id,
  #         body: %Mempool.TxEvent{
  #           id: ^id,
  #           tx: %Mempool.Tx{backend: _, code: ^code}
  #         }
  #       }
  #     } ->
  #       :ok
  #   after
  #     1000 -> :error_tx
  #   end
  # end

  ############################################################
  #                       Private Helpers                    #
  ############################################################

  @doc """
  I flush the current processes' mailbox to ensure its empty.
  """
  def flush_mailbox!() do
    receive do
      _ -> flush_mailbox!()
    after
      0 ->
        :ok
    end
  end

  @doc """
  I create a random transaction event.
  """
  @spec random_tx_event(String.t(), atom()) ::
          {EventBroker.Event.t(), String.t(), String.t(), atom()}
  def random_tx_event(node_id, backend \\ :transparent_resource) do
    id = "#{:erlang.phash2(make_ref())}"
    code = "#{:erlang.phash2(make_ref())}"

    event =
      Event.new_with_body(node_id, %Mempool.TxEvent{
        id: id,
        tx: %Mempool.Tx{backend: backend, code: code}
      })

    {event, id, code, backend}
  end

  @doc """
  I create a random consensus event.
  """
  @spec consensus_event(String.t(), [binary()]) :: EventBroker.Event.t()
  def consensus_event(node_id, order) do
    Event.new_with_body(node_id, %Mempool.ConsensusEvent{
      order: order
    })
  end

  @doc """
  I create a random block event
  """
  @spec block_event(String.t(), [String.t()], non_neg_integer()) ::
          EventBroker.Event.t()
  def block_event(node_id, order, round) do
    Event.new_with_body(node_id, %Mempool.BlockEvent{
      order: order,
      round: round
    })
  end
end
