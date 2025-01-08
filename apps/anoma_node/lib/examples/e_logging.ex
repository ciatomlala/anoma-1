defmodule Anoma.Node.Examples.ELogging do
  @moduledoc """
  I define examples that test the behavior of the logging engine.
  """

  alias Anoma.Node.Event
  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Tables
  alias Anoma.Node.Transaction.Backends
  alias Anoma.Node.Transaction.Mempool

  # alias Anoma.Node.Logging
  # alias Anoma.Node.Transaction.Storage

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

    {enode, id_1}
  end

  @doc """
  I test whether multiple transactions events are received properly.
  """
  @spec check_multiple_tx_events(ENode.t()) :: ENode.t()
  def check_multiple_tx_events(enode \\ ENode.start_node()) do
    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    {event_1, id_1, code_1, backend_1} = random_tx_event(enode.node_id)
    EventBroker.event(event_1)

    {event_2, id_2, code_2, backend_2} = random_tx_event(enode.node_id)
    EventBroker.event(event_2)

    # assert that the event is received
    assert_receive(
      {:mnesia_table_event,
       {:write, {^events_table, ^id_1, {^backend_1, ^code_1}}, _}},
      5000
    )

    # assert that the event is received
    assert_receive(
      {:mnesia_table_event,
       {:write, {^events_table, ^id_2, {^backend_2, ^code_2}}, _}},
      5000
    )

    :mnesia.unsubscribe({:table, events_table, :simple})

    enode
  end

  ############################################################
  #                      Consensus event                     #
  ############################################################

  @spec check_consensus_event(ENode.t()) :: ENode.t()
  def check_consensus_event(enode \\ ENode.start_node()) do
    # fire events using a previous example
    check_tx_event(enode)

    # subscribe to events coming from the events table
    events_table = Tables.table_events(enode.node_id)
    :mnesia.subscribe({:table, events_table, :simple})

    consensus_event(["id 1"], enode.node_id)

    assert_receive(
      {:mnesia_table_event,
       {:write, {^events_table, :consensus, [["id 1"]]}, _}},
      5000
    )

    :mnesia.unsubscribe({:table, events_table, :simple})

    assert {:atomic, [{^events_table, :consensus, [["id 1"]]}]} =
             :mnesia.transaction(fn ->
               :mnesia.read(events_table, :consensus)
             end)

    enode
  end

  @doc """
  I fire a consensus event and test whether a notification is sent
  of that exact event.
  """

  # @spec check_consensus_event(ENode.t()) :: ENode.t()
  # def check_consensus_event(enode \\ ENode.start_node()) do
  #   # subscribe to events coming from the events table
  #   events_table = Tables.table_events(enode.node_id)
  #   :mnesia.subscribe({:table, events_table, :simple})

  #   # fire an event using previous example
  #   {_node, event_id} = check_tx_event(enode)

  #   assert_receive(
  #     {:mnesia_table_event,
  #      {:write, {^events_table, :consensus, [[^event_id]]}, _}},
  #     5000
  #   )

  #   :mnesia.unsubscribe({:table, events_table, :simple})

  #   assert {:atomic, [{^events_table, :consensus, [["id 1"]]}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(events_table, :consensus)
  #            end)

  #   enode
  # end

  # @spec check_tx_event(String.t()) :: String.t()
  # def check_tx_event(node_id \\ Node.example_random_id()) do
  #   ENode.start_node(node_id: node_id)
  #   table_name = Tables.table_events(node_id)

  #   :mnesia.subscribe({:table, table_name, :simple})

  #   tx_event("id 1", :transparent_resource, "code 1", node_id)

  #   assert_receive(
  #     {:mnesia_table_event,
  #      {:write, {_, "id 1", {:transparent_resource, "code 1"}}, _}},
  #     5000
  #   )

  #   assert {:atomic,
  #           [{^table_name, "id 1", {:transparent_resource, "code 1"}}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, "id 1")
  #            end)

  #   :mnesia.unsubscribe({:table, table_name, :simple})
  #   node_id
  # end

  # @spec check_consensus_event_multiple(String.t()) :: String.t()
  # def check_consensus_event_multiple(
  #       node_id \\ Node.example_random_id()
  #       |> Base.url_encode64()
  #     ) do
  #   check_multiple_tx_events(node_id)
  #   table_name = Tables.table_events(node_id)

  #   :mnesia.subscribe({:table, table_name, :simple})

  #   consensus_event(["id 1"], node_id)
  #   consensus_event(["id 2"], node_id)

  #   assert_receive(
  #     {:mnesia_table_event,
  #      {:write, {^table_name, :consensus, [["id 1"], ["id 2"]]}, _}},
  #     5000
  #   )

  #   :mnesia.unsubscribe({:table, table_name, :simple})

  #   assert {:atomic, [{^table_name, :consensus, [["id 1"], ["id 2"]]}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, :consensus)
  #            end)

  #   node_id
  # end

  # ############################################################
  # #                         Block event                      #
  # ############################################################

  # @spec check_block_event(String.t()) :: String.t()
  # def check_block_event(
  #       node_id \\ Node.example_random_id()
  #       |> Base.url_encode64()
  #     ) do
  #   check_consensus_event(node_id)
  #   table_name = Tables.table_events(node_id)

  #   :mnesia.subscribe({:table, table_name, :simple})

  #   block_event(["id 1"], 0, node_id)

  #   assert_receive(
  #     {:mnesia_table_event, {:delete, {^table_name, "id 1"}, _}},
  #     5000
  #   )

  #   :mnesia.unsubscribe({:table, table_name, :simple})

  #   assert {:atomic, [{^table_name, :consensus, []}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, :consensus)
  #            end)

  #   assert {:atomic, []} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, "id 1")
  #            end)

  #   node_id
  # end

  # @spec check_block_event_multiple(String.t()) :: String.t()
  # def check_block_event_multiple(
  #       node_id \\ Node.example_random_id()
  #       |> Base.url_encode64()
  #     ) do
  #   check_consensus_event_multiple(node_id)
  #   table_name = Tables.table_events(node_id)

  #   :mnesia.subscribe({:table, table_name, :simple})
  #   block_event(["id 1"], 0, node_id)

  #   assert_receive(
  #     {:mnesia_table_event, {:delete, {^table_name, "id 1"}, _}},
  #     5000
  #   )

  #   assert {:atomic, [{^table_name, :consensus, [["id 2"]]}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, :consensus)
  #            end)

  #   assert {:atomic, []} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, "id 1")
  #            end)

  #   block_event(["id 2"], 0, node_id)

  #   assert_receive(
  #     {:mnesia_table_event, {:delete, {^table_name, "id 2"}, _}},
  #     5000
  #   )

  #   :mnesia.unsubscribe({:table, table_name, :simple})

  #   assert {:atomic, [{^table_name, :consensus, []}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, :consensus)
  #            end)

  #   assert {:atomic, []} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, "id 2")
  #            end)

  #   node_id
  # end

  # @spec check_block_event_leave_one_out(String.t()) :: String.t()
  # def check_block_event_leave_one_out(
  #       node_id \\ Node.example_random_id()
  #       |> Base.url_encode64()
  #     ) do
  #   check_consensus_event_multiple(node_id)
  #   table_name = Tables.table_events(node_id)

  #   :mnesia.subscribe({:table, table_name, :simple})
  #   block_event(["id 1"], 0, node_id)

  #   assert_receive(
  #     {:mnesia_table_event, {:delete, {^table_name, "id 1"}, _}},
  #     5000
  #   )

  #   :mnesia.unsubscribe({:table, table_name, :simple})

  #   assert {:atomic, [{^table_name, :consensus, [["id 2"]]}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, :consensus)
  #            end)

  #   assert {:atomic, []} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, "id 1")
  #            end)

  #   assert {:atomic,
  #           [{^table_name, "id 2", {:transparent_resource, "code 2"}}]} =
  #            :mnesia.transaction(fn ->
  #              :mnesia.read(table_name, "id 2")
  #            end)

  #   node_id
  # end

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

  # @spec create_event_table(String.t()) :: atom()
  # defp create_event_table(node_id) do
  #   Tables.initialize_tables_for_node(node_id)

  #   table_name = Tables.table_events(node_id)

  #   :mnesia.transaction(fn ->
  #     :mnesia.write({table_name, :round, -1})
  #   end)

  #   table_name
  # end

  # @spec replay_ensure_created_tables(String.t()) :: :ok
  # defp replay_ensure_created_tables(node_id) do
  #   Tables.initialize_tables_for_node(node_id)

  #   :ok
  # end

  ############################################################
  #                       Private Helpers                    #
  ############################################################

  # @doc """
  # I fire a transaction event.
  # """
  # @spec tx_event(binary(), Backends.backend(), Noun.t(), String.t()) :: :ok
  # def tx_event(id, backend, code, node_id) do
  #   event =
  #     Event.new_with_body(node_id, %Mempool.TxEvent{
  #       id: id,
  #       tx: %Mempool.Tx{backend: backend, code: code}
  #     })

  #   EventBroker.event(event)
  # end

  @doc """
  I create a random transaction event.
  """
  @spec random_tx_event(String.t(), atom()) ::
          {Event.t(), String.t(), String.t(), atom()}
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
  @spec consensus_event(list(binary()), String.t()) :: :ok
  def consensus_event(order, node_id) do
    event =
      Event.new_with_body(node_id, %Mempool.ConsensusEvent{
        order: order
      })

    EventBroker.event(event)
  end

  # @spec block_event(list(binary()), non_neg_integer(), String.t()) :: :ok
  # def block_event(order, round, node_id) do
  #   event =
  #     Node.Event.new_with_body(node_id, %Mempool.BlockEvent{
  #       order: order,
  #       round: round
  #     })

  #   EventBroker.event(event)
  # end
end
