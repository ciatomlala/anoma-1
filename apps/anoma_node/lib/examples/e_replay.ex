defmodule Anoma.Node.Examples.EReplay do
  @moduledoc """
  I define examples that test the behavior of the replay mechanism.
  """

  alias Anoma.Node.Event
  alias Anoma.Node.Examples.ELogging
  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Replay
  alias Anoma.Node.Tables
  alias Anoma.Node.Transaction.Mempool

  import ExUnit.Assertions

  use EventBroker.WithSubscription

  @doc """
  I try replay for the given node, and assert it succeeded.
  """
  @spec replay_succeeds(ENode.t()) :: ENode.t()
  def replay_succeeds(enode \\ ENode.start_node()) do
    # stop the given node.
    ENode.stop_node(enode)

    # try and replay this node.
    assert {:ok, _} = Replay.replay_for(enode.node_id)

    # start the previous node again.
    ENode.start_node(node_id: enode.node_id, grpc_port: enode.grpc_port)
  end

  @doc """
  I execute replay on a node that has one consensus in its storage.
  """
  @spec replay_with_consensus(ENode.t()) :: ENode.t()
  def replay_with_consensus(enode \\ ENode.start_node()) do
    # use examples from logging to populate the tables
    # this example leaves one transaction in the mempool
    {_node, tx_ids, block_tx_ids} = ELogging.check_block_event_subset(enode)

    IO.inspect(tx_ids, label: "tx_ids")
    IO.inspect(block_tx_ids, label: "block_tx_ids")

    # assert replay works
    with_subscription [[%Mempool.TxFilter{}]] do
      replay_succeeds(enode)

      for _ <- 1..100 do
        receive do
          m ->
            IO.inspect(m)
        after
          0 -> :ok
        end
      end
    end

    enode
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
end
