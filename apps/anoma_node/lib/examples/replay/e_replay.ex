defmodule Anoma.Node.Examples.EReplay do
  @moduledoc """
  I define examples that test the behavior of the replay mechanism.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Replay

  import ExUnit.Assertions

  use EventBroker.WithSubscription

  require Logger

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
  I execute replay on a node that has a transaction in its mempool.
  """
  @spec replay_with_transaction(ENode.t()) :: ENode.t()
  def replay_with_transaction(enode \\ ENode.start_node()) do
    # # insert a transaction into the mempool
    # {_node, _transaction} = EMempool.add_transaction(enode)

    # # mock the backends implementation to crash whenever a transaction is evaluated
    # # execute_fn = fn _node_id, _tx, _id -> raise "All broken" end

    # # with_mock Backends, execute: execute_fn do
    # # end

    # # assert replay works for this node.
    # replay_succeeds(enode)

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
