defmodule Anoma.Node.Examples.EReplay.StartState do
  @moduledoc """
  I define examples on how the start state of a node is computed.
  """

  alias Anoma.Node.Event
  alias Anoma.Node.Examples.ELogging
  alias Anoma.Node.Examples.ETransaction
  alias Anoma.Node.Examples.Mempool, as: EMempool
  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Replay
  alias Anoma.Node.Tables
  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Transaction.Backends

  # def storage_state(enode \\ ENode.start_node()) do
  #   # write a new block to storage to ensure that the table is modified.

  #   # start the previous node again.
  #   ENode.start_node(node_id: enode.node_id, grpc_port: enode.grpc_port)
  # end
end
