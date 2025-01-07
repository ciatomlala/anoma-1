defmodule Anoma.Node.Supervisor do
  @moduledoc """
  I am the top level supervisor for the Anoma node.
  """

  use Supervisor

  require Logger

  alias Anoma.Node.Intents
  alias Anoma.Node.Logging
  alias Anoma.Node.Transaction
  alias Anoma.Node.Transport

  @type args :: [
          node_id: String.t(),
          grpc_port: non_neg_integer(),
          tx_args: any()
        ]

  @args [
    :node_id,
    :tx_args,
    grpc_port: 0,
    replay: true
  ]

  @spec child_spec(any()) :: map()
  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary
    }
  end

  @spec start_link(args) :: term()
  def start_link(args) do
    args = Keyword.validate!(args, @args)
    name = Anoma.Node.Registry.via(args[:node_id], __MODULE__)
    Supervisor.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(args) do
    Logger.info("starting node with #{inspect(args)}")
    Process.set_label(__MODULE__)

    # validate arguments
    args = Keyword.validate!(args, @args)

    node_id = args[:node_id]
    grpc_port = args[:grpc_port]
    tx_args = args[:tx_args]

    children = [
      {Transport.Supervisor, node_id: node_id, grpc_port: grpc_port},
      {Transaction.Supervisor, node_id: node_id, tx_args: tx_args},
      {Intents.Supervisor, node_id: node_id},
      {Logging, node_id: node_id}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
