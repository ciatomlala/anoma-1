defmodule Anoma.Supervisor do
  @moduledoc """
  I am the top level supervisor for the Anoma node application.

  I manage the shared processes and multiple nodes.

  ### Shared Processes
   - Registry
   - NodeSupervisor
  """

  use Supervisor

  alias Anoma.Node.Tables

  @spec start_link(any()) :: Supervisor.on_start()
  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_args) do
    Process.set_label(__MODULE__)

    :ok = Anoma.Node.Tables.initialize_storage()

    children = [
      {Elixir.Registry, keys: :unique, name: Anoma.Node.Registry},
      {DynamicSupervisor, name: Anoma.Node.NodeSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc """
  I start a new node with the given `node_id`.
  """
  @spec start_node(
          list(
            {:node_id, String.t()}
            | {:grpc_port, non_neg_integer()}
            | {:tx_args, any()}
          )
        ) :: DynamicSupervisor.on_start_child()
  def start_node(args) do
    args =
      Keyword.validate!(args, [
        :node_id,
        :grpc_port,
        tx_args: [mempool: [], ordering: [], storage: []],
        replay: true
      ])

    node_id = args[:node_id]

    with {:ok, _} <- initialize_storage(node_id),
         {:ok, tx_args} <- replay_node(node_id, args[:replay]),
         tx_args <-
           if(tx_args == :no_replay, do: args[:tx_args], else: tx_args) do
      args = Keyword.put(args, :tx_args, tx_args)

      DynamicSupervisor.start_child(
        Anoma.Node.NodeSupervisor,
        {Anoma.Node.Supervisor, args}
      )
    else
      {:error, :failed_to_initialize_storage} ->
        {:error, :failed_to_start_node, :failed_to_initialize_storage}
    end
  end

  ############################################################
  #                  Private Helpers                         #
  ############################################################

  @spec initialize_storage(String.t()) ::
          {:ok, :existing_node | :new_node}
          | {:error, :failed_to_initialize_storage}
  defp initialize_storage(node_id) do
    # check if the node has existing tables, and initialize them if need be.
    case Tables.initialize_tables_for_node(node_id) do
      {:ok, :created} ->
        {:ok, :new_node}

      {:ok, :existing} ->
        {:ok, :existing_node}

      {:error, _e} ->
        {:error, :failed_to_initialize_storage}
    end
  end

  # @doc """
  # I execute a replay for the given node id.
  # I return whether it succeeded or not.
  # """
  defp replay_node(_node_id, false) do
    {:ok, :no_replay}
  end

  defp replay_node(node_id, true) do
    case Anoma.Node.Replay.replay_for(node_id) do
      {:ok, supervisor_args} ->
        {:ok, supervisor_args}
    end
  end
end
