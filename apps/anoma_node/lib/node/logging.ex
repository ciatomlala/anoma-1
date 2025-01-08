defmodule Anoma.Node.Logging do
  @moduledoc """
  I am the Logging Engine.

  I combine the classic logger with replay functionality. In particular,
  I store the most recent data coming outside of a Node so that if it fails
  I can re-do the actions fed to me in a linear fashion.

  ### Public API

  I provide the following public functionality:

  #### Replay

  - `restart_with_replay/1`
  - `replay_args/1`
  - `try_launch/2`
  - `replay_setup/2`
  - `replay_table_clone/3`

  #### Other

  - `table_name/1`
  - `init_table/2`
  - `log_event/3`
  """

  alias Anoma.Node
  alias Anoma.Node.Logging
  alias Anoma.Node.Registry
  alias Anoma.Node.Tables
  alias Anoma.Node.Transaction.Mempool

  use EventBroker.DefFilter
  use GenServer
  use TypedStruct

  require Node.Event
  require Logger

  ############################################################
  #                         State                            #
  ############################################################

  @typedoc """
  I am the loggging message type flag.

  I specify what logging levels are currently supported by the Logging
  Engine.
  """
  @type flag :: :info | :debug | :warning | :error

  @typep startup_options() ::
           {:node_id, String.t()} | {:table, atom()} | {:rocks, bool()}

  typedstruct module: LoggingEvent do
    @typedoc """
    I am the type of a logging event.

    I specify the format of any logging message sent.

    ### Fields

    - `:flag` - The level at which the event ought to be logged.
    - `:msg` - A logging message.
    """

    field(:flag, Logging.flag())
    field(:msg, binary())
  end

  typedstruct do
    @typedoc """
    I am the type of the Logging Engine.

    I store a Node ID with which I am associated alongside a table which
    stores all relevant events.

    ### Fields

    - `:node_id` - The ID of the Node to which a Logging Engine
                   instantiation is bound.
    """

    field(:node_id, String.t())
  end

  deffilter LoggingFilter do
    %EventBroker.Event{
      body: %Node.Event{body: %Anoma.Node.Logging.LoggingEvent{}}
    } ->
      true

    %EventBroker.Event{body: %Node.Event{body: %Mempool.TxEvent{}}} ->
      true

    %EventBroker.Event{body: %Node.Event{body: %Mempool.ConsensusEvent{}}} ->
      true

    %EventBroker.Event{body: %Node.Event{body: %Mempool.BlockEvent{}}} ->
      true

    _ ->
      false
  end

  ############################################################
  #                    Genserver Helpers                     #
  ############################################################

  @doc """
  I am the start_link function of the Logging Engine.

  I register the Engine with the supplied Node ID provided by the arguments
  and check that the table keyword has been provided.
  """

  @spec start_link(list(startup_options())) :: term()
  def start_link(args) do
    args = Keyword.validate!(args, [:node_id])
    name = Registry.via(args[:node_id], __MODULE__)
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @doc """
  I am the initialization function for the Logging Engine.

  From the specified arguments, I get the Node ID, the table name, as well
  as the boolean indicating whether the table should be backed by RocksDB.

  I then initialize the table with the given name and backing options,
  subscribe to logging messages and then launch the Engine with the given
  options.
  """

  @impl true
  def init(args) do
    Process.set_label(__MODULE__)

    args = Keyword.validate!(args, [:node_id])

    # initialize the necessary tables for the logging engine
    init_table(args[:node_id])

    node_id = args[:node_id]

    EventBroker.subscribe_me([
      Node.Event.node_filter(node_id),
      logging_filter()
    ])

    {:ok, %Logging{node_id: node_id}}
  end

  ############################################################
  #                      Public Filters                      #
  ############################################################

  @doc """
  I am the logging filter.

  I filter for any incoming messages the Logging Engine cares about.
  """

  @spec logging_filter() :: LoggingFilter.t()
  def logging_filter() do
    %__MODULE__.LoggingFilter{}
  end

  ############################################################
  #                    Genserver Behavior                    #
  ############################################################

  @impl true
  def handle_info(
        e = %EventBroker.Event{
          body: %Node.Event{
            body: %Logging.LoggingEvent{}
          }
        },
        state
      ) do
    {:noreply, handle_logging_event(e, state)}
  end

  def handle_info(
        e = %EventBroker.Event{
          body: %Node.Event{
            body: %Mempool.TxEvent{}
          }
        },
        state
      ) do
    {:noreply, handle_tx_event(e, state)}
  end

  def handle_info(
        e = %EventBroker.Event{
          body: %Node.Event{
            body: %Mempool.ConsensusEvent{}
          }
        },
        state
      ) do
    {:noreply, handle_consensus_event(e, state)}
  end

  def handle_info(
        e = %EventBroker.Event{
          body: %Node.Event{
            body: %Mempool.BlockEvent{}
          }
        },
        state
      ) do
    {:noreply, handle_block_event(e, state)}
  end

  ############################################################
  #                 Genserver Implementation                 #
  ############################################################

  @spec handle_logging_event(EventBroker.Event.t(), t()) :: t()
  defp handle_logging_event(
         %EventBroker.Event{
           body: %Node.Event{
             body: %Logging.LoggingEvent{
               flag: flag,
               msg: msg
             }
           }
         },
         state
       ) do
    log_fun({flag, msg})
    state
  end

  @spec handle_tx_event(EventBroker.Event.t(), t()) :: t()
  defp handle_tx_event(
         %EventBroker.Event{
           body: %Node.Event{
             body: %Mempool.TxEvent{
               id: id,
               tx: %Mempool.Tx{backend: backend, code: code}
             }
           }
         },
         state
       ) do
    :mnesia.transaction(fn ->
      table = Tables.table_events(state.node_id)
      :mnesia.write({table, id, {backend, code}})
    end)

    log_fun({:info, "Transaction Launched. Id: #{inspect(id)}"})
    state
  end

  @spec handle_consensus_event(EventBroker.Event.t(), t()) :: t()
  defp handle_consensus_event(
         %EventBroker.Event{
           body: %Node.Event{
             body: %Mempool.ConsensusEvent{
               order: list
             }
           }
         },
         state
       ) do
    :mnesia.transaction(fn ->
      table = Tables.table_events(state.node_id)
      pending = match(:consensus, table)
      :mnesia.write({table, :consensus, pending ++ [list]})
    end)

    log_fun({:info, "Consensus provided order. List: #{inspect(list)}"})
    state
  end

  @spec handle_block_event(EventBroker.Event.t(), t()) :: t()
  defp handle_block_event(
         %EventBroker.Event{
           body: %Node.Event{
             body: %Mempool.BlockEvent{
               order: id_list,
               round: round
             }
           }
         },
         state
       ) do
    table = Tables.table_events(state.node_id)

    :mnesia.transaction(fn ->
      for id <- id_list do
        :mnesia.delete({table, id})
      end

      current_pending = match(:consensus, table)
      :mnesia.write({table, :consensus, tl(current_pending)})
      :mnesia.write({table, :round, round})
    end)

    log_fun({:info, "Block succesfully committed. Round: #{inspect(round)}"})
    state
  end

  ############################################################
  #                           Helpers                        #
  ############################################################

  defp log_fun({:debug, msg}), do: Logger.debug(msg)

  defp log_fun({:info, msg}), do: Logger.info(msg)

  defp log_fun({:warning, msg}), do: Logger.warning(msg)

  defp log_fun({:error, msg}), do: Logger.error(msg)

  @doc """
  I am the log event function.

  I provide an interface to "log" new messages in an easy format.

  Given a Node ID, a flag, and a message, I create a new event with
  appropriate flag and message.
  """

  @spec log_event(String.t(), flag(), binary()) :: :ok
  def log_event(node_id, flag, msg) do
    Node.Event.new_with_body(node_id, %__MODULE__.LoggingEvent{
      flag: flag,
      msg: msg
    })
    |> EventBroker.event()
  end

  @spec match(atom(), atom()) :: any()
  defp match(flag, table) do
    case :mnesia.read({table, flag}) do
      [] -> []
      [{_, ^flag, current_pending}] -> current_pending
    end
  end

  @spec init_table(String.t()) :: :ok
  defp init_table(node_id) do
    # initialize the tables
    Tables.initialize_tables_for_node(node_id)

    # clear the table if it was not empty
    Tables.clear_table(Tables.table_events(node_id))

    # insert default record in the events table
    table = Tables.table_events(node_id)

    :mnesia.transaction(fn ->
      :mnesia.write({table, :round, -1})
    end)

    :ok
  end
end
