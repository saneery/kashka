defmodule Kashka.GenConsumer do
  @moduledoc """
  Module to start consuming process.
  """

  use GenServer
  require Logger

  alias Kashka.Kafka
  alias Kashka.Http

  @type opts() :: [
          {:url, Kashka.Http.conn()}
          | {:topics, [String.t()]}
          | {:consumer_group, String.t()}
          | {:module, module()}
          | {:instance_id, String.t()}
          | {:consumer_opts, %{}}
          | {:records_opts, %{}}
          | {:delete_on_exists, boolean()}
          | {:retry_on_exists, boolean()}
          | any()
        ]

  @doc """
  Invoked on process initialization. Optional.

  ## Parameters

    - conn: is opened connection to created consumer
    - opts: is opts passed into `start_link/1`

  Returns connection and internal state for `c:handle_message_set/3`
  """
  @callback init(conn :: Kashka.Http.t(), opts :: opts()) :: {:ok, Kashka.Http.t(), any()}

  @doc """
  Invoked on each received records batch.

  ## Parameters

    - conn: is opened connection to created consumer
    - state: any term returned from `c:init/2` or previous `c:handle_message_set/3`
    - message_set: received records list
  """
  @callback handle_message_set(conn :: Kashka.Http.t(), state :: any(), message_set :: [map()]) ::
              {:ok, Kashka.Http.t(), any()}

  @optional_callbacks init: 2

  @type state() :: %__MODULE__{
          instance_id: String.t(),
          conn: Kashka.Http.t(),
          internal_state: any(),
          opts: opts()
        }
  defstruct conn: nil, instance_id: nil, opts: nil, internal_state: nil

  @doc """
  Start consumer process and subscribe to topics. Allowed options are:

    - url: url or url with opts
    - topics: list of topics (see `Kashka.Kafka.subscribe/2`)
    - consumer_group: consumer group name
    - module: module which implements Kashka.GenConsumer behaviour
    - instance_id: consumer instance_id (see `Kashka.Kafka.create_consumer/3`)
    - consumer_opts: consumer opts (see `Kashka.Kafka.create_consumer/3`)
    - records_opts: consumer opts (see `Kashka.Kafka.get_records/3`)
    - delete_on_exists: allow to try do delete consumer with the same instance_id if it exists
    - retry_on_exists: allow to retry creating consumer every 5 seconds if consumer with the same instance_id exists
  """
  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Start named consumer process and subscribe to topics. Opts are the same as `start_link/1`
  """
  @spec start_link(GenServer.name(), opts()) :: GenServer.on_start()
  def start_link(name, opts) do
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    opts = load_defaults(opts)
    Process.flag(:trap_exit, true)
    st = %__MODULE__{conn: opts.url, instance_id: opts[:instance_id], opts: opts}
    {:ok, do_init(st)}
  end

  defp do_init(st) do
    case create_consumer(st) do
      {:ok, conn, instance_id, base_uri} ->
        Logger.info("Consumer with base uri #{base_uri} created")

        {:ok, conn} =
          Http.reconnect_to(conn, base_uri)
          |> Kafka.subscribe(st.opts.topics)

        Logger.info("Consumer #{instance_id} subscribed to topics #{inspect(st.opts.topics)}")

        {:ok, conn, internal_state} =
          if function_exported?(st.opts.module, :init, 2) do
            st.opts.module.init(conn, st.opts)
          else
            {:ok, conn, st.opts}
          end

        Process.send(self(), :timeout, [])

        %{st | conn: conn, instance_id: instance_id, internal_state: internal_state}

      :retry ->
        Process.send_after(self(), :init, 5000)
        st
    end
  end

  @impl true
  def terminate(reason, st) do
    Logger.info("Going to terminate #{st.instance_id} with reason #{inspect(reason)}")
    {:ok, conn} = Kafka.delete_consumer(st.conn)
    Logger.info("Consumer #{st.instance_id} deleted successfully")
    Kafka.close(conn)
    Logger.info("Connection #{st.instance_id} closed successfully")
  end

  @impl true
  def handle_info(:init, st) do
    {:noreply, do_init(st)}
  end

  def handle_info(:timeout, state) do
    Logger.debug("Going to request records")

    {:ok, conn, records} =
      Kafka.get_records(state.conn, state.opts.records_opts, state.opts.format)

    Logger.debug("Consumer #{state.instance_id} received #{length(records)} records")

    {:ok, conn, internal_state} =
      state.opts.module.handle_message_set(conn, state.internal_state, records)

    Process.send(self(), :timeout, [])

    {:noreply, %{state | conn: conn, internal_state: internal_state}, 0}
  end

  def handle_info(message, state) do
    Logger.info("Unknown message #{inspect(message)}")
    {:noreply, state, 0}
  end

  defp create_consumer(state) do
    Logger.debug("Trying to create consumer with name #{inspect state.instance_id}")

    consumer_opts =
      state.opts.consumer_opts
      |> Map.put(:name, state.instance_id)

    case Kafka.create_consumer(state.conn, state.opts.consumer_group, consumer_opts) do
      {:ok, conn, %{"instance_id" => instance_id, "base_uri" => base_uri}} ->
        Logger.debug("Consumer #{instance_id} created")
        {:ok, conn, instance_id, base_uri}

      {:error, :exists} ->
        cond do
          state.opts.delete_on_exists ->
            Logger.info("Deleting old consumer")

            {:ok, conn} = Kafka.delete_consumer(state.conn, state.opts.consumer_group, state.instance_id)
            create_consumer(%{state | conn: conn})

          state.opts.retry_on_exists ->
            Logger.info("Consumer #{state.instance_id} already exists. Waiting for 5 seconds and retry")
            :retry

          true ->
            raise "Consumer #{state.instance_id} already exists"
        end
    end
  end

  defp load_defaults(opts) do
    opts = Map.new(opts)

    format = Map.get(opts, :format, :binary)

    records_opts =
      Map.merge(%{max_bytes: 100_000, timeout: 1000}, Map.get(opts, :records_opts, %{}))

    consumer_opts = Map.get(opts, :consumer_opts, %{}) |> Map.merge(%{format: format})

    opts
    |> Map.put(:records_opts, records_opts)
    |> Map.put(:consumer_opts, consumer_opts)
    |> Map.put(:format, format)
    |> Map.put_new(:delete_on_exists, false)
    |> Map.put_new(:retry_on_exists, false)
  end
end
