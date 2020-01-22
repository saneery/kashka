defmodule Kashka.GenConsumer do
  @moduledoc """
  Module to start consuming process
  """

  use GenServer
  require Logger

  alias Kashka.Kafka
  alias Kashka.Http

  @type opts() :: [
          {:url, Kashka.Http.args()}
          | {:topics, [String.t()]}
          | {:consumer_group, String.t()}
          | {:module, module()}
          | {:name, String.t()}
          | {:consumer_opts, %{}}
          | {:records_opts, %{}}
          | {:delete_on_exists, boolean()}
          | {:retry_on_exists, boolean()}
          | any()
        ]

  @callback init(conn :: Kashka.Http.t(), opts :: opts()) :: {:ok, Kashka.Http.t(), any()}
  @callback handle_message_set(conn :: Kashka.Http.t(), state :: any(), message_set :: [map()]) ::
              {:ok, Kashka.Http.t(), any()}

  @optional_callbacks init: 2

  @type state() :: %__MODULE__{
          name: String.t(),
          conn: Kashka.Http.t(),
          internal_state: any(),
          opts: opts()
        }
  defstruct conn: nil, name: nil, opts: nil, internal_state: nil

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec start_link(GenServer.name(), opts()) :: GenServer.on_start()
  def start_link(name, opts) do
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    opts = load_defaults(opts)
    Process.flag(:trap_exit, true)
    st = %__MODULE__{conn: opts.url, name: opts.name, opts: opts}
    {:ok, do_init(st)}
  end

  defp do_init(st) do
    case create_consumer(st) do
      {:ok, conn, base_uri} ->
        Logger.info("Consumer with base uri #{base_uri} created")

        {:ok, conn} =
          Http.reconnect_to(conn, base_uri)
          |> Kafka.subscribe(st.opts.topics)

        Logger.info("Consumer #{st.name} subscribed to topics #{inspect(st.opts.topics)}")

        {:ok, conn, internal_state} =
          if function_exported?(st.opts.module, :init, 2) do
            st.opts.module.init(conn, st.opts)
          else
            {:ok, conn, st.opts}
          end

        Process.send(self(), :timeout, [])

        %{st | conn: conn, internal_state: internal_state}

      :retry ->
        Process.send_after(self(), :init, 5000)
        st
    end
  end

  @impl true
  def terminate(reason, st) do
    Logger.info("Going to terminate #{st.name} with reason #{inspect(reason)}")
    {:ok, conn} = Kafka.delete_consumer(st.conn)
    Logger.info("Consumer #{st.name} deleted successfully")
    Kafka.close(conn)
    Logger.info("Connection #{st.name} closed successfully")
  end

  @impl true
  def handle_info(:init, st) do
    {:noreply, do_init(st)}
  end

  def handle_info(:timeout, state) do
    Logger.debug("Going to request records")

    {:ok, conn, records} =
      Kafka.get_records(state.conn, state.opts.records_opts, state.opts.format)

    Logger.debug("Consumer #{state.name} received #{length(records)} records")

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
    Logger.debug("Trying to create consumer #{state.name}")

    consumer_opts =
      state.opts.consumer_opts
      |> Map.put(:name, state.name)

    case Kafka.create_consumer(state.conn, state.opts.consumer_group, consumer_opts) do
      {:ok, conn, %{"base_uri" => base_uri}} ->
        Logger.debug("Consumer #{state.name} created")
        {:ok, conn, base_uri}

      {:error, :exists} ->
        cond do
          state.opts.delete_on_exists ->
            Logger.info("Deleting old consumer")

            {:ok, conn} = Kafka.delete_consumer(state.conn, state.opts.consumer_group, state.name)
            create_consumer(%{state | conn: conn})

          state.opts.retry_on_exists ->
            Logger.info("Consumer #{state.name} already exists. Waiting for 5 seconds and retry")
            :retry

          true ->
            raise "Consumer #{state.name} already exists"
        end
    end
  end

  defp random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
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
    |> Map.put_new(:name, random_string(10))
    |> Map.put_new(:delete_on_exists, false)
    |> Map.put_new(:retry_on_exists, false)
  end
end
