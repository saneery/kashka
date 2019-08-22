defmodule Kashka.GenConsumer do
  use GenServer
  require Logger

  alias Kashka.MintHelper
  alias Kashka.Kafka

  @callback init(opts :: Map.t()) :: any
  @callback handle_message_set(conn :: any, state :: any, message_set :: [Map.t()]) :: any

  defstruct uri: nil, conn: nil, name: nil, opts: nil, internal_state: nil

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    opts = load_defaults(opts)
    name = Map.get(opts, :name)
    Process.flag(:trap_exit, true)

    state = %__MODULE__{conn: opts.url, name: name, opts: opts}
    {:ok, base_uri} = create_consumer(state)
    {:ok, conn} = Kafka.subscribe(base_uri, opts.topics)

    st = %{state | conn: conn, internal_state: opts.module.init(opts)}
    {:ok, st, 0}
  end

  @impl true
  def terminate(reason, %{uri: uri} = st) do
    Logger.debug("Going to terminate #{st.name} with reason #{inspect(reason)}")
    {:ok, conn} = Kafka.delete_consumer(st.conn)
    Logger.debug("Consumer #{st.name} deleted successfully")
    {:noreply, %{st | conn: conn}}
  end

  @impl true
  def handle_info(:timeout, state) do
    Logger.debug("Going to request records")
    # timeout = state.opts.records_opts.timeout
    # TODO
    {:ok, conn, records} = Kafka.get_records(state.conn, state.opts.records_opts)
    Logger.debug("#{length(records)} received")

    {:ok, conn, internal_state} =
      state.opts.module.handle_message_set(conn, state.internal_state, records)

    {:noreply, %{state | conn: conn, internal_state: internal_state}, 0}
  end

  def handle_info(message, state) do
    Logger.debug("Unknown message #{inspect(message)}")
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
        Kafka.close(conn)
        {:ok, base_uri}

      {:error, :exists} ->
        {:ok, conn} = Kafka.delete_consumer(state.conn, state.opts.consumer_group, state.name)
        create_consumer(%{state | conn: conn})
    end
  end

  defp delete(state, path) do
    Logger.debug("Trying to delete")
    {:ok, conn} = Kafka.delete_consumer(state.conn)
    %{state | conn: conn}
  end

  defp random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end

  defp load_defaults(opts) do
    opts = Map.new(opts)

    records_opts =
      Map.get(opts, :records_opts, %{})
      |> Map.new()
      |> Map.merge(%{max_bytes: 100_000, timeout: 1000})

    opts
    |> Map.put(:records_opts, records_opts)
    |> Map.put_new(:name, random_string(10))
    |> Map.put_new(:consumer_opts, %{
      format: "json",
      "auto.offset.reset": "earliest",
      "auto.commit.enable": false
    })
  end
end
