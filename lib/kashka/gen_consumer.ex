defmodule Kashka.GenConsumer do
  use GenServer
  require Logger

  alias Kashka.Kafka

  @type opts() :: [
          {:url, String.t()}
          | {:topics, [String.t()]}
          | {:consumer_group, String.t()}
          | {:module, module()}
          | {:name, String.t()}
          | {:consumer_opts, %{}}
          | {:records_opts, %{}}
          | {:delete_on_exists, boolean()}
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
    name = Map.get(opts, :name)
    Process.flag(:trap_exit, true)

    url = URI.parse(opts.url)

    state = %__MODULE__{conn: url, name: name, opts: opts}
    {:ok, base_uri} = create_consumer(state)

    base_uri =
      if opts.preserve_schema_and_port do
        %{URI.parse(base_uri) | scheme: url.scheme, port: url.port}
      else
        base_uri
      end

    {:ok, conn} = Kafka.subscribe(base_uri, opts.topics)

    {:ok, conn, internal_state} =
      if function_exported?(opts.module, :init, 2) do
        opts.module.init(conn, opts)
      else
        {:ok, conn, opts}
      end

    st = %{state | conn: conn, internal_state: internal_state}
    {:ok, st, 0}
  end

  @impl true
  def terminate(reason, st) do
    Logger.info("Going to terminate #{st.name} with reason #{inspect(reason)}")
    {:ok, conn} = Kafka.delete_consumer(st.conn)
    Logger.info("Consumer #{st.name} deleted successfully")
    {:noreply, %{st | conn: conn}}
  end

  @impl true
  def handle_info(:timeout, state) do
    Logger.debug("Going to request records")
    # timeout = state.opts.records_opts.timeout
    # TODO
    {:ok, conn, records} =
      Kafka.get_records(state.conn, state.opts.records_opts, state.opts.format)

    Logger.debug("Consumer #{state.name} received #{length(records)} records")

    {:ok, conn, internal_state} =
      state.opts.module.handle_message_set(conn, state.internal_state, records)

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
        Kafka.close(conn)
        {:ok, base_uri}

      {:error, :exists} ->
        case state.opts.delete_on_exists do
          true ->
            Logger.info("Deleting old consumer")

            {:ok, conn} =
              Kafka.consumer_path(state.conn, state.opts.consumer_group, state.name)
              |> Kafka.delete_consumer()

            Kafka.close(conn)
            create_consumer(state)

          false ->
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
    |> Map.put_new(:preserve_schema_and_port, false)
  end
end
