defmodule Kashka.Kafka do
  @content {"Content-Type", "application/vnd.kafka.v2+json"}
  @content_json {"Content-Type", "application/vnd.kafka.json.v2+json"}
  @content_binary {"Content-Type", "application/vnd.kafka.binary.v2+json"}

  @accept {"Accept", "application/vnd.kafka.v2+json"}
  @accept_json {"Accept", "application/vnd.kafka.json.v2+json"}
  @accept_binary {"Accept", "application/vnd.kafka.binary.v2+json"}

  alias Kashka.Http
  require Logger

  @type http_error :: {:error, :http, code :: non_neg_integer(), iodata()}

  @spec topics(Http.t()) :: {:ok, Http.t(), %{}} | http_error()
  def topics(conn) do
    with {:ok, conn, data} <- request(conn, "GET", "topics", [@accept], "") do
      {:ok, conn, Jason.decode!(data)}
    end
  end

  @spec offsets(Http.t(), %{}) :: {:ok, Http.t(), %{}} | http_error()
  def offsets(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    with {:ok, conn, data} <- request(conn, "GET", "offsets", [@accept], data) do
      offsets = Jason.decode!(data) |> Map.get("offsets")
      {:ok, conn, offsets}
    end
  end

  @spec assignments(Http.t()) :: {:ok, Http.t(), %{}} | http_error()
  def assignments(conn) do
    with {:ok, conn, data} <- request(conn, "GET", "assignments", [@accept], "") do
      partitions = Jason.decode!(data) |> Map.get("partitions")
      {:ok, conn, partitions}
    end
  end

  @spec positions_end(Http.t(), %{}) :: {:ok, Http.t()} | http_error()
  def positions_end(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    with {:ok, conn, _data} <- request(conn, "POST", "positions/end", [@content], data) do
      {:ok, conn}
    end
  end

  @spec commit(Http.t(), %{}) :: {:ok, Http.t()} | http_error()
  def commit(conn, opts \\ %{}) do
    body =
      case opts do
        %{} -> ""
        _ -> Jason.encode!(opts)
      end

    with {:ok, conn, _data} <- request(conn, "POST", "offsets", [@content], body) do
      {:ok, conn}
    end
  end

  @spec produce(Http.t(), String.t(), [%{}], :json | :binary) :: {:ok, Http.t()} | http_error()
  def produce(conn, topic, records, format \\ :json)

  def produce(conn, topic, records, :json) when is_list(records) do
    path = Path.join(["topics", topic])
    data = Jason.encode!(%{records: records})

    with {:ok, conn, _} <- request(conn, "POST", path, [@content_json, @accept], data) do
      {:ok, conn}
    end
  end

  def produce(conn, topic, records, :binary) when is_list(records) do
    path = Path.join(["topics", topic])

    records =
      Enum.map(records, fn e ->
        %{e | value: :base64.encode(e.value), key: :base64.encode(e.key)}
      end)

    data = Jason.encode!(%{records: records})

    with {:ok, conn, _} <- request(conn, "POST", path, [@content_binary, @accept], data) do
      {:ok, conn}
    end
  end

  @spec get_records(Http.t(), %{}, :json | :binary) :: {:ok, Http.t(), [%{}]} | http_error()
  def get_records(conn, opts, format \\ :json)

  def get_records(conn, opts, :json) do
    query = URI.encode_query(opts)

    with {:ok, conn, data} <- request(conn, "GET", "records?#{query}", [@accept_json], "") do
      {:ok, conn, Jason.decode!(data)}
    end
  end

  def get_records(conn, opts, :binary) do
    query = URI.encode_query(opts)

    with {:ok, conn, json} <-
           request(conn, "GET", "records?#{query}", [@accept_binary], "") do
      data =
        json
        |> Jason.decode!()
        |> Enum.map(fn e ->
          %{e | "value" => :base64.decode(e["value"]), "key" => :base64.decode(e["key"])}
        end)

      {:ok, conn, data}
    end
  end

  @spec create_consumer(Kashka.Http.t(), String.t(), map()) ::
          {:ok, Kashka.Http.t(), map()} | {:error, :exists} | http_error()
  def create_consumer(conn, consumer_group, opts) do
    body = Jason.encode!(opts)
    path = Path.join(["consumers", consumer_group])

    case Http.request(conn, "POST", path, [@content], body) do
      {:ok, conn, 200, data} ->
        {:ok, conn, Jason.decode!(data)}

      {:ok, conn, 409, _data} ->
        close(conn)
        {:error, :exists}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @spec delete_consumer(Kashka.Http.t()) :: {:ok, Kashka.Http.t()} | http_error()
  def delete_consumer(conn) do
    with {:ok, conn, _} <- request(conn, "DELETE", "", [@content], "") do
      {:ok, conn}
    end
  end

  @spec delete_consumer(Kashka.Http.t(), String.t(), String.t()) ::
          {:ok, Kashka.Http.t()} | http_error()
  def delete_consumer(conn, group, name) do
    path = Path.join(["consumers", group, "instances", name])

    with {:ok, conn, _} <- request(conn, "DELETE", path, [@content], "") do
      {:ok, conn}
    end
  end

  def subscribe(conn, topics) when is_list(topics) do
    data = Jason.encode!(%{"topics" => topics})

    with {:ok, conn, _} <- request(conn, "POST", "subscription", [@content], data) do
      {:ok, conn}
    end
  end

  def close(conn) do
    Http.close(conn)
  end

  @spec request(Http.t(), String.t(), String.t(), Mint.Types.headers(), iodata()) ::
          {:ok, Http.t(), iodata()} | http_error()
  defp request(conn, method, path, headers, body) do
    case Http.request(conn, method, path, headers, body, http_timeout()) do
      {:ok, conn, code, data} when code >= 200 and code < 300 ->
        {:ok, conn, data}

      {:ok, conn, code, data} ->
        :ok = close(conn)
        {:error, :http, code, data}
    end
  end

  defp http_timeout() do
    Application.get_env(:kashka, :http_timeout, 20000)
  end
end
