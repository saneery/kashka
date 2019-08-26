defmodule Kashka.Kafka do
  @consumer_headers [
    {"Content-Type", "application/vnd.kafka.v2+json"},
    {"Accept", "application/vnd.kafka.json.v2+json"}
  ]

  @produce_headers [
    {"Content-Type", "application/vnd.kafka.json.v2+json"},
    {"Accept", "application/vnd.kafka.v2+json"}
  ]

  alias Kashka.Http
  require Logger

  @type http_error :: {:error, :http, non_neg_integer(), iodata()}

  def topics(conn) do
    h = [{"Accept", "application/vnd.kafka.v2+json"}]

    with {:ok, conn, data} <- request(conn, "GET", "topics", h, "") do
      {:ok, conn, Jason.decode!(data)}
    end
  end

  @spec offsets(Http.t(), %{}) :: {:ok, Http.t(), %{}} | http_error()
  def offsets(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    case Http.request(conn, "GET", "offsets", @consumer_headers, data, 20000) do
      {:ok, conn, 200, data} ->
        offsets = Jason.decode!(data) |> Map.get("offsets")
        {:ok, conn, offsets}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @spec assignments(Http.t()) :: {:ok, Http.t(), %{}} | http_error()
  def assignments(conn) do
    case Http.request(conn, "GET", "assignments", @consumer_headers, "", 20000) do
      {:ok, conn, 200, data} ->
        partitions = Jason.decode!(data) |> Map.get("partitions")
        {:ok, conn, partitions}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @spec positions_end(Http.t(), %{}) :: {:ok, Http.t()} | http_error()
  def positions_end(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    case Http.request(conn, "POST", "positions/end", @consumer_headers, data, 20000) do
      {:ok, conn, 200, _data} ->
        {:ok, conn}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @spec commit(Http.t(), %{}) :: {:ok, Http.t()} | http_error()
  def commit(conn, opts \\ %{}) do
    body =
      case opts do
        %{} -> ""
        _ -> Jason.encode!(opts)
      end

    with {:ok, conn, _data} <- request(conn, "POST", "offsets", @consumer_headers, body, 20000) do
      {:ok, conn}
    end
  end

  @spec produce_json(Http.t(), String.t(), [%{}]) :: {:ok, Http.t()} | http_error()
  def produce_json(conn, topic, records) when is_list(records) do
    path = Path.join(["topics", topic])
    data = Jason.encode!(%{records: records})

    case Http.request(conn, "POST", path, @produce_headers, data) do
      {:ok, conn, 200, _data} ->
        {:ok, conn}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @spec produce_binary(Http.t(), String.t(), [%{}]) :: {:ok, Http.t()} | http_error()
  def produce_binary(conn, topic, records) when is_list(records) do
    path = Path.join(["topics", topic])

    records =
      Enum.map(records, fn e ->
        %{e | value: :base64.encode(e.value)}
      end)

    data = Jason.encode!(%{records: records})

    headers = [
      {"Content-Type", "application/vnd.kafka.binary.v2+json"},
      {"Accept", "application/vnd.kafka.v2+json"}
    ]

    case Http.request(conn, "POST", path, headers, data) do
      {:ok, conn, 200, _data} ->
        {:ok, conn}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @spec get_records(Http.t(), %{}) :: {:ok, Http.t(), [%{}]} | http_error()
  def get_records(conn, opts) do
    query = URI.encode_query(opts)

    case Http.request(conn, "GET", "records?#{query}", @consumer_headers, "", 20000) do
      {:ok, conn, 200, data} ->
        {:ok, conn, Jason.decode!(data)}

      {:ok, conn, code, data} ->
        :ok = close(conn)
        {:error, :http, code, data}
    end
  end

  @spec get_binary_records(Http.t(), %{}) :: {:ok, Http.t(), [%{}]} | http_error()
  def get_binary_records(conn, opts) do
    query = URI.encode_query(opts)

    headers = [
      {"Content-Type", "application/vnd.kafka.v2+json"},
      {"Accept", "application/vnd.kafka.binary.v2+json"}
    ]

    case Http.request(conn, "GET", "records?#{query}", headers, "", 20000) do
      {:ok, conn, 200, json} ->
        data =
          json
          |> Jason.decode!()
          |> Enum.map(fn e -> %{e | "value" => :base64.decode(e["value"])} end)

        {:ok, conn, data}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @spec create_consumer(Kashka.Http.t(), String.t(), %{}) ::
          {:ok, Kashka.Http.t(), %{}} | {:error, :exists} | http_error()
  def create_consumer(conn, consumer_group, opts) do
    body = Jason.encode!(opts)
    path = Path.join(["consumers", consumer_group])

    case Http.request(conn, "POST", path, @consumer_headers, body) do
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
    with {:ok, conn, _} <- request(conn, "DELETE", "", @consumer_headers, "") do
      {:ok, conn}
    end
  end

  @spec delete_consumer(Kashka.Http.t(), String.t(), String.t()) ::
          {:ok, Kashka.Http.t()} | http_error()
  def delete_consumer(conn, group, name) do
    path = Path.join(["consumers", group, "instances", name])

    with {:ok, conn, _} <- request(conn, "DELETE", path, @consumer_headers, "") do
      {:ok, conn}
    end
  end

  def subscribe(conn, topics) when is_list(topics) do
    data = Jason.encode!(%{"topics" => topics})

    with {:ok, conn, _} <- request(conn, "POST", "subscription", @consumer_headers, data) do
      {:ok, conn}
    end
  end

  def close(conn) do
    Http.close(conn)
  end

  @spec request(Http.t(), String.t(), String.t(), Mint.Types.headers(), iodata(), non_neg_integer) ::
          {:ok, Http.t(), iodata()} | http_error()
  defp request(conn, method, path, headers, body, timeout \\ 20000) do
    case Http.request(conn, method, path, headers, body, timeout) do
      {:ok, conn, code, data} when code >= 200 and code < 300 ->
        {:ok, conn, data}

      {:ok, conn, code, data} ->
        :ok = close(conn)
        {:error, :http, code, data}
    end
  end
end
