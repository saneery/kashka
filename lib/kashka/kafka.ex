defmodule Kashka.Kafka do
  @consumer_headers [
    {"Content-Type", "application/vnd.kafka.v2+json"},
    {"Accept", "application/vnd.kafka.json.v2+json"}
  ]

  @proder_headers [
    {"Content-Type", "application/vnd.kafka.json.v2+json"},
    {"Accept", "application/vnd.kafka.v2+json"}
  ]

  alias Kashka.MintHelper
  require Logger

  def offsets(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    with {:ok, conn, code, data} <-
           MintHelper.request(conn, "GET", "offsets", @consumer_headers, data, 20000) do
      case code do
        200 ->
          offsets = Jason.decode!(data) |> Map.get("offsets")
          {:ok, conn, offsets}

        _ ->
          {:error, :http, code, data}
      end
    end
  end

  def assignments(conn) do
    with {:ok, conn, code, data} <-
           MintHelper.request(conn, "GET", "assignments", @consumer_headers, "", 20000) do
      case code do
        200 ->
          partitions = Jason.decode!(data) |> Map.get("partitions")
          {:ok, conn, partitions}

        _ ->
          {:error, :http, code, data}
      end
    end
  end

  def positions_end(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    with {:ok, conn, code, data} <-
           MintHelper.request(conn, "POST", "positions/end", @consumer_headers, data, 20000) do
      case code do
        204 ->
          {:ok, conn}

        _ ->
          {:error, :http, code, data}
      end
    end
  end

  def commit(conn, opts \\ nil) do
    body =
      case opts do
        nil -> ""
        _ -> Jason.encode!(opts)
      end

    with {:ok, conn, 200, _} <-
           MintHelper.request(conn, "POST", "offsets", @consumer_headers, body, 20000) do
      {:ok, conn}
    end
  end

  def produce_json(conn, topic, records) when is_list(records) do
    path = Path.join(["topics", topic])
    data = Jason.encode!(%{records: records})

    with {:ok, conn, code, data} <- MintHelper.request(conn, "POST", path, @proder_headers, data) do
      case code do
        200 ->
          {:ok, conn}

        _ ->
          {:error, :http, code, data}
      end
    end
  end

  def get_records(conn, opts) do
    query = URI.encode_query(opts)

    with {:ok, conn, 200, data} <-
           MintHelper.request(conn, "GET", "records?#{query}", @consumer_headers, "", 20000) do
      {:ok, conn, Jason.decode!(data)}
    end
  end

  def create_consumer(conn, consumer_group, opts) do
    body = Jason.encode!(opts)
    path = Path.join(["consumers", consumer_group])

    with {:ok, conn, code, data} <-
           MintHelper.request(conn, "POST", path, @consumer_headers, body) do
      case code do
        200 ->
          {:ok, conn, Jason.decode!(data)}

        409 ->
          {:error, :exists}

        _ ->
          {:error, :http, code, data}
      end
    end
  end

  def delete_consumer(conn) do
    with {:ok, conn, 204, _} <- MintHelper.request(conn, "DELETE", "", @consumer_headers, "") do
      {:ok, conn}
    end
  end

  def delete_consumer(conn, group, name) do
    path = Path.join(["consumers", group, "instances", name])
    with {:ok, conn, 204, _} <- MintHelper.request(conn, "DELETE", path, @consumer_headers, "") do
      {:ok, conn}
    end
  end

  def subscribe(conn, topics) when is_list(topics) do
    data = Jason.encode!(%{"topics" => topics})

    with {:ok, conn, 204, _} <-
           MintHelper.request(conn, "POST", "subscription", @consumer_headers, data) do
      {:ok, conn}
    end
  end

  def close(conn) do
    MintHelper.close(conn)
  end
end
