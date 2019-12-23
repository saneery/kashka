defmodule Kashka.Kafka do
  @moduledoc """
  Module to make direct requests to Kafka Rest Proxy.
  Use https://docs.confluent.io/current/kafka-rest/api.html to figure out how to use
  """

  @content {"Content-Type", "application/vnd.kafka.v2+json"}
  @content_json {"Content-Type", "application/vnd.kafka.json.v2+json"}
  @content_binary {"Content-Type", "application/vnd.kafka.binary.v2+json"}

  @accept {"Accept", "application/vnd.kafka.v2+json"}
  @accept_json {"Accept", "application/vnd.kafka.json.v2+json"}
  @accept_binary {"Accept", "application/vnd.kafka.binary.v2+json"}

  alias Kashka.Http
  require Logger

  @type http_error :: {:error, :http, code :: non_neg_integer(), iodata()}

  @doc """
  Requests all topics list
  """
  @spec topics(Http.t()) :: {:ok, Http.t(), %{}} | http_error()
  def topics(conn) do
    with {:ok, conn, data} <- request(conn, "GET", "topics", [@accept], "") do
      {:ok, conn, Jason.decode!(data)}
    end
  end

  @doc """
  Requests offsets for consumer.
  See https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-offsets

  ## Parameters

    - conn: connection or url for created consumer
    - partitions: list of `%{topic: "test", partition: 0}` maps
  """
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

  @spec commit(Http.t(), %{} | nil) :: {:ok, Http.t()} | http_error()
  def commit(conn, offsets_or_records \\ nil) do
    body =
      case offsets_or_records do
        nil -> ""
        list when is_list(list) -> Jason.encode!(%{offsets: extract_offsets(list)})
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
        e
        |> Map.update(:value, nil, &Base.encode64/1)
        |> Map.update(:key, nil, &Base.encode64/1)
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
          e
          |> Map.update("value", nil, &decode64/1)
          |> Map.update("key", nil, &decode64/1)
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
    extra_path = Path.join(["consumers", group, "instances", name])

    with {:ok, conn, _} <- request(conn, "DELETE", extra_path, [@content], "") do
      {:ok, conn}
    end
  end

  @spec subscribe(Kashka.Http.t(), [%{}]) :: {:ok, Kashka.Http.t()} | http_error
  def subscribe(conn, topics) when is_list(topics) do
    data = Jason.encode!(%{"topics" => topics})

    with {:ok, conn, _} <- request(conn, "POST", "subscription", [@content], data) do
      {:ok, conn}
    end
  end

  @spec close(Kashka.Http.t()) :: :ok
  def close(conn) do
    Http.close(conn)
  end

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

  defp extract_offsets(records) do
    records
    |> Enum.reduce(%{}, fn m, acc ->
      key = {m["topic"] || m[:topic], m["partition"] || m[:partition]}
      old_offset = Map.get(acc, key, 0)
      new_offset = m["offset"] || m[:offset]
      Map.put(acc, key, Enum.max([old_offset, new_offset]))
    end)
    |> Enum.map(fn {{topic, partition}, offset} ->
      %{topic: topic, partition: partition, offset: offset}
    end)
  end

  defp decode64(nil), do: nil
  defp decode64(str), do: Base.decode64!(str)
end
