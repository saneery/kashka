defmodule Kashka.Kafka do
  @moduledoc """
  Module to make direct requests to Kafka Rest Proxy.
  Use [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html) to figure out how to use
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

  @type partition :: %{
          topic: String.t() | atom(),
          partition: integer()
        }

  @type json_records :: %{
          required(:key) => String.t(),
          required(:value) => any(),
          optional(:partition) => integer()
        }

  @type binary_records :: %{
          required(:key) => String.t(),
          required(:value) => String.t(),
          optional(:partition) => integer()
        }

  @type records_query_params :: %{
          optional(:timeout) => integer(),
          optional(:max_bytes) => integer()
        }

  @type create_consumer_params :: %{
    optional(:name) => String.t(),
    optional(:format) => :binary | :json,
    optional(:"auto.offset.reset") => :earliest | :latest,
    optional(:"auto.commit.enable") => true | false,
    optional(:"fetch.min.bytes") => integer(),
    optional(:"consumer.request.timeout.ms") => integer(),
  }

  @doc """
  Requests all topics list
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#get--topics)

  ## Parameters

    - conn: connection or url for rest proxy
  """
  @spec topics(Http.conn()) :: {:ok, Http.t(), [String.t()]} | http_error()
  def topics(conn) do
    with {:ok, conn, data} <- request(conn, "GET", "topics", [@accept], "") do
      {:ok, conn, Jason.decode!(data)}
    end
  end

  @doc """
  Requests offsets for consumer.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-offsets)

  ## Parameters

    - conn: connection or url for created consumer
    - partitions: a list of partitions like `[%{topic: "test", partition: 0}]`
  """
  @spec offsets(Http.conn(), [partition()]) :: {:ok, Http.t(), [any()]} | http_error()
  def offsets(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    with {:ok, conn, data} <- request(conn, "GET", "offsets", [@accept], data) do
      offsets = Jason.decode!(data) |> Map.get("offsets")
      {:ok, conn, offsets}
    end
  end

  @doc """
  Get the list of partitions currently manually assigned to this consumer.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-assignments)

  ## Parameters

    - conn: connection or url for created consumer
  """
  @spec assignments(Http.t()) :: {:ok, Http.t(), [any()]} | http_error()
  def assignments(conn) do
    with {:ok, conn, data} <- request(conn, "GET", "assignments", [@accept], "") do
      partitions = Jason.decode!(data) |> Map.get("partitions")
      {:ok, conn, partitions}
    end
  end

  @doc """
  Seek to the last offset for each of the given partitions.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name)-instances-(string-instance)-positions-end)

  ## Parameters

    - conn: connection or url for created consumer
  """
  @spec positions_end(Http.t(), [partition()]) :: {:ok, Http.t()} | http_error()
  def positions_end(conn, partitions) do
    data = Jason.encode!(%{partitions: partitions})

    with {:ok, conn, _data} <- request(conn, "POST", "positions/end", [@content], data) do
      {:ok, conn}
    end
  end

  @doc """
  Commit a list of offsets for the consumer.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name)-instances-(string-instance)-offsets)

  ## Parameters

    - conn: connection or url for created consumer
    - offsets_or_records: offsets list or records returned from `get_records/3`
  """
  @spec commit(Http.conn(), [map()] | nil) :: {:ok, Http.t()} | http_error()
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

  @doc """
  Produce messages to a topic.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name))

  ## Parameters

    - conn: connection or url for rest proxy
    - topic: topic name
    - records: records list
    - format: records format json or binary
  """
  @spec produce(Http.conn(), String.t(), [json_records() | binary_records()], :json | :binary) :: {:ok, Http.t()} | http_error()
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

  @doc """
  Produce messages to a topic.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-records)

  ## Parameters

    - conn: connection or url for created consumer
    - opts: query Parameters
    - format: created consumer records format: json or binary
  """
  @spec get_records(Http.conn(), records_query_params(), :json | :binary) :: {:ok, Http.t(), [map()]} | http_error()
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

  @doc """
  Create a new consumer instance in the consumer group.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name))

  ## Parameters

    - conn: connection or url for created consumer
    - consumer_group: the name of the consumer group to join
    - opts: consumer settings

  Returns map with "base_uri" and "instance_id" fields. For example:
  ```
    %{
      "instance_id" => "my_consumer",
      "base_uri" => "http://proxy-instance.kafkaproxy.example.com/consumers/testgroup/instances/my_consumer"
    }
  ```
  """
  @spec create_consumer(Kashka.Http.t(), String.t(), create_consumer_params()) ::
          {:ok, Kashka.Http.t(), map()} | {:error, :exists} | http_error()
  def create_consumer(conn, consumer_group, opts) do
    body = Jason.encode!(opts)
    path = Path.join(["consumers", consumer_group])

    case Http.request(conn, "POST", path, [@content], body) do
      {:ok, conn, 200, json} ->
        {:ok, conn, Jason.decode!(json)}

      {:ok, conn, 409, _data} ->
        close(conn)
        {:error, :exists}

      {:ok, conn, code, data} ->
        close(conn)
        {:error, :http, code, data}
    end
  end

  @doc """
  Destroy the consumer instance.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#delete--consumers-(string-group_name)-instances-(string-instance))

  ## Parameters

    - conn: connection or url for created consumer
  """
  @spec delete_consumer(Kashka.Http.t()) :: {:ok, Kashka.Http.t()} | http_error()
  def delete_consumer(conn) do
    with {:ok, conn, _} <- request(conn, "DELETE", "", [@content], "") do
      {:ok, conn}
    end
  end

  @doc """
  Destroy the consumer instance.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#delete--consumers-(string-group_name)-instances-(string-instance)).
  This function mostly used in tests when there is only one kafka rest proxy instance.

  ## Parameters

    - conn: connection or url for rest proxy instance
    - group: consumer group name
    - name: consumer instance_id
  """
  @spec delete_consumer(Kashka.Http.t(), String.t(), String.t()) ::
          {:ok, Kashka.Http.t()} | http_error()
  def delete_consumer(conn, group, name) do
    extra_path = Path.join(["consumers", group, "instances", name])

    with {:ok, conn, _} <- request(conn, "DELETE", extra_path, [@content], "") do
      {:ok, conn}
    end
  end

  @doc """
  Subscribe to the given list of topics.
  See [Confluent REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name)-instances-(string-instance)-subscription)

  ## Parameters

    - conn: connection or url for rest proxy
    - topics: a list of topics to subscribe
  """
  @spec subscribe(Kashka.Http.t(), [String.t()]) :: {:ok, Kashka.Http.t()} | http_error
  def subscribe(conn, topics) when is_list(topics) do
    data = Jason.encode!(%{"topics" => topics})

    with {:ok, conn, _} <- request(conn, "POST", "subscription", [@content], data) do
      {:ok, conn}
    end
  end

  @doc """
  Build connection to exsisting consumer. 
  This function mostly used in tests when there is only one kafka rest proxy instance

  ## Parameters

    - conn: connection or url for rest proxy instance
    - group: consumer group name
    - name: consumer instance_id
  """
  @spec move_to_existing_consumer(Kashka.Http.t(), String.t(), String.t()) :: Kashka.Http.t()
  def move_to_existing_consumer(conn, group, name) do
    Http.append_path(conn, Path.join(["consumers", group, "instances", name]))
  end

  @doc """
  Close HTTP connection.
  """
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
