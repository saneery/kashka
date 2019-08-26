defmodule Kashka.KafkaTest do
  use ExUnit.Case

  alias Kashka.Kafka

  @url "http://localhost:8082/"

  setup do
    {:ok, conn, topic} = get_new_topic(@url)
    Kafka.delete_consumer(conn, "consumer_group", "my")
    [conn: conn, topic: topic]
  end

  test "creating consumer" do
    assert {:ok, conn, %{"base_uri" => uri}} =
             Kafka.create_consumer(@url, "consumer_group", %{name: "my", format: "json"})

    assert {:ok, conn} = Kafka.delete_consumer(conn, "consumer_group", "my")
    assert :ok == Kafka.close(conn)
  end

  test "produce json", %{topic: topic} do
    assert {:ok, conn} = Kafka.produce_json(@url, topic, [%{value: %{foo: "bar"}}])
    assert :ok == Kafka.close(conn)
  end

  test "produce_binary", %{topic: topic} do
    assert {:ok, conn} = Kafka.produce_json(@url, topic, [%{value: "123456"}])
    assert :ok == Kafka.close(conn)
  end

  test "delete consumers", %{conn: conn} do
    assert {:ok, conn, %{"base_uri" => uri}} =
             Kafka.create_consumer(conn, "consumer_group", %{name: "my", format: "json"})

    assert {:ok, _} = Kafka.delete_consumer(uri)
  end

  describe "subscribed json consumer" do
    setup %{conn: conn, topic: topic} do
      assert {:ok, conn} = Kafka.produce_json(conn, topic, [%{value: %{foo: "bar"}}])

      assert {:ok, conn, %{"base_uri" => uri}} =
               Kafka.create_consumer(conn, "consumer_group", %{name: "my", format: "json"})

      {:ok, conn} = Kafka.subscribe(uri, [topic])
      {:ok, conn, []} = Kafka.get_records(conn, %{timeout: 0})
      [conn: conn]
    end

    test "assignments", %{conn: conn, topic: topic} do
      {:ok, _conn, [%{"partition" => 0, "topic" => ^topic}]} = Kafka.assignments(conn)
    end

    test "getting offsets", %{conn: conn, topic: topic} do
      partitions = [%{"partition" => 0, "topic" => topic}]

      {:ok, conn, []} = Kafka.get_records(conn, %{timeout: 0})
      {:ok, conn} = Kafka.commit(conn)

      {:ok, _conn, [%{"partition" => 0, "topic" => ^topic, "offset" => offset}]} =
        Kafka.offsets(conn, partitions)

      assert is_number(offset) and offset >= 0
    end

    test "positions_end", %{conn: conn, topic: topic} do
      partitions = [%{"partition" => 0, "topic" => topic}]
      Kafka.positions_end(conn, partitions)
    end

    test "get_records json", %{conn: conn, topic: topic} do
      {:ok, conn, _} = Kafka.get_records(conn, %{timeout: 0})
      {:ok, conn} = Kafka.commit(conn)

      {:ok, c} = Kafka.produce_json(@url, topic, [%{value: %{foo: "bar"}}])
      Kafka.close(c)

      {:ok, _conn, [record]} = Kafka.get_records(conn, %{timeout: 0})

      assert %{
               "key" => nil,
               "offset" => _,
               "partition" => 0,
               "topic" => ^topic,
               "value" => %{"foo" => "bar"}
             } = record
    end
  end

  def get_new_topic(conn, prefix \\ "test") do
    regex = Regex.compile!(prefix <> "(\\d+)")
    {:ok, conn, topics} = Kafka.topics(@url)

    last_number =
      topics
      |> Enum.map(fn e ->
        case Regex.run(regex, e) do
          nil -> nil
          [_, num | _] -> num
        end
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> List.last()

    last_number = last_number || 0

    {:ok, conn, "#{prefix}#{last_number + 1}"}
  end

  describe "subscribed binary consumer" do
    setup %{conn: conn, topic: topic} do
      assert {:ok, conn, %{"base_uri" => uri}} =
               Kafka.create_consumer(conn, "consumer_group", %{name: "my", format: "binary"})

      {:ok, conn} = Kafka.subscribe(uri, [topic])
      {:ok, conn, []} = Kafka.get_binary_records(conn, %{timeout: 0})
      [conn: conn]
    end

    test "get_records binary", %{conn: conn, topic: topic} do
      {:ok, conn, _} = Kafka.get_binary_records(conn, %{timeout: 0})
      {:ok, conn} = Kafka.commit(conn)

      {:ok, c} = Kafka.produce_binary(@url, topic, [%{value: "123"}])
      Kafka.close(c)

      {:ok, _conn, [record]} = Kafka.get_binary_records(conn, %{timeout: 0})

      assert %{
               "key" => nil,
               "offset" => _,
               "partition" => 0,
               "topic" => ^topic,
               "value" => "123"
             } = record
    end
  end
end
