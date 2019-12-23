defmodule Kashka.KafkaTest do
  use ExUnit.Case

  alias Kashka.Kafka

  import KashkaTest.Helpers

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
    assert {:ok, conn} = Kafka.produce(@url, topic, [%{value: %{foo: "bar"}}])
    assert :ok == Kafka.close(conn)
  end

  test "produce_binary", %{topic: topic} do
    assert {:ok, conn} = Kafka.produce(@url, topic, [%{key: "123", value: "123456"}])
    assert :ok == Kafka.close(conn)
  end

  test "delete consumers", %{conn: conn} do
    assert {:ok, conn, %{"base_uri" => uri}} =
             Kafka.create_consumer(conn, "consumer_group", %{name: "my", format: "json"})

    assert {:ok, _} = Kafka.delete_consumer(uri)
  end

  test "delete existing consumers", %{conn: conn} do
    assert {:ok, conn, %{"base_uri" => uri}} =
             Kafka.create_consumer(conn, "consumer_group", %{name: "my", format: "json"})

    assert {:ok, _} =
             conn
             |> Kafka.move_to_existing_consumer("consumer_group", "my")
             |> Kafka.delete_consumer()
  end

  describe "subscribed json consumer" do
    setup %{conn: conn, topic: topic} do
      assert {:ok, conn} = Kafka.produce(conn, topic, [%{value: %{foo: "bar"}}])

      assert {:ok, conn, %{"base_uri" => uri}} =
               Kafka.create_consumer(conn, "consumer_group", %{name: "my", format: "json"})

      {:ok, conn} = Kafka.subscribe(uri, [topic])
      {:ok, conn, []} = Kafka.get_records(conn, %{timeout: 0})
      [conn: conn]
    end

    test "assignments", %{conn: conn, topic: topic} do
      {:ok, _conn, [%{"partition" => 0, "topic" => ^topic}]} = Kafka.assignments(conn)
    end

    test "commiting offsets manually", %{conn: conn, topic: topic} do
      {:ok, conn} = Kafka.commit(conn, [%{"offset" => 100, "partition" => 0, "topic" => topic}])

      {:ok, _conn, [%{"offset" => 101}]} =
        Kafka.offsets(conn, [%{"partition" => 0, "topic" => topic}])

      {:ok, conn} = Kafka.commit(conn, [%{offset: 200, partition: 0, topic: topic}])

      {:ok, _conn, [%{"offset" => 201}]} =
        Kafka.offsets(conn, [%{"partition" => 0, "topic" => topic}])

      {:ok, conn} =
        Kafka.commit(conn, [
          %{offset: 400, partition: 0, topic: topic},
          %{offset: 300, partition: 0, topic: topic}
        ])

      {:ok, _conn, [%{"offset" => 401}]} =
        Kafka.offsets(conn, [%{"partition" => 0, "topic" => topic}])
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

      {:ok, c} = Kafka.produce(@url, topic, [%{value: %{foo: "bar"}}])
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

  describe "subscribed binary consumer" do
    setup %{conn: conn, topic: topic} do
      assert {:ok, conn, %{"base_uri" => uri}} =
               Kafka.create_consumer(conn, "consumer_group", %{name: "my", format: "binary"})

      {:ok, conn} = Kafka.subscribe(uri, [topic])
      {:ok, conn, []} = Kafka.get_records(conn, %{timeout: 0}, :binary)
      [conn: conn]
    end

    test "get_records binary", %{conn: conn, topic: topic} do
      {:ok, conn, _} = Kafka.get_records(conn, %{timeout: 0}, :binary)
      {:ok, conn} = Kafka.commit(conn)

      {:ok, c} = Kafka.produce(@url, topic, [%{key: "321", value: "123"}], :binary)
      Kafka.close(c)

      {:ok, _conn, [record]} = Kafka.get_records(conn, %{timeout: 0}, :binary)

      assert %{
               "key" => "321",
               "offset" => _,
               "partition" => 0,
               "topic" => ^topic,
               "value" => "123"
             } = record
    end
  end
end
