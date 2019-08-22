defmodule Kashka.KafkaTest do
  use ExUnit.Case

  alias Kashka.Kafka

  @url "http://localhost:8082/"

  setup do
    Kafka.delete_consumer(@url, "consumer_group", "my")
    :ok
  end

  test "creating consumer" do
    assert {:ok, conn, %{"base_uri" => uri}} =
             Kafka.create_consumer(@url, "consumer_group", %{name: "my", format: "json"})

    assert {:ok, conn} = Kafka.delete_consumer(conn, "consumer_group", "my")
    assert :ok == Kafka.close(conn)
  end

  test "produce json" do
    assert {:ok, conn} = Kafka.produce_json(@url, "json", [%{value: %{foo: "bar"}}])
    assert :ok == Kafka.close(conn)
  end

  test "delete consumers" do
      assert {:ok, conn, %{"base_uri" => uri}} =
               Kafka.create_consumer(@url, "consumer_group", %{name: "my", format: "json"})
      assert {:ok, _} = Kafka.delete_consumer(uri)
  end

  describe "subscribed consumers" do
    setup do
      assert {:ok, conn, %{"base_uri" => uri}} =
               Kafka.create_consumer(@url, "consumer_group", %{name: "my", format: "json"})
      {:ok, conn} = Kafka.subscribe(uri, ["json"])
      {:ok, conn, []} = Kafka.get_records(conn, %{timeout: 0})
      [conn: conn]
    end

    test "assignments", %{conn: conn} do
      {:ok, _conn, [%{"partition" => 0, "topic" => "json"}]} = Kafka.assignments(conn)
    end

    test "getting offsets", %{conn: conn} do
      partitions = [%{"partition" => 0, "topic" => "json"}]

      {:ok, _conn, [%{"partition" => 0, "topic" => "json", "offset" => offset}]} =
        Kafka.offsets(conn, partitions)

      assert is_number(offset) and offset >= 0
    end

    test "positions_end", %{conn: conn} do
      partitions = [%{"partition" => 0, "topic" => "json"}]
      Kafka.positions_end(conn, partitions)
    end

    test "get_records", %{conn: conn} do
      {:ok, conn, _} = Kafka.get_records(conn, %{timeout: 0})
      {:ok, conn} = Kafka.commit(conn)

      {:ok, c} = Kafka.produce_json(@url, "json", [%{value: %{foo: "bar"}}])
      Kafka.close(c)

      {:ok, _conn, [record]} = Kafka.get_records(conn, %{timeout: 0})

      assert %{
               "key" => nil,
               "offset" => _,
               "partition" => 0,
               "topic" => "json",
               "value" => %{"foo" => "bar"}
             } = record
    end
  end
end
