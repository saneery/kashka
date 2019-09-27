defmodule Kashka.GenConsumerTest do
  use ExUnit.Case

  alias Kashka.Kafka
  alias Kashka.GenConsumer

  import KashkaTest.Helpers

  @url "http://localhost:8082/"

  defmodule TestModule do
    @behaviour Kashka.GenConsumer

    def init(conn, _) do
      {:ok, conn, :state}
    end

    def handle_message_set(conn, :state, message_set) do
      Process.whereis(:test_process)
      |> send(message_set)

      {:ok, conn, :state}
    end
  end

  defmodule TestModuleWithoutInit do
    @behaviour Kashka.GenConsumer

    def handle_message_set(conn, %{format: :json} = state, message_set) do
      Process.whereis(:test_process)
      |> send(message_set)

      {:ok, conn, state}
    end
  end

  setup do
    {:ok, conn, topic} = get_new_topic(@url)
    Kafka.consumer_path(conn, "consumer_group", "my") |> Kafka.delete_consumer
    Process.register(self(), :test_process)
    [conn: conn, topic: topic]
  end

  test "deletes old consumer while creating new one", %{topic: topic} do
    args = [
      url: @url,
      name: "my",
      consumer_group: "consumer_group",
      topics: [topic],
      module: TestModule,
      delete_on_exists: true
    ]

    {:ok, pid} = GenConsumer.start_link(args)
    {:ok, pid} = GenConsumer.start_link(args)
  end

  test "json consuming", %{conn: conn, topic: topic} do
    assert {:ok, conn} = Kafka.produce(conn, topic, [%{value: %{foo: "bar"}}], :json)

    {:ok, pid} =
      GenConsumer.start_link(
        url: @url,
        name: "my",
        consumer_group: "consumer_group",
        topics: [topic],
        module: TestModule,
        format: :json,
        consumer_opts: %{"auto.offset.reset": :earliest}
      )

    assert_receive [
                     %{
                       "key" => nil,
                       "offset" => 0,
                       "partition" => 0,
                       "topic" => ^topic,
                       "value" => %{"foo" => "bar"}
                     }
                   ],
                   5000

    GenServer.stop(pid)
  end

  test "binary consuming", %{conn: conn, topic: topic} do
    assert {:ok, conn} = Kafka.produce(conn, topic, [%{value: "test"}], :binary)

    {:ok, pid} =
      GenConsumer.start_link(
        url: @url,
        name: "my",
        consumer_group: "consumer_group",
        topics: [topic],
        module: TestModule,
        format: :binary,
        consumer_opts: %{"auto.offset.reset": :earliest}
      )

    assert_receive [
                     %{
                       "key" => nil,
                       "offset" => 0,
                       "partition" => 0,
                       "topic" => ^topic,
                       "value" => "test"
                     }
                   ],
                   5000

    GenServer.stop(pid)
  end

  test "consumer without init", %{conn: conn, topic: topic} do
    assert {:ok, conn} = Kafka.produce(conn, topic, [%{value: %{foo: "bar"}}], :json)

    {:ok, pid} =
      GenConsumer.start_link(
        url: @url,
        name: "my",
        consumer_group: "consumer_group",
        topics: [topic],
        module: TestModuleWithoutInit,
        format: :json,
        consumer_opts: %{"auto.offset.reset": :earliest}
      )

    assert_receive [
                     %{
                       "key" => nil,
                       "offset" => 0,
                       "partition" => 0,
                       "topic" => ^topic,
                       "value" => %{"foo" => "bar"}
                     }
                   ],
                   5000

    GenServer.stop(pid)
  end
end
