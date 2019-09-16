defmodule Kashka.GenConsumerTest do
  use ExUnit.Case

  alias Kashka.Kafka
  alias Kashka.GenConsumer

  import KashkaTest.Helpers

  @url "http://localhost:8082/"

  defmodule TestModule do
    @behaviour Kashka.GenConsumer

    def init(_) do
      :ok
    end

    def handle_message_set(conn, state, message_set) do
      Process.whereis(:test_process)
      |> send(message_set)

      {:ok, conn, :state}
    end
  end

  setup do
    {:ok, conn, topic} = get_new_topic(@url)
    Kafka.delete_consumer(conn, "consumer_group", "my")
    Process.register(self(), :test_process)
    [conn: conn, topic: topic]
  end

  test "creating consumer", %{conn: conn, topic: topic} do
    assert {:ok, conn} = Kafka.produce(conn, topic, [%{value: %{foo: "bar"}}])

    GenConsumer.start_link(
      url: @url,
      name: "my",
      consumer_group: "consumer_group",
      topics: [topic],
      module: TestModule
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
  end
end
