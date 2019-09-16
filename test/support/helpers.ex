defmodule KashkaTest.Helpers do
  alias Kashka.Kafka

  def get_new_topic(conn, prefix \\ "test") do
    regex = Regex.compile!(prefix <> "(\\d+)")
    {:ok, conn, topics} = Kafka.topics(conn)

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
end
