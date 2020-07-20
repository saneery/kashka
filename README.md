# Kashka

Elixir Kafka REST proxy client for [Confluent REST Proxy](https://docs.confluent.io/current/kafka-rest/index.html).

## Examples

### Produce

```elixir
{:ok, conn} = Kashka.Kafka.produce("http://localhost:8082/", "topic_name', [%{value: %{foo: "bar"}}])
:ok == Kashka.Kafka.close(conn)
```

See `Kashka.Kafka.produce/4` for details.

### Consume

```elixir
defmodule TestModule do
  @behaviour Kashka.GenConsumer

  def init(conn, _args) do
    {:ok, conn, :state}
  end

  def handle_message_set(conn, :state, message_set) do
    IO.inspect(message_set)

    #[%{"key" => nil, "offset" => 0, "partition" => 0, "topic" => "test_name", "value" => %{"foo" => "bar"}}]

    {:ok, conn, :state}
  end
end

args = [
  url: "http://localhost:8082/",
  instance_id: "my",
  consumer_group: "consumer_group",
  topics: ["topic_name"],
  module: TestModule,
  delete_on_exists: false,
  retry_on_exists: true,
  consumer_opts: %{"auto.offset.reset": :earliest, "auto.commit.enable": true},
]

{:ok, pid} = GenConsumer.start_link(args)
```

See `Kashka.GenConsumer` for details.

### More

See [tests](https://github.com/funbox/kashka/blob/master/test/kashka/gen_consumer_test.exs) for more examples.

### Kafka URL explanation

Kafka URL can be a String or a Keyword list. Keyword list can contain `url`, `headers`, `fix_schema`, `fix_port`, `fix_host` keys.

Examples:

```elixir
[url: "http://127.0.0.1:8811", headers: [{"host", "smth.com"}]]
```

With `fix_* keys`:

```elixir
[url: "http://127.0.0.1", fix_port: true, fix_schema: true, fix_host: true, headers: [{"host", "smth.com"}]]
```

`fix_*` keys used to automatically preprocess the URL returned from the consumer creation method. 

For example if `create_consumer` method returns 
`https://smth1.com:443/comsumers/group/instances/name` as a `base_uri`, 
it is transformed to `http://127.0.0.1:80/comsumers/group/instances/name` with host header `smth1.com`.

It can be helpful while connecting to Kafka REST API through Nginx proxy.

## How to run tests

1. Run `doker-compose up`.
2. Run `mix test` in separate terminal.

## Roadmap

* Make protection from endless loop in `Kashka.Http.request`.
* Correctly process other errors in `Kashka.Http.request`.

[![Sponsored by FunBox](https://funbox.ru/badges/sponsored_by_funbox_centered.svg)](https://funbox.ru)
