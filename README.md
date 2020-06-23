# KafkaRest

Elixir kafka rest proxy client for [Confluent REST Proxy](https://docs.confluent.io/current/kafka-rest/index.html)

## Examples

### Produce
```
{:ok, conn} = Kafka.produce("http://localhost:8082/", "topic_name', [%{value: %{foo: "bar"}}])
:ok == Kafka.close(conn)
```
### Consume

```
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
  name: "my",
  consumer_group: "consumer_group",
  topics: ["topic_name"],
  module: TestModule,
  delete_on_exists: false,
  retry_on_exists: true,
  consumer_opts: %{"auto.offset.reset": :earliest, "auto.commit.enable": true},
]

{:ok, pid} = GenConsumer.start_link(args)
```

### More

See tests for more examples

### Kafka URL explanation

Kafka URL can be a String or a Keyword list. Keyword list can contains url, headers, fix_schema, fix_port, fix_host keys.

Examples:

```
[url: "http://127.0.0.1:8811", headers: [{"host", "smth.com"}]]

```

With `fix_* keys`:
```
[url: "http://127.0.0.1", fix_port: true, fix_schema: true, fix_host: true, headers: [{"host", "smth.com"}]]
```

`fix_*` keys used to automatically preprocess the url returned from create consumer method. For example if create_consumer method returns 
`https://smth1.com:443/comsumers/group/instances/name` as a base_uri, it is transformed to `http://127.0.0.1:80/comsumers/group/instances/name` with host header `smth1.com`

It can be helpfull while connecting to kafka resp api through nginx proxy

## How to run tests

* run `doker-compose up` in separate window
* run `mix test`

## TODO

* Make protection from endless loop in Kashka.Http.request
* Correctly process other errors in Kashka.Http.request
* Add documentation

[![Sponsored by FunBox](https://funbox.ru/badges/sponsored_by_funbox_compact.svg)](https://funbox.ru)
