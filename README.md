# KafkaRest

Elixir kafka rest proxy client https://docs.confluent.io/current/kafka-rest/index.html

## Testing

Run zookeeper, kafka and kafka rest proxy with `docker-compose up`. Then run `mix test`

## Examples

See tests for examples

### Kafka URL explanation

Kafka URL can be a String or a Keyword list. Keyword list can contains url, headers, fix_schema, fix_port, fix_host keys.

Examples:

```
[url: "http://127.0.0.1:8811", headers: [{"host", "smth.com"}]]

[url: "http://127.0.0.1", fix_port: true, fix_schema: true, fix_host: true, headers: [{"host", "smth.com"}]]
```

`fix_*` keys can be used with automatically preprocess the url returned from create consumer method. For example url

```
"https://smth.com:443/comsumers/group/instances/name"
```

will be transformed to
```
"http://127.0.0.1:80/comsumers/group/instances/name" with host header "smth.com"
```

## How to run tests

# run `doker-compose up` in separate window
# run `mix test`

## TODO

* Make protection from endless loop in Kashka.Http.request
* Correctly process other errors in Kashka.Http.request
* Throw elixir error in place of `{:mint_helper, :timeout}`
* Add documentation

[![Sponsored by FunBox](https://funbox.ru/badges/sponsored_by_funbox_compact.svg)](https://funbox.ru)
