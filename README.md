# KafkaRest

Elixir kafka rest proxy client https://docs.confluent.io/current/kafka-rest/index.html

## Testing

Run zookeeper, kafka and kafka rest proxy with `docker-compose up`. Then run `mix test`

## Examples

See tests for examples

## TODO

* Make protection from endless loop in Kashka.Http.request
* Correctly process other errors in Kashka.Http.request
* Throw elixir error in place of `{:mint_helper, :timeout}`
* Add documentation

[![Sponsored by FunBox](https://funbox.ru/badges/sponsored_by_funbox_compact.svg)](https://funbox.ru)
