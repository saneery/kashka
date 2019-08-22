defmodule KashkaTest do
  use ExUnit.Case
  doctest Kashka

  test "greets the world" do
    assert Kashka.hello() == :world
  end
end
