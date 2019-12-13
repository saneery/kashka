defmodule Kashka.HttpTest do
  use ExUnit.Case

  alias Kashka.Http

  test "http_headers" do
    Bypass.open(port: 8811)
    |> Bypass.expect(fn conn ->
      ["ya.ru"] = Plug.Conn.get_req_header(conn, "host")
      Plug.Conn.resp(conn, 200, "")
    end)

    assert {:ok, conn, 200, body} = Http.request("http://127.0.0.1:8811", "GET", "v1", [{"host","ya.ru"}], "")
    assert {:ok, conn, 200, body} = Http.request({"http://127.0.0.1:8811", [{"host","ya.ru"}]}, "GET", "v1", [], "")
  end
end
