defmodule Kashka.HttpTest do
  use ExUnit.Case

  alias Kashka.Http

  setup do
    me = self()

    Bypass.open(port: 8811)
    |> Bypass.expect(fn conn ->
      headers = Plug.Conn.get_req_header(conn, "host")
      Process.send(me, %{headers: headers}, [])
      Plug.Conn.resp(conn, 200, "")
    end)
  end

  test "http_headers" do
    assert {:ok, conn, 200, body} =
             Http.request("http://127.0.0.1:8811", "GET", "v1", [{"host", "ya1.ru"}], "")

    assert_receive %{headers: ["ya1.ru"]}

    assert {:ok, conn, 200, body} =
             Http.request(
               {"http://127.0.0.1:8811", headers: [{"host", "ya2.ru"}]},
               "GET",
               "v1",
               [],
               ""
             )

    assert_receive %{headers: ["ya2.ru"]}

    assert {:ok, conn, 200, body} =
             Http.request(
               {"http://127.0.0.1:8811", headers: [{"host", "ya2.ru"}]},
               "GET",
               "v1",
               [{"host", "ya2.ru"}],
               ""
             )

    assert_receive %{headers: ["ya2.ru"]}
  end

  test "ip arg" do
    url = {"http://127.0.0.1:8811", fix_host: true, headers: [{"host", "ya1.ru"}]}
    assert {:ok, conn, 200, body} = Http.request(url, "GET", "v1", [], "")
    assert_receive %{headers: ["ya1.ru"]}

    Http.reconnect_to(conn, "http://ya2.ru:8811")
    |> Http.request("GET", "v1", [], "")

    assert_receive %{headers: ["ya2.ru"]}

    # assert {:ok, conn, 200, body} =
    #          Http.request({"http://ya1.ru:8811", ip: "127.0.0.1"}, "GET", "v1", [], "")

    # assert_receive %{headers: ["ya1.ru"]}

    # Http.reconnect_to(conn, "http://ya2.ru:8811")
    # |> Http.request("GET", "v1", [], "")
    # assert_receive %{headers: ["ya2.ru"]}
  end
end
