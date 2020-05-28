defmodule Kashka.HttpTest do
  use ExUnit.Case

  alias Kashka.Http

  setup do
    me = self()

    Bypass.open(port: 8811)
    |> Bypass.expect(fn conn ->
      headers = Plug.Conn.get_req_header(conn, "host")
      Process.send(me, %{headers: headers, path: conn.request_path}, [])
      Plug.Conn.resp(conn, 200, "")
    end)
  end

  test "http_headers" do
    assert {:ok, conn, 200, body} =
             Http.request("http://127.0.0.1:8811", "GET", "v1", [{"host", "smth1.com"}], "")

    assert_receive %{headers: ["smth1.com"]}

    assert {:ok, conn, 200, body} =
             Http.request(
               [url: "http://127.0.0.1:8811", headers: [{"host", "smth2.com"}]],
               "GET",
               "v1",
               [],
               ""
             )

    assert_receive %{headers: ["smth2.com"]}

    assert {:ok, conn, 200, body} =
             Http.request(
               [url: "http://127.0.0.1:8811", headers: [{"host", "smth2.com"}]],
               "GET",
               "v1",
               [{"host", "smth2.com"}],
               ""
             )

    assert_receive %{headers: ["smth2.com"]}
  end

  test "ip arg" do
    url = [url: "http://127.0.0.1:8811", fix_host: true, headers: [{"host", "smth1.com"}]]
    assert {:ok, conn, 200, body} = Http.request(url, "GET", "v1", [], "")
    assert_receive %{headers: ["smth1.com"]}

    Http.reconnect_to(conn, "http://smth2.com:8811")
    |> Http.request("GET", "v1", [], "")

    assert_receive %{headers: ["smth2.com"]}
  end

  test "append_path" do
    url = "http://127.0.0.1:8811"
    assert {:ok, conn, 200, body} = Http.request(url, "GET", "v1", [], "")
    assert_receive %{headers: ["127.0.0.1:8811"], path: "/v1"}

    assert {:ok, conn, 200, body} =
             conn
             |> Http.append_path("smth")
             |> Http.request("GET", "v1", [], "")

    assert_receive %{headers: ["127.0.0.1:8811"], path: "/smth/v1"}
  end
end
