defmodule Kashka.MintHelper do
  @connect_opts [transport_opts: [verify: :verify_none]]
  alias Mint.HTTP

  require Logger

  def close({_, conn}) do
    {:ok, _} = HTTP.close(conn)
    :ok
  end

  def request(state, method, path, headers, body, timeout \\ 20000)

  def request(uri, method, path, headers, body, timeout) when is_binary(uri) do
    request(URI.parse(uri), method, path, headers, body, timeout)
  end
  def request(%URI{} = uri, method, path, headers, body, timeout) do
    request({uri, nil}, method, path, headers, body, timeout)
  end

  def request({uri, nil}, method, path, headers, body, timeout) do
    request({uri, open(uri)}, method, path, headers, body, timeout)
  end

  def request({uri, conn}, method, path, headers, body, timeout) do
    full_path = Path.join(uri.path, path)
    Logger.debug("Send #{method} to #{full_path} with body #{body}")
    headers = [{"Host", uri.authority} | headers]

    case HTTP.request(conn, method, full_path, headers, body) do
      {:ok, conn, _request_ref} ->
        {:ok, conn, response} = receive_all_response(conn, timeout)
        {:ok, status, body} = get_status_and_body(response)
        {:ok, {uri, conn}, status, body}

      {:error, _conn, %Mint.HTTPError{reason: :closed}} ->
        request({uri, nil}, method, path, headers, body, timeout)
      {:error, _conn, %Mint.TransportError{reason: :closed}} ->
        request({uri, nil}, method, path, headers, body, timeout)
    end
  end

  defp open(uri) do
    Logger.debug("Going to reconnect")

    case uri.scheme do
      "http" ->
        {:ok, conn} = HTTP.connect(:http, uri.host, uri.port)
        conn

      "https" ->
        {:ok, conn} = HTTP.connect(:https, uri.host, uri.port, @connect_opts)
        conn
    end
  end

  def get_status_and_body(response) do
    with {:ok, code} <- get_status(response) do
      {:ok, code, get_data(response)}
    end
  end

  def receive_all_response(conn, timeout \\ 20000, data \\ []) do
    receive do
      {:ssl, _, _} = message ->
        process_data(conn, message, data, timeout)

      {:tcp, _, _} = message ->
        process_data(conn, message, data, timeout)
    after
      timeout ->
        :erlang.error({:mint_helper, :timeout})
    end
  end

  defp process_data(conn, message, data, timeout) do
    with {:ok, conn, mint_responses} <- HTTP.stream(conn, message) do
      case :lists.keyfind(:done, 1, mint_responses) do
        false ->
          receive_all_response(conn, timeout, [mint_responses | data])

        _ ->
          data =
            [mint_responses | data]
            |> Enum.reverse()
            |> List.flatten()

          {:ok, conn, data}
      end
    end
  end

  defp get_status(response) do
    case :lists.keyfind(:status, 1, response) do
      {:status, _, code} -> {:ok, code}
      false -> {:error, :status_not_found}
    end
  end

  defp get_data(response) do
    response
    |> Enum.filter(fn e -> :data == :erlang.element(1, e) end)
    |> Enum.map(&:erlang.element(3, &1))
  end
end
