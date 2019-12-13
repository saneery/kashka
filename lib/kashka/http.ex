defmodule Kashka.Http do
  @https_connect_opts [transport_opts: [verify: :verify_none]]

  alias Mint.HTTP

  require Logger

  @type t :: {URI.t(), Mint.HTTP.t()} | URI.t() | String.t()

  @spec close(t()) :: :ok
  def close({_, conn}) do
    {:ok, _} = HTTP.close(conn)
    :ok
  end

  def path(uri) when is_binary(uri) do
    uri
  end

  def path(%URI{} = uri) do
    URI.to_string(uri)
  end

  def path({%URI{} = uri, _conn}) do
    URI.to_string(uri)
  end

  @spec request(t(), String.t(), String.t(), Mint.Types.headers(), iodata(), non_neg_integer) ::
          {:ok, t(), non_neg_integer(), iodata()}
  def request(state, method, path, headers, body, timeout \\ 20000)

  def request(uri, method, path, headers, body, timeout) when is_binary(uri) do
    request(URI.parse(uri), method, path, headers, body, timeout)
  end

  def request(%URI{} = uri, method, path, headers, body, timeout) do
    request({uri, open(uri)}, method, path, headers, body, timeout)
  end

  def request({%URI{} = uri, conn}, method, path, headers, body, timeout) do
    full_path = Path.join(uri.path || "/", path)
    Logger.debug(fn -> "Send #{method} to #{full_path} with body #{body}" end)
    new_headers = headers |> Mint.Core.Util.put_new_header("host", uri.authority)

    case HTTP.request(conn, method, full_path, new_headers, body) do
      {:ok, conn, _request_ref} ->
        {:ok, conn, response} = receive_all_response(conn, timeout)
        {:ok, status, body} = get_status_and_body(response)
        {:ok, {uri, conn}, status, body}

      {:error, _conn, %Mint.HTTPError{reason: :closed}} ->
        request(uri, method, path, headers, body, timeout)

      {:error, _conn, %Mint.TransportError{reason: :closed}} ->
        request(uri, method, path, headers, body, timeout)
    end
  end

  defp open(uri) do
    Logger.debug("Going to reconnect")

    case uri.scheme do
      "http" ->
        {:ok, conn} = HTTP.connect(:http, uri.host, uri.port)
        conn

      "https" ->
        {:ok, conn} = HTTP.connect(:https, uri.host, uri.port, @https_connect_opts)
        conn
    end
  end

  defp get_status_and_body(response) do
    with {:ok, code} <- get_status(response) do
      {:ok, code, get_data(response)}
    end
  end

  defp receive_all_response(conn, timeout, data \\ []) do
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
