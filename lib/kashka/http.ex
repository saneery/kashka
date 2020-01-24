defmodule Kashka.Http do
  @moduledoc """
  Specialized wrapper around Mint library
  """

  @https_connect_opts [transport_opts: [verify: :verify_none]]

  defmodule TimeoutException do
    defexception message: "Kafka Rest API response timeout"
  end

  alias Mint.HTTP

  require Logger

  defstruct uri: nil,
            mint: nil,
            headers: [],
            fix_schema: false,
            fix_port: false,
            fix_host: false

  @type t :: %Kashka.Http{
          uri: URI.t(),
          mint: Mint.HTTP.t(),
          headers: [],
          fix_schema: boolean(),
          fix_port: boolean(),
          fix_host: boolean()
        }

  @type args :: String.t() | Keyword.t()

  @spec close(t()) :: :ok
  def close(%__MODULE__{mint: conn}) do
    {:ok, _} = HTTP.close(conn)
    :ok
  end

  @spec reconnect_to(t(), String.t()) :: %__MODULE__{}
  def reconnect_to(%__MODULE__{uri: uri, mint: conn} = state, url) do
    HTTP.close(conn)

    s = %{state | mint: nil, uri: URI.parse(url)}
    s = if state.fix_schema, do: %{s | uri: %{s.uri | scheme: uri.scheme}}, else: s
    s = if state.fix_port, do: %{s | uri: %{s.uri | port: uri.port}}, else: s

    s =
      if state.fix_host do
        %{s | uri: %{s.uri | host: uri.host}, headers: set_header(s.headers, "host", s.uri.host)}
      else
        s
      end

    %{s | mint: mint_connect(s.uri)}
  end

  def append_path(%__MODULE__{uri: uri} = state, path) do
    %{state | uri: %{uri | path: Path.join(uri.path || "/", path)}}
  end

  @spec request(
          t() | args(),
          String.t(),
          String.t(),
          Mint.Types.headers(),
          iodata(),
          non_neg_integer
        ) ::
          {:ok, t(), non_neg_integer(), iodata()}
  def request(state, method, path, headers, body, timeout \\ 20000)

  def request(%__MODULE__{uri: uri, mint: conn} = st, method, path, headers, body, timeout) do
    full_path = Path.join(uri.path || "/", path)
    Logger.debug(fn -> "Send #{method} to #{full_path} with body #{body}" end)

    new_headers =
      st.headers
      |> Enum.reduce(headers, fn {k, v}, acc -> Mint.Core.Util.put_new_header(acc, k, v) end)
      |> Mint.Core.Util.put_new_header("host", uri.authority)

    case HTTP.request(conn, method, full_path, new_headers, body) do
      {:ok, conn, _request_ref} ->
        case receive_all_response(conn, timeout) do
          {:ok, conn, response} ->
            {:ok, status, body} = get_status_and_body(response)
            st = %{st | mint: conn}
            close(st)
            {:ok, st, status, body}

          {:error, conn, %Mint.HTTPError{reason: {:server_closed_connection, _, _}} = error, _} ->
            Logger.info("server_closed_connection error: #{inspect(error)}. Retry")
            request(%{st | mint: mint_connect(st.uri)}, method, path, headers, body, timeout)
        end

      {:error, _conn, %Mint.HTTPError{reason: :closed}} ->
        request(%{st | mint: mint_connect(st.uri)}, method, path, headers, body, timeout)

      {:error, _conn, %Mint.TransportError{reason: :closed}} ->
        request(%{st | mint: mint_connect(st.uri)}, method, path, headers, body, timeout)
    end
  end

  def request(%__MODULE__{} = st, method, path, headers, body, timeout) do
    request(st, method, path, headers, body, timeout)
  end

  def request(smth, method, path, headers, body, timeout) do
    connect(smth)
    |> request(method, path, headers, body, timeout)
  end

  @spec connect(args()) :: t()
  def connect(smth) do
    st = build_state(smth)
    %{st | mint: mint_connect(st.uri)}
  end

  defp mint_connect(uri) do
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
        raise TimeoutException, message: "Kafka Rest API response timeout (#{timeout} ms)"
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

  defp build_state(url) when is_binary(url) do
    build_state(url: url)
  end

  defp build_state(args) when is_list(args) do
    %__MODULE__{
      uri: Keyword.get(args, :url, []) |> URI.parse(),
      headers: Keyword.get(args, :headers, []),
      fix_host: Keyword.get(args, :fix_host, false),
      fix_port: Keyword.get(args, :fix_port, false),
      fix_schema: Keyword.get(args, :fix_schema, false)
    }
  end

  defp set_header(headers, name, value) do
    :lists.keystore(name, 1, headers, {name, value})
  end
end
