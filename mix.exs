defmodule Kashka.MixProject do
  use Mix.Project

  def project do
    [
      app: :kashka,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mint, "~> 0.4.0"},
      {:jason, "~> 1.1"},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:dialyxir, "~> 0.5.1", only: :dev},
      {:credo, "~> 1.1.2", only: :dev, runtime: false},
      {:excoveralls, "~> 0.11.2", only: :test},
      {:bypass, "~> 1.0", only: :test}
    ]
  end
end
