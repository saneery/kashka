defmodule Kashka.MixProject do
  use Mix.Project

  def project do
    [
      app: :kashka,
      version: "0.3.1",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      source_url: "https://github.com/funbox/kashka",
      deps: deps(),
      docs: docs(),
      description: "Elixir kafka rest proxy client for Confluent REST Proxy",
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:castore, "~> 0.1.0"},
      {:mint, "~> 1.0"},
      {:jason, "~> 1.1"},
      {:ex_doc, "~> 0.28.4", only: :dev, runtime: false},
      {:earmark, "~> 1.4", only: :dev, runtime: false},
      {:dialyxir, "~> 0.5.1", only: :dev},
      {:credo, "~> 1.1.2", only: :dev, runtime: false},
      {:excoveralls, "~> 0.11.2", only: :test},
      {:bypass, "~> 1.0", only: :test}
    ]
  end

  defp docs do
    [
      source_url_pattern: "https://github.com/funbox/kashka",
      main: "readme",
      extras: ["README.md"]
    ]
  end

  defp package do
    [
      name: :kashka,
      files: ["lib", "mix.exs", "README*"],
      maintainers: ["FunBox"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/funbox/kashka"
      }
    ]
  end
end
