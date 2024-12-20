defmodule ElixirProject.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixir_project,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Specify your application's dependencies
  defp deps do
    [
      {:postgrex, "~> 0.16.5"} # PostgreSQL driver for Elixir
    ]
  end
end
