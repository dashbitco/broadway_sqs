defmodule BroadwaySqs.MixProject do
  use Mix.Project

  def project do
    [
      app: :broadway_sqs,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:broadway, git: "https://github.com/plataformatec/broadway.git"},
      {:ex_aws_sqs, "~> 2.0"},
      {:hackney, "~> 1.9", only: [:dev]},
      {:sweet_xml, "~> 0.6"}
    ]
  end
end
