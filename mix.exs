defmodule BroadwaySqs.MixProject do
  use Mix.Project

  @version "0.1.0"
  @description "A SQS connector for Broadway"

  def project do
    [
      app: :broadway_sqs,
      version: @version,
      elixir: "~> 1.5",
      name: "BroadwaySQS",
      description: @description,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
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
      {:broadway, git: "https://github.com/plataformatec/broadway.git"},
      {:ex_aws_sqs, "~> 2.0"},
      {:hackney, "~> 1.9", only: [:dev]},
      {:sweet_xml, "~> 0.6"},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "BroadwaySQS.Producer",
      source_ref: "v#{@version}",
      source_url: "https://github.com/plataformatec/broadway_sqs"
    ]
  end

  defp package do
    %{
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/plataformatec/broadway_sqs"}
    }
  end
end
