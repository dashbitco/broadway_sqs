# BroadwaySQS

A AWS SQS connector for [Broadway](https://github.com/plataformatec/broadway).

This project provides:

  * `BroadwaySQS.SQSProducer` - A GenStage producer that continuously receives messages from
    a SQS queue and acknowledge them after being successfully processed.
  * `BroadwaySQS.SQSClient` - A generic behaviour to implement SQS clients.
  * `BroadwaySQS.ExAwsClient` - Default SQS client used by `BroadwaySQS.SQSProducer`.


## Installation

Add `:broadway_sqs` to the list of dependencies in `mix.exs` along with `:broadway` and
the HTTP client of your choice (defaults to `:hackney`):

```elixir
def deps do
  [
    {:broadway, "~> 0.1.0"},
    {:broadway_sqs, "~> 0.1.0"},
    {:hackney, "~> 1.9"},
  ]
end
```

## Usage

Configure Broadway with one or more producers using `BroadwaySQS.SQSProducer`:

```elixir
Broadway.start_link(MyBroadway, %{},
  name: MyBroadway,
  producers: [
    default: [
      module: BroadwaySQS.SQSProducer,
      arg: [
        sqs_client: {BroadwaySQS.ExAwsClient, [
          queue_name: "my_queue",
        ]}
      ],
    ],
  ],
)
```

