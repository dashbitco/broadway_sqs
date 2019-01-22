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

## License

Copyright 2019 Plataformatec

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.