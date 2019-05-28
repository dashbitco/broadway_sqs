# BroadwaySQSExample

## Instructions

  1. Make sure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` env vars are set.
  2. run `mix deps.get`
  3. run tests `mix test`
  4. run `iex -S mix`
  5. send some messages to SQS for processing with `BroadwaySQSExample.Helpers.send_strings()`
  6. send some messages to SQS for processing with `BroadwaySQSExample.Helpers.send_ints()`
