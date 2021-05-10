# BroadwaySQSExample

## Instructions

  1. Make sure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` env vars are set.
  2. Run `mix deps.get`
  3. Run tests `mix test`
  4. Run `iex -S mix`
  5. Send some messages to SQS for processing with `BroadwaySQSExample.Helpers.send_strings()`
  6. Send some messages to SQS for processing with `BroadwaySQSExample.Helpers.send_ints()`
