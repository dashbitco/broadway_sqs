use Mix.Config

config :broadway_sqs_example,
  int_queue: "TEST-int-queue",
  string_queue: "TEST-string-queue"

import_config "#{Mix.env()}.exs"
