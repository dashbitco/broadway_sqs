# Changelog

## v0.6.0-dev

  * Implement `prepare_for_draining/1` to make sure no more messages will be fetched after draining

## v0.5.0 (2019-11-05)

  * Update to Broadway v0.5.0

## v0.4.0 (2019-09-26)

  * Replace option `:queue_name` with `:queue_url` to keep compatibility with ex_aws_sqs >= v3.0.0

## v0.3.0 (2019-09-18)

  * Update `ex_aws` dependency to `~> 3.0`
  * Update `broadway` dependency to `~> 0.4.0`

## v0.2.0 (2019-04-26)

  * Automatically add `message_id`, `receipt_handle` and `md5_of_body` to the message's metadata
  * New option `:attribute_names`
  * New option `:message_attribute_names`
  * New option `:visibility_timeout`

## v0.1.0 (2019-02-19)

  * Initial release
