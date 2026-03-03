resource "aws_kinesis_stream" "this" {
  name        = "{{.StreamName}}"
  shard_count = 1
}
