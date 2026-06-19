resource "aws_cleanrooms_collaboration" "this" {
  name                     = "{{.CollabName}}"
  description              = "{{.Description}}"
  creator_display_name     = "tf-test-creator"
  creator_member_abilities = ["CAN_QUERY", "CAN_RECEIVE_RESULTS"]
  query_log_status         = "ENABLED"

  tags = {
    Env = "test"
  }
}
