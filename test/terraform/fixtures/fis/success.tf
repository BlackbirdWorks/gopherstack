resource "aws_fis_experiment_template" "this" {
  description = "tf-fis-{{.Suffix}}"
  role_arn    = "arn:aws:iam::000000000000:role/fis-role"

  stop_condition {
    source = "none"
  }

  action {
    name      = "wait"
    action_id = "aws:fis:wait"

    parameter {
      key   = "duration"
      value = "PT5S"
    }
  }

  tags = {
    Environment = "test"
  }
}
