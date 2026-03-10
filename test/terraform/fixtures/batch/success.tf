resource "aws_batch_compute_environment" "this" {
  compute_environment_name = "tf-ce-{{.Suffix}}"
  type                     = "UNMANAGED"
}

resource "aws_batch_job_queue" "this" {
  name     = "tf-jq-{{.Suffix}}"
  state    = "ENABLED"
  priority = 1

  compute_environment_order {
    compute_environment = aws_batch_compute_environment.this.arn
    order               = 1
  }
}
