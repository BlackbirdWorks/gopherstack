resource "aws_athena_workgroup" "this" {
  name        = "{{.WorkGroupName}}"
  description = "Test workgroup"

  configuration {
    result_configuration {
      output_location = "s3://my-bucket/prefix/"
    }
  }

  tags = {
    Environment = "test"
  }
}
