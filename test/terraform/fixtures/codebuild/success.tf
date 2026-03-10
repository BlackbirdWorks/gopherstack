resource "aws_codebuild_project" "this" {
  name        = "tf-project-{{.Suffix}}"
  description = "Terraform test project"
  service_role = "arn:aws:iam::000000000000:role/codebuild-role"

  artifacts {
    type = "NO_ARTIFACTS"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:1.0"
    type         = "LINUX_CONTAINER"
  }

  source {
    type = "NO_SOURCE"
    buildspec = "version: 0.2\nphases:\n  build:\n    commands:\n      - echo Hello"
  }
}
