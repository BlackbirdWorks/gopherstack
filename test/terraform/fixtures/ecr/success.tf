resource "aws_ecr_repository" "this" {
  name                 = "{{.RepoName}}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}
