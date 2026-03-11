resource "aws_codecommit_repository" "this" {
  repository_name = "{{.RepositoryName}}"
  description     = "Test repository"

  tags = {
    Environment = "test"
  }
}
