resource "aws_codeartifact_domain" "this" {
  domain = "{{.DomainName}}"

  tags = {
    Environment = "test"
  }
}

resource "aws_codeartifact_repository" "this" {
  repository = "{{.RepositoryName}}"
  domain     = aws_codeartifact_domain.this.domain

  description = "Test repository"
}
