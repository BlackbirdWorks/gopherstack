resource "aws_ecrpublic_repository" "this" {
  repository_name = "{{.RepositoryName}}"

  catalog_data {
    about_text        = "About {{.RepositoryName}}"
    architectures     = ["ARM"]
    description       = "Test repository for {{.RepositoryName}}"
    operating_systems = ["Linux"]
    usage_text        = "Usage instructions for {{.RepositoryName}}"
  }

  tags = {
    Environment = "test"
    Owner       = "terraform"
  }
}

resource "aws_ecrpublic_repository_policy" "this" {
  repository_name = aws_ecrpublic_repository.this.repository_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowPull"
        Effect    = "Allow"
        Principal = "*"
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
        ]
      }
    ]
  })
}
