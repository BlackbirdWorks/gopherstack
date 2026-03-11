resource "aws_efs_file_system" "this" {
  creation_token   = "{{.CreationToken}}"
  performance_mode = "generalPurpose"

  tags = {
    Name        = "{{.CreationToken}}"
    Environment = "test"
  }
}
