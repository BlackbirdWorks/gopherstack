resource "aws_media_store_container" "this" {
  name = "{{.ContainerName}}"

  tags = {
    Environment = "test"
  }
}
