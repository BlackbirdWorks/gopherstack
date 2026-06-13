resource "aws_detective_graph" "this" {
  tags = {
    Environment = "test"
    Name        = "{{.GraphName}}"
  }
}
