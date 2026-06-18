resource "aws_rekognition_collection" "this" {
  collection_id = "{{.CollectionID}}"

  tags = {
    Environment = "test"
  }
}
