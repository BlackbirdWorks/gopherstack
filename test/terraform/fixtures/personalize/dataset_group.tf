resource "aws_personalize_dataset_group" "this" {
  name = "{{.DatasetGroupName}}"

  tags = {
    Environment = "test"
  }
}
