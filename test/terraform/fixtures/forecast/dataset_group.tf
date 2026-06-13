resource "aws_forecast_dataset_group" "this" {
  dataset_group_name = "{{.DatasetGroupName}}"
  domain             = "CUSTOM"

  tags = {
    Environment = "test"
  }
}
