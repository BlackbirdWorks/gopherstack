resource "aws_resourcegroups_group" "this" {
  name = "{{.GroupName}}"

  resource_query {
    query = jsonencode({
      ResourceTypeFilters = ["AWS::AllSupported"]
      TagFilters = [{
        Key    = "env"
        Values = ["test"]
      }]
    })
  }
}
