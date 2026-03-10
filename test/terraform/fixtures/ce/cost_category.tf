resource "aws_ce_cost_category" "this" {
  name         = "{{.CategoryName}}"
  rule_version = "CostCategoryExpression.v1"

  rule {
    value = "Engineering"
    rule {
      dimension {
        key           = "LINKED_ACCOUNT"
        values        = ["123456789012"]
        match_options = ["EQUALS"]
      }
    }
    type = "REGULAR"
  }

  tags = {
    Environment = "test"
  }
}
