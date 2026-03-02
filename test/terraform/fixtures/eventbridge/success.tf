resource "aws_cloudwatch_event_bus" "this" {
  name = "{{.BusName}}"
}

resource "aws_cloudwatch_event_rule" "this" {
  name                = "{{.RuleName}}"
  event_bus_name      = aws_cloudwatch_event_bus.this.name
  schedule_expression = "rate(5 minutes)"
}
