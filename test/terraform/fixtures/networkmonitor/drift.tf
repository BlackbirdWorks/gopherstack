resource "aws_networkmonitor_monitor" "this" {
  monitor_name      = "{{.MonitorName}}"
  aggregation_period = {{.AggregationPeriod}}
}
