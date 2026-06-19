resource "aws_networkmonitor_monitor" "this" {
  monitor_name      = "{{.MonitorName}}"
  aggregation_period = 30

  tags = {
    Env = "test"
  }
}
