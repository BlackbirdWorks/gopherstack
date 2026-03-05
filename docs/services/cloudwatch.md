# CloudWatch

In-memory CloudWatch implementation supporting metric data, alarms, and metric queries.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `PutMetricData` | Publish custom metric data points |
| `GetMetricStatistics` | Query metric statistics over a time range |
| `GetMetricData` | Query one or more metrics with math expressions |
| `ListMetrics` | List available metrics with optional filters |
| `PutMetricAlarm` | Create or update a metric alarm |
| `DescribeAlarms` | Get alarm details and state |
| `DeleteAlarms` | Delete one or more alarms |
| `ListTagsForResource` | List tags on an alarm |
| `TagResource` | Add tags to an alarm |
| `UntagResource` | Remove tags from an alarm |

## AWS CLI Examples

```bash
# Publish a custom metric
aws --endpoint-url http://localhost:8000 cloudwatch put-metric-data \
    --namespace MyApp \
    --metric-name RequestCount \
    --value 42 \
    --unit Count

# Query metric statistics
aws --endpoint-url http://localhost:8000 cloudwatch get-metric-statistics \
    --namespace MyApp \
    --metric-name RequestCount \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-02T00:00:00Z \
    --period 3600 \
    --statistics Sum

# Create an alarm
aws --endpoint-url http://localhost:8000 cloudwatch put-metric-alarm \
    --alarm-name HighErrorRate \
    --namespace MyApp \
    --metric-name ErrorCount \
    --threshold 100 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 \
    --period 300 \
    --statistic Sum

# Describe alarms
aws --endpoint-url http://localhost:8000 cloudwatch describe-alarms

# List metrics
aws --endpoint-url http://localhost:8000 cloudwatch list-metrics \
    --namespace MyApp
```

## Known Limitations

- Alarm state transitions do not trigger actions (SNS notifications, Auto Scaling, etc.).
- Composite alarms are not supported.
- Metric Insights queries in `GetMetricData` are not fully evaluated; basic `SUM/AVG/MIN/MAX` are supported.
- CloudWatch dashboards, anomaly detection, and contributor insights are not implemented.
- Metrics older than the in-memory retention window are not evicted automatically.
