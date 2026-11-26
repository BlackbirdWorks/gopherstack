# CloudWatch

In-memory CloudWatch metrics and alarms, with dashboards, anomaly detectors, Contributor Insights rules and metric streams.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws cloudwatch put-metric-data --namespace MyApp --metric-name RequestCount \
    --value 42 --unit Count
aws cloudwatch get-metric-statistics --namespace MyApp --metric-name RequestCount \
    --start-time 2024-01-01T00:00:00Z --end-time 2024-01-02T00:00:00Z \
    --period 3600 --statistics Sum
aws cloudwatch put-metric-alarm --alarm-name HighErrorRate --namespace MyApp \
    --metric-name ErrorCount --threshold 100 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 --period 300 --statistic Sum
aws cloudwatch describe-alarms
aws cloudwatch list-metrics --namespace MyApp
```

## Supported operations

| Area | Operations |
|------|------------|
| Metrics | `PutMetricData`, `GetMetricStatistics`, `GetMetricData`, `ListMetrics`, `GetMetricWidgetImage` |
| Alarms | `PutMetricAlarm`, `PutCompositeAlarm`, `PutLogAlarm`, `DescribeAlarms`, `DescribeAlarmsForMetric`, `DescribeAlarmHistory`, `DescribeAlarmContributors`, `DeleteAlarms`, `SetAlarmState`, `EnableAlarmActions`, `DisableAlarmActions` |
| Alarm mute rules | `PutAlarmMuteRule`, `GetAlarmMuteRule`, `ListAlarmMuteRules`, `DeleteAlarmMuteRule` |
| Dashboards | `PutDashboard`, `GetDashboard`, `ListDashboards`, `DeleteDashboards` |
| Anomaly detection | `PutAnomalyDetector`, `DescribeAnomalyDetectors`, `DeleteAnomalyDetector` |
| Contributor Insights | `PutInsightRule`, `DescribeInsightRules`, `EnableInsightRules`, `DisableInsightRules`, `DeleteInsightRules`, `GetInsightRuleReport`, `PutManagedInsightRules`, `ListManagedInsightRules` |
| Metric streams | `PutMetricStream`, `GetMetricStream`, `ListMetricStreams`, `StartMetricStreams`, `StopMetricStreams`, `DeleteMetricStream` |
| Datasets | `GetDataset`, `AssociateDatasetKmsKey`, `DisassociateDatasetKmsKey` |
| OTel enrichment | `GetOTelEnrichment`, `StartOTelEnrichment`, `StopOTelEnrichment`, `UpdateOTelEnrichment` |
| Resource metrics | `Create/Get/Update/DeleteResourceMetricsConfiguration` |
| Tags | `TagResource`, `UntagResource`, `ListTagsForResource` |

## Behaviour

- A janitor evaluates metric alarms every 10 s and sweeps expired metric data every 5 minutes.
- Alarm state changes run the configured actions for SNS, Lambda, EC2 (`arn:aws:automate`) and Auto Scaling targets.
- The service also answers the `rpc-v2-cbor` protocol used by current SDKs.

## Differences from AWS

> [!NOTE]
> `ListMetrics` `IncludeLinkedAccounts` and `OwningAccount` have no effect. There are no cross-account observability links, so no other account's metrics exist.
