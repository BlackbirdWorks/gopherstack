# CloudWatch Logs

In-memory CloudWatch Logs with log groups and streams, Logs Insights queries, filters, deliveries, transformers and scheduled queries.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws logs create-log-group --log-group-name /myapp/application
aws logs create-log-stream --log-group-name /myapp/application \
    --log-stream-name 2024/01/01/instance-1
aws logs put-log-events --log-group-name /myapp/application \
    --log-stream-name 2024/01/01/instance-1 \
    --log-events '[{"timestamp":1704067200000,"message":"App started"}]'
aws logs get-log-events --log-group-name /myapp/application \
    --log-stream-name 2024/01/01/instance-1
aws logs filter-log-events --log-group-name /myapp/application --filter-pattern "ERROR"
aws logs put-retention-policy --log-group-name /myapp/application --retention-in-days 30
```

## Supported operations

| Area | Operations |
|------|------------|
| Groups and streams | `CreateLogGroup`, `DeleteLogGroup`, `DescribeLogGroups`, `ListLogGroups`, `CreateLogStream`, `DeleteLogStream`, `DescribeLogStreams`, `PutLogGroupDeletionProtection`, `AssociateKmsKey`, `DisassociateKmsKey` |
| Events | `PutLogEvents`, `GetLogEvents`, `FilterLogEvents`, `GetLogRecord`, `GetLogObject`, `StartLiveTail` |
| Retention and tags | `PutRetentionPolicy`, `DeleteRetentionPolicy`, `TagLogGroup`, `UntagLogGroup`, `ListTagsLogGroup`, `TagResource`, `UntagResource`, `ListTagsForResource` |
| Logs Insights | `StartQuery`, `GetQueryResults`, `StopQuery`, `DescribeQueries`, `GetLogGroupFields`, `GetLogFields`, `ListLogGroupsForQuery`, `Put/Describe/DeleteQueryDefinition` |
| Scheduled queries | `Create/Get/Update/Delete/ListScheduledQuery`, `GetScheduledQueryHistory` |
| Metric filters | `PutMetricFilter`, `DescribeMetricFilters`, `DeleteMetricFilter`, `TestMetricFilter` |
| Subscription filters | `PutSubscriptionFilter`, `DescribeSubscriptionFilters`, `DeleteSubscriptionFilter` |
| Export and import | `CreateExportTask`, `DescribeExportTasks`, `CancelExportTask`, `CreateImportTask`, `DescribeImportTasks`, `DescribeImportTaskBatches`, `CancelImportTask` |
| Deliveries | delivery sources, destinations, destination policies, `CreateDelivery`, `GetDelivery`, `DescribeDeliveries`, `DeleteDelivery`, `UpdateDeliveryConfiguration` |
| Cross-account destinations | `PutDestination`, `PutDestinationPolicy`, `DescribeDestinations`, `DeleteDestination` |
| Account and resource policies | `Put/Describe/DeleteAccountPolicy`, `Put/Describe/DeleteResourcePolicy` |
| Data protection and indexing | `Put/Get/DeleteDataProtectionPolicy`, `Put/Describe/DeleteIndexPolicy`, `DescribeFieldIndexes`, `DescribeConfigurationTemplates` |
| Transformers | `PutTransformer`, `GetTransformer`, `DeleteTransformer`, `TestTransformer` |
| Anomaly detection | `CreateLogAnomalyDetector`, `GetLogAnomalyDetector`, `ListLogAnomalyDetectors`, `UpdateLogAnomalyDetector`, `DeleteLogAnomalyDetector`, `ListAnomalies`, `UpdateAnomaly` |
| Integrations | `PutIntegration`, `GetIntegration`, `ListIntegrations`, `DeleteIntegration`, S3 table integration source association |
| Lookup tables | `Create/Get/Update/Delete/DescribeLookupTable(s)` |
| Other | `PutSyslogConfiguration`, `ListSyslogConfigurations`, `DeleteSyslogConfiguration`, `Get/PutStorageTierPolicy`, `PutBearerTokenAuthentication`, `ListAggregateLogGroupSummaries` |

## Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `CLOUDWATCHLOGS_JANITOR_INTERVAL` | `1m` | Janitor tick |
| `CLOUDWATCHLOGS_MAX_RETENTION_DAYS` | `14` | Retention for groups with no retention policy |

## Differences from AWS

> [!WARNING]
> Retention is enforced. The janitor deletes events past the group's `retentionInDays`. A group with no policy keeps events for `CLOUDWATCHLOGS_MAX_RETENTION_DAYS` (14 by default), where AWS keeps them forever.

- Logs Insights runs the Logs Insights QL engine only. `QueryLanguage` SQL and PPL are not honoured.
- Anomaly `Histogram`, `LogSamples`, `PatternString` and `PatternTokens` are never derived from log content: there is no pattern-detection engine.
- `IncludeLinkedAccounts` has no effect (no cross-account links). `DataSources`, `FieldIndexNames` and `LogGroup.DataProtectionStatus` are not modelled.
- `DescribeConfigurationTemplates` and `DescribeFieldIndexes` always return an empty list.
- Scheduled query runs do not simulate delivery, so `Destinations` is always empty.
- `DeliverySource.Status` and `StatusReason` are not modelled.
- S3 table integration associations are all top-level and `ACTIVE`.
