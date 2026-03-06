# CloudWatch Logs

In-memory CloudWatch Logs implementation supporting log groups, streams, and log events with filtering.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateLogGroup` | Create a log group |
| `DeleteLogGroup` | Delete a log group and all its streams |
| `DescribeLogGroups` | List log groups with optional prefix filter |
| `CreateLogStream` | Create a log stream in a group |
| `DescribeLogStreams` | List log streams in a group |
| `PutLogEvents` | Write log events to a stream |
| `GetLogEvents` | Read log events from a stream |
| `FilterLogEvents` | Search log events across streams in a group |
| `PutRetentionPolicy` | Set log retention in days |
| `DeleteRetentionPolicy` | Remove retention policy |
| `TagLogGroup` | Add tags to a log group (legacy API) |
| `UntagLogGroup` | Remove tags from a log group (legacy API) |
| `ListTagsLogGroup` | List tags on a log group (legacy API) |
| `ListTagsForResource` | List tags on a log group (new API) |

## AWS CLI Examples

```bash
# Create a log group
aws --endpoint-url http://localhost:8000 logs create-log-group \
    --log-group-name /myapp/application

# Create a log stream
aws --endpoint-url http://localhost:8000 logs create-log-stream \
    --log-group-name /myapp/application \
    --log-stream-name 2024/01/01/instance-1

# Put log events
aws --endpoint-url http://localhost:8000 logs put-log-events \
    --log-group-name /myapp/application \
    --log-stream-name 2024/01/01/instance-1 \
    --log-events '[{"timestamp":1704067200000,"message":"App started"}]'

# Get log events
aws --endpoint-url http://localhost:8000 logs get-log-events \
    --log-group-name /myapp/application \
    --log-stream-name 2024/01/01/instance-1

# Filter log events by pattern
aws --endpoint-url http://localhost:8000 logs filter-log-events \
    --log-group-name /myapp/application \
    --filter-pattern "ERROR"

# Set retention policy
aws --endpoint-url http://localhost:8000 logs put-retention-policy \
    --log-group-name /myapp/application \
    --retention-in-days 30
```

## Known Limitations

- Log event retention is stored but not enforced; events are not automatically purged.
- CloudWatch Logs Insights query language (`StartQuery`, `GetQueryResults`) is not implemented.
- Log metric filters are not supported.
- Cross-account log sharing is not available.
