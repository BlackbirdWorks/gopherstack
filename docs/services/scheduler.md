# EventBridge Scheduler

In-memory Scheduler implementation for managing scheduled tasks.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateSchedule` | Create a new schedule |
| `GetSchedule` | Get schedule configuration |
| `ListSchedules` | List all schedules |
| `DeleteSchedule` | Delete a schedule |
| `UpdateSchedule` | Update schedule configuration |
| `TagResource` | Add tags to a schedule |
| `ListTagsForResource` | List tags on a schedule |

## AWS CLI Examples

```bash
# Create a rate-based schedule
aws --endpoint-url http://localhost:8000 scheduler create-schedule \
    --name my-schedule \
    --schedule-expression "rate(5 minutes)" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --target '{
        "Arn":"arn:aws:lambda:us-east-1:000000000000:function:my-function",
        "RoleArn":"arn:aws:iam::000000000000:role/scheduler-role"
    }'

# Create a cron-based schedule
aws --endpoint-url http://localhost:8000 scheduler create-schedule \
    --name daily-job \
    --schedule-expression "cron(0 8 * * ? *)" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --target '{
        "Arn":"arn:aws:sqs:us-east-1:000000000000:my-queue",
        "RoleArn":"arn:aws:iam::000000000000:role/scheduler-role",
        "Input":"{\"task\":\"daily-report\"}"
    }'

# List schedules
aws --endpoint-url http://localhost:8000 scheduler list-schedules

# Get a schedule
aws --endpoint-url http://localhost:8000 scheduler get-schedule \
    --name my-schedule

# Delete a schedule
aws --endpoint-url http://localhost:8000 scheduler delete-schedule \
    --name my-schedule
```

## Known Limitations

- Schedules are stored but **not executed**. No targets are actually invoked at schedule time.
- Schedule groups are not implemented.
- Flexible time windows and timezone-aware schedules are accepted but not enforced.
