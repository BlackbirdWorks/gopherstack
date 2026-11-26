# EventBridge Scheduler

In-memory EventBridge Scheduler. Schedules fire: a runner checks for due schedules every second and delivers to the target.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws scheduler create-schedule --name my-schedule \
    --schedule-expression "rate(5 minutes)" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --target '{"Arn":"arn:aws:lambda:us-east-1:000000000000:function:my-function",
               "RoleArn":"arn:aws:iam::000000000000:role/scheduler-role"}'

aws scheduler create-schedule --name daily-job \
    --schedule-expression "cron(0 8 * * ? *)" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --target '{"Arn":"arn:aws:sqs:us-east-1:000000000000:my-queue",
               "RoleArn":"arn:aws:iam::000000000000:role/scheduler-role",
               "Input":"{\"task\":\"daily-report\"}"}'

aws scheduler list-schedules
aws scheduler get-schedule --name my-schedule
aws scheduler delete-schedule --name my-schedule
```

## Supported operations

| Area | Operations |
|------|------------|
| Schedules | `CreateSchedule`, `GetSchedule`, `ListSchedules`, `UpdateSchedule`, `DeleteSchedule` |
| Schedule groups | `CreateScheduleGroup`, `GetScheduleGroup`, `ListScheduleGroups`, `DeleteScheduleGroup` |
| Tags | `TagResource`, `UntagResource`, `ListTagsForResource` |

Supports `rate()`, `cron()` and `at()` expressions and `ScheduleExpressionTimezone`.

Targets: Lambda, SQS (including FIFO), SNS, EventBridge, Kinesis, Firehose, Step Functions, ECS, SageMaker, CodeBuild, CodePipeline and Inspector, plus universal `aws-sdk` targets (`arn:aws:scheduler:::aws-sdk:...`).
