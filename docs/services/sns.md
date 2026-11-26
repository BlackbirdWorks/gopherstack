# SNS

In-memory SNS with standard and FIFO topics, subscriptions, filter policies, platform applications and the SMS sandbox.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws sns create-topic --name my-topic
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic \
    --protocol sqs \
    --notification-endpoint arn:aws:sqs:us-east-1:000000000000:my-queue
aws sns publish \
    --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic \
    --subject "Test" --message "Event payload" \
    --message-attributes '{"EventType":{"DataType":"String","StringValue":"OrderPlaced"}}'
aws sns list-subscriptions-by-topic \
    --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic
```

## Supported operations

| Area | Operations |
|------|------------|
| Topics | `CreateTopic`, `DeleteTopic`, `ListTopics`, `GetTopicAttributes`, `SetTopicAttributes`, `GetDataProtectionPolicy`, `PutDataProtectionPolicy` |
| Subscriptions | `Subscribe`, `ConfirmSubscription`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `GetSubscriptionAttributes`, `SetSubscriptionAttributes` |
| Publishing | `Publish`, `PublishBatch` |
| Permissions | `AddPermission`, `RemovePermission` |
| Tags | `TagResource`, `UntagResource`, `ListTagsForResource` |
| Platform applications | `CreatePlatformApplication`, `DeletePlatformApplication`, `ListPlatformApplications`, `GetPlatformApplicationAttributes`, `SetPlatformApplicationAttributes`, `CreatePlatformEndpoint`, `DeleteEndpoint`, `GetEndpointAttributes`, `SetEndpointAttributes`, `ListEndpointsByPlatformApplication` |
| SMS | `GetSMSAttributes`, `SetSMSAttributes`, `CheckIfPhoneNumberIsOptedOut`, `ListPhoneNumbersOptedOut`, `OptInPhoneNumber`, `ListOriginationNumbers` |
| SMS sandbox | `CreateSMSSandboxPhoneNumber`, `VerifySMSSandboxPhoneNumber`, `DeleteSMSSandboxPhoneNumber`, `ListSMSSandboxPhoneNumbers`, `GetSMSSandboxAccountStatus` |

## Behaviour

- `sqs`, `lambda`, `firehose` and mobile-push (`application`) subscriptions are confirmed on creation.
- `http`, `https`, `email` and `email-json` subscriptions stay pending until `ConfirmSubscription` is called with the token. HTTP(S) endpoints receive a `SubscriptionConfirmation` message carrying a `SubscribeURL`.
- Filter policies are evaluated on fan-out, on message attributes or on the body (`FilterPolicyScope=MessageBody`).
- Delivery-status logging uses the `*SuccessFeedbackRoleArn` and `*FailureFeedbackRoleArn` topic attributes.

## Differences from AWS

> [!NOTE]
> `SMSMonthToDateSpentUSD` and `SMSSuccessRate` are not emitted. They need per-country SMS pricing and carrier receipts.
