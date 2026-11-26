# SES

In-memory SES (v1 API) with identities, templates, configuration sets, receipt rules and a mailbox simulator. SES v2 is a separate service on the same port.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws ses verify-email-identity --email-address test@example.com
aws ses list-identities
aws ses send-email --from test@example.com \
    --destination '{"ToAddresses":["success@simulator.amazonses.com"]}' \
    --message '{"Subject":{"Data":"Test"},"Body":{"Text":{"Data":"Hello"}}}'
aws ses get-identity-verification-attributes --identities test@example.com
```

Sent messages are kept in memory (`SES_EMAIL_TTL`, 24h by default, at most 10000). `GET /_aws/ses` returns them in LocalStack's per-message shape, and `DELETE /_aws/ses` clears them.

## Supported operations

| Area | Operations |
|------|------------|
| Sending | `SendEmail`, `SendRawEmail`, `SendTemplatedEmail`, `SendBulkTemplatedEmail`, `SendBounce`, `SendCustomVerificationEmail` |
| Identities | `VerifyEmailIdentity`, `VerifyEmailAddress`, `VerifyDomainIdentity`, `VerifyDomainDkim`, `ListIdentities`, `ListVerifiedEmailAddresses`, `DeleteIdentity`, `DeleteVerifiedEmailAddress`, `GetIdentityVerificationAttributes`, `GetIdentityDkimAttributes`, `GetIdentityMailFromDomainAttributes`, `GetIdentityNotificationAttributes` |
| Identity settings | `SetIdentityDkimEnabled`, `SetIdentityFeedbackForwardingEnabled`, `SetIdentityHeadersInNotificationsEnabled`, `SetIdentityMailFromDomain`, `SetIdentityNotificationTopic`, `PutIdentityPolicy`, `GetIdentityPolicies`, `ListIdentityPolicies`, `DeleteIdentityPolicy` |
| Templates | `CreateTemplate`, `GetTemplate`, `ListTemplates`, `UpdateTemplate`, `DeleteTemplate`, `TestRenderTemplate`, custom verification email templates |
| Configuration sets | create, describe, list, delete, event destinations (create, update, delete), tracking options, `PutConfigurationSetDeliveryOptions`, `UpdateConfigurationSetSendingEnabled`, `UpdateConfigurationSetReputationMetricsEnabled` |
| Receipt rules | rule sets (create, clone, delete, list, describe, reorder, set active), rules (create, update, delete, describe, set position), receipt filters |
| Account | `GetSendQuota`, `GetSendStatistics`, `GetAccountSendingEnabled`, `UpdateAccountSendingEnabled` |

## Behaviour

- Mailbox simulator addresses: `success@`, `bounce@`, `ooto@`, `complaint@` and `suppressionlist@` at `simulator.amazonses.com`, with an optional `+label`. Bounces and complaints produce notifications.
- Configuration-set SNS event destinations and identity notification topics publish `Send`, `Delivery`, `Bounce` and `Complaint` events.
- Real relay: when `SMTP_HOST` is set (optional `SMTP_USER`, `SMTP_PASS`; port 25 unless given), accepted messages are also relayed over SMTP. Delivery is asynchronous and failures are only logged. Simulator recipients are never relayed.
- Inbound mail: set `SES_INBOUND_ENABLED=true` to run an SMTP listener that applies the active receipt rule set. It supports STARTTLS with a self-signed certificate and refuses unverified recipients, or ones no enabled rule matches, with `550`. Rule actions: AddHeader, S3, SNS, Lambda, Bounce and Stop.

## Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `SES_EMAIL_TTL` | `24h` | How long sent emails are kept |
| `SES_JANITOR_INTERVAL` | `1m` | Janitor tick |
| `SES_INBOUND_ENABLED` | `false` | Run the inbound SMTP listener |
| `SES_INBOUND_ADDR` | `127.0.0.1:2525` | Inbound listen address |
| `SES_INBOUND_REGIONS` | none | Extra per-region listeners as `region=host:port,...` |
| `SES_INBOUND_MAX_SIZE` | `41943040` | Maximum inbound message size in bytes |
| `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS` | none | Relay target for real delivery |

## Differences from AWS

> [!NOTE]
> Messages are not delivered anywhere unless `SMTP_HOST` is set.

- The sender must be a verified identity, or `MessageRejected` is returned. The sandbox rule that recipients must be verified is not enforced.
- A missing template variable renders the literal `{{var}}` instead of failing with `MissingRenderingAttribute`.
- `GetSendStatistics` `Rejects` is always 0: there is no content scanner.
- `FromArn` and `TemplateArn` are accepted but not acted on.
- `SendBounce` is addressed to the bounced recipients: there is no inbound mailbox to resolve the original sender.
