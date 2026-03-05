# SES — Simple Email Service

Stub SES implementation for email sending and identity management. Emails are accepted and acknowledged but not actually sent.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `SendEmail` | Send a formatted email (accepted, not delivered) |
| `SendRawEmail` | Send a raw MIME message (accepted, not delivered) |
| `VerifyEmailIdentity` | Register an email address as verified |
| `ListIdentities` | List verified email addresses and domains |
| `GetIdentityVerificationAttributes` | Get verification status |
| `DeleteIdentity` | Remove a verified identity |

## AWS CLI Examples

```bash
# Verify an email identity
aws --endpoint-url http://localhost:8000 ses verify-email-identity \
    --email-address test@example.com

# List verified identities
aws --endpoint-url http://localhost:8000 ses list-identities

# Send an email
aws --endpoint-url http://localhost:8000 ses send-email \
    --from sender@example.com \
    --destination '{"ToAddresses":["recipient@example.com"]}' \
    --message '{"Subject":{"Data":"Test","Charset":"UTF-8"},"Body":{"Text":{"Data":"Hello!","Charset":"UTF-8"}}}'

# Get verification attributes
aws --endpoint-url http://localhost:8000 ses get-identity-verification-attributes \
    --identities test@example.com
```

## Known Limitations

- Emails are accepted and return a `MessageId` but are **never actually sent**.
- Email sending quotas and bounce/complaint handling are not implemented.
- SES v2 API is not supported; only the legacy v1 API is implemented.
- Configuration sets, event destinations, and suppression lists are not available.
- Domain verification and DKIM setup are accepted but not enforced.
