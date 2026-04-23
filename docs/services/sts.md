# STS — Security Token Service

In-memory STS implementation for authentication, role assumption, and credential management.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `GetCallerIdentity` | Return the current account, user ID, and ARN |
| `AssumeRole` | Assume an IAM role and get temporary credentials |
| `GetSessionToken` | Get temporary credentials for the current user |
| `GetAccessKeyInfo` | Get the account ID associated with an access key |
| `DecodeAuthorizationMessage` | Decode an authorization failure message |
| `AssumeRoleWithWebIdentity` | Assume a role using an OIDC web identity token |
| `AssumeRoleWithSAML` | Assume a role using a SAML assertion |
| `GetFederationToken` | Get temporary credentials for a federated user |
| `GetDelegatedAccessToken` | Exchange a trade-in token for delegated credentials |
| `GetWebIdentityToken` | Mint a mock web identity token |
| `AssumeRoot` | Generate short-lived root credentials for a target account |

## Session Janitor Configuration

STS runs a background janitor that evicts expired sessions from in-memory storage.

- `STS_JANITOR_INTERVAL` (default: `30s`): interval between janitor sweep ticks
- `JANITOR_TIMEOUT` (global): optional per-sweep timeout for janitor work across services

Dashboard endpoint:

- `GET /dashboard/api/sts/metrics` returns current STS session sweep metrics:
  - `activeSessions`
  - `expiredSessions`
  - `sweepCount`
  - `expiredEvictions`
  - `totalSessionsCreated` — lifetime count of sessions issued since startup
  - `opsAssumeRole` — number of AssumeRole calls made
  - `opsGetSessionToken` — number of GetSessionToken calls made
  - `opsGetCallerIdentity` — number of GetCallerIdentity calls made
  - `opsGetFederationToken` — number of GetFederationToken calls made
  - `opsAssumeRoleWithSAML` — number of AssumeRoleWithSAML calls made
  - `opsAssumeRoleWithWebIdentity` — number of AssumeRoleWithWebIdentity calls made
  - `opsGetAccessKeyInfo` — number of GetAccessKeyInfo calls made
  - `opsDecodeAuthorizationMessage` — number of DecodeAuthorizationMessage calls made
  - `opsGetDelegatedAccessToken` — number of GetDelegatedAccessToken calls made

## AWS CLI Examples

```bash
# Get caller identity (always returns the mock account)
aws --endpoint-url http://localhost:8000 sts get-caller-identity

# Assume a role
aws --endpoint-url http://localhost:8000 sts assume-role \
    --role-arn arn:aws:iam::000000000000:role/MyRole \
    --role-session-name my-session

# Get a session token (with custom duration)
aws --endpoint-url http://localhost:8000 sts get-session-token \
    --duration-seconds 7200

# Get access key info
aws --endpoint-url http://localhost:8000 sts get-access-key-info \
    --access-key-id AKIAIOSFODNN7EXAMPLE

# Assume a role with web identity (OIDC)
aws --endpoint-url http://localhost:8000 sts assume-role-with-web-identity \
    --role-arn arn:aws:iam::000000000000:role/WebIdentityRole \
    --role-session-name wi-session \
    --web-identity-token eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyIn0.

# Assume a role with SAML
aws --endpoint-url http://localhost:8000 sts assume-role-with-saml \
    --role-arn arn:aws:iam::000000000000:role/SAMLRole \
    --principal-arn arn:aws:iam::000000000000:saml-provider/MyIdP \
    --saml-assertion dGVzdA==

# Get a federation token
aws --endpoint-url http://localhost:8000 sts get-federation-token \
    --name my-federated-user

# Decode an authorization message
aws --endpoint-url http://localhost:8000 sts decode-authorization-message \
    --encoded-message <base64-encoded-message>
```

## Response Values

All operations return the configured mock account ID (`--account-id`, default `000000000000`) and region (`--region`, default `us-east-1`).

Assumed-role credentials return dummy access key IDs and secrets that are accepted by Gopherstack but are not valid for real AWS.

## Parameter Validation

- `RoleArn` must be a valid 6-part ARN beginning with `arn:` and using `iam` as the service component.
- `RoleSessionName` must be 2–64 characters and match the AWS-allowed character set `[\w+=,.@:-]`.
- Session duration must be within AWS-imposed limits for each operation type.

## Known Limitations

- Session policies and inline session policies are accepted but not enforced.
- MFA token validation is not implemented.
