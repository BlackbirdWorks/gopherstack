# STS

In-memory STS: caller identity, role assumption, session tokens, federation.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws sts get-caller-identity
aws sts assume-role --role-arn arn:aws:iam::000000000000:role/MyRole \
    --role-session-name my-session
aws sts get-session-token --duration-seconds 7200
aws sts get-federation-token --name my-federated-user
aws sts assume-role-with-web-identity \
    --role-arn arn:aws:iam::000000000000:role/WebIdentityRole \
    --role-session-name wi-session --web-identity-token "$JWT"
aws sts assume-role-with-saml \
    --role-arn arn:aws:iam::000000000000:role/SAMLRole \
    --principal-arn arn:aws:iam::000000000000:saml-provider/MyIdP \
    --saml-assertion dGVzdA==
```

## Supported operations

`AssumeRole`, `AssumeRoleWithSAML`, `AssumeRoleWithWebIdentity`, `AssumeRoot`, `GetCallerIdentity`, `GetSessionToken`, `GetFederationToken`, `GetAccessKeyInfo`, `DecodeAuthorizationMessage`, `GetDelegatedAccessToken`, `GetWebIdentityToken`.

## Behaviour

- Responses use the configured account (`--account-id` / `ACCOUNT_ID`, default `000000000000`) and region (`--region` / `REGION`, default `us-east-1`).
- Issued credentials are accepted by Gopherstack and are not valid on real AWS.
- `RoleArn` and `RoleSessionName` are validated as on AWS (session name 2-64 characters of `[\w+=,.@:-]`). Duration limits are checked per operation.
- The role's trust policy is evaluated when the caller's ARN is known and the role has a trust policy. Otherwise the call is allowed. `ExternalId` and MFA conditions are checked.
- Web-identity tokens are checked against registered OIDC providers.
- A background janitor evicts expired sessions every `STS_JANITOR_INTERVAL` (default `30s`).
- `GET /dashboard/api/sts/metrics` returns session counts, sweep counters and per-operation call counts.

## Differences from AWS

> [!WARNING]
> MFA codes are not validated. `SerialNumber` and `TokenCode` must both be present and well-formed, but any code satisfies `aws:MultiFactorAuthPresent`.

- Inline and managed session policies are validated and counted towards the packed-policy limit, but are not enforced.
- `JWTPayloadSizeExceededException` is not raised: no documented size limit exists to enforce.
