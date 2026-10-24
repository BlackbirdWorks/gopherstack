---
service: ssooidc
sdk_module: aws-sdk-go-v2/service/ssooidc@v1.43.3   # version audited against
last_audit_commit: 94a8059bf
last_audit_date: 2026-10-12
overall: B            # new service; every op checked field-by-field against the pinned serializers/deserializers/types
ops:
  RegisterClient: {wire: ok, errors: ok, state: ok, persist: ok, note: "public clients only; grants device_code/authorization_code/refresh_token; redirect URIs https or loopback http"}
  StartDeviceAuthorization: {wire: ok, errors: ok, state: ok, persist: ok, note: "verificationUri points at the emulator's /_gopherstack/sso/device page"}
  CreateToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "device_code, refresh_token and authorization_code (PKCE S256) grants"}
  CreateTokenWithIAM: {wire: ok, errors: ok, state: ok, persist: ok, note: "authorization_code and refresh_token only; jwt-bearer and token-exchange answer UnsupportedGrantTypeException"}
families:
  device_flow: {status: ok, note: "AuthorizationPendingException until approved; SlowDownException when polled inside the interval (interval grows 5s per slow_down); ExpiredTokenException after expiresIn (600s); AccessDeniedException when denied; the device code is single use"}
  emulator_endpoints: {status: ok, note: "GET/POST /_gopherstack/sso/device?user_code=..[&action=approve|deny][&user=name] and GET /_gopherstack/sso/authorize (PKCE) are emulator-only, not AWS APIs"}
items_still_open:
  - RegisterClient AuthorizationEndpoint/TokenEndpoint: returned only when authorization_code is registered; whether AWS returns them for every client is not determinable from the SDK (bd: gopherstack-cefu7.3)
gaps: []
structural_gaps:
  - CreateTokenWithIAM jwt-bearer and token-exchange grants need an external identity provider and trusted token issuer trust chain; no data source exists in an emulator
deferred: []
leaks: {status: clean, note: "no goroutines; expired devices, codes and tokens are pruned on write"}
---

## Notes

Protocol is REST-JSON (`restJson1`), signing name `sso-oauth`. The SDK sends all four calls
unsigned (CreateTokenWithIAM is SigV4-signed), so the matcher keys on method and exact path
(`POST /client/register`, `/device_authorization`, `/token`, `/token?aws_iam=t`) and refuses
requests signed for another service or carrying other query strings (S3 subresources).

Errors are `400` (`401` for InvalidClientException, `500` for InternalServerException) with the
exception name in `X-Amzn-Errortype` and an RFC 6749 body (`error`, `error_description`) plus `message`.

**Who signs in.** StartDeviceAuthorization has no user. The person approving the device picks one:
`/_gopherstack/sso/device?user_code=ABCD-EFGH&action=approve&user=<UserName>` approves as that Identity
Store user (case-insensitive, any Identity Center instance); omitting `user` approves as the user whose
name sorts first. Without `action` the page shows a form with Approve and Deny buttons. An unknown user is a
404 and the code stays pending. `/_gopherstack/sso/authorize` follows the same `user` rule.

**Tokens.** Access tokens live 8 hours, refresh tokens 90 days, client secrets 90 days. A refresh token is
issued only to clients registered with the `refresh_token` grant; refreshing returns a new access token and the
same refresh token. Clients registered without `grantTypes` may use the device code grant only.
Lifetimes are emulator choices, not read from AWS documentation.

`idToken` is never returned: the SDK documents it as not implemented by the service.

State (clients, device authorizations, authorization codes, tokens) persists and is shared across regions.
