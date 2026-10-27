---
service: sso
sdk_module: aws-sdk-go-v2/service/sso@v1.38.4   # version audited against
last_audit_commit: 94a8059bf
last_audit_date: 2026-10-12
overall: B            # new service; every op checked field-by-field against the pinned serializers/deserializers/types
ops:
  GetRoleCredentials: {wire: ok, errors: ok, state: ok, persist: ok, note: "credentials minted by STS AssumeRole into the permission set's IAM role; expiration is epoch milliseconds"}
  ListAccounts: {wire: ok, errors: ok, state: ok, persist: ok, note: "accounts of the signed-in user's direct and group account assignments, sorted by account ID"}
  ListAccountRoles: {wire: ok, errors: ok, state: ok, persist: ok, note: "permission set names assigned in the account, sorted"}
  Logout: {wire: ok, errors: ok, state: ok, persist: ok, note: "revokes the access token and its refresh token"}
items_still_open:
  - ListAccountRoles/GetRoleCredentials for an unassigned account or role: answers ResourceNotFoundException "No access"; real AWS reports ForbiddenException, which the pinned SDK does not model (bd: gopherstack-cefu7.3)
  - max_result bounds: default 10, cap 1000, below 1 is InvalidRequestException; AWS bounds are not in the SDK (bd: gopherstack-cefu7.3)
gaps: []
structural_gaps:
  - Role ARNs and assumed-role ARNs use the emulator's single IAM account; an assignment to another account ID still resolves to a role in the emulator account
deferred: []
leaks: {status: clean, note: "stateless handler; no goroutines"}
---

## Notes

Protocol is REST-JSON (`restJson1`), signing name `awsssoportal`. The SDK sends calls unsigned with
`x-amz-sso_bearer_token`; the matcher requires that header (it separates these paths from S3 path-style
requests) and rejects other signing scopes. A missing, unknown or expired token is `UnauthorizedException` (401).

**Wiring.** The handler holds no state. `internal/app/sso.go` wires a token source (the `ssooidc` backend) and a
directory at call time: entitlements come from `ssoadmin` account assignments for the user (`USER`) plus every
group the user belongs to in `identitystore` (`GROUP`); account names and emails come from Organizations when it
has the account, else the account ID is the name.

**Credentials.** GetRoleCredentials lazily creates the IAM role `AWSReservedSSO_<PermissionSetName>_<hash>` under
`/aws-reserved/sso.amazonaws.com/` (trust: `sso.amazonaws.com`), applies the permission set's inline policy and
managed policies the IAM backend knows (unknown ARNs are skipped), then calls STS `AssumeRoleForService`. The session
name is the user name, so `GetCallerIdentity` returns `assumed-role/AWSReservedSSO_<name>_<hash>/<user>`.
Policy removals on the permission set are not propagated to an existing role.
