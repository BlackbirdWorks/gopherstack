---
service: iam
sdk_module: aws-sdk-go-v2/service/iam   # version: see go.mod (backfilled)
last_audit_commit: 71cd5441
last_audit_date: 2026-07-05
overall: A   # ~1111 LOC genuine fixes incl. 2 disguised stubs
protocol: aws-query -> XML
families:
  users_groups_roles: {status: ok, note: CRUD + path/ARN verified; tags-at-creation FIXED (were dropped)}
  policies:      {status: ok, note: managed+inline, versions, default version; PolicyXML gained Tags field}
  access_keys:   {status: ok, note: PROVEN — create/rotate/status, secret only on create}
  providers:     {status: ok, note: PROVEN — SAML/OIDC CRUD, server certificates, login profile, password policy}
ops:
  ListInstanceProfilesForRole: {wire: ok, errors: ok, state: ok, persist: ok, note: FIXED disguised stub — ignored RoleName, hardcoded empty; now wired to real backend}
  GetAccountAuthorizationDetails: {wire: partial, errors: ok, state: ok, persist: ok, note: FIXED fabricated fake v1 policy version -> real ListPolicyVersions; STILL no Marker/MaxItems/Filter pagination + missing RoleDetail.InstanceProfileList}
  GetRole/GetRolePolicy/GetUserPolicy/GetGroupPolicy/GetPolicyVersion: {wire: ok, note: FIXED policy documents now percent-encoded (RFC 3986) at wire boundary; stored plain}
gaps:
  - comprehensiveBackend uses own sync.Mutex alongside coarse lockmetrics.RWMutex — violates one-coarse-lock rule (bd: gopherstack-gjp)
  - GetAccountAuthorizationDetails pagination + RoleDetail.InstanceProfileList (bd: gopherstack-gjp)
leaks: {status: clean, note: persistence leaks FIXED — handler tags + comprehensive state (SSH keys, MFA links, access-advisor, last-accessed) now snapshotted; go test -race passes}
---

## Notes
- HTTP status codes FIXED: NoSuchEntity 404, EntityAlreadyExists/DeleteConflict/LimitExceeded 409 (were all 400); default code ServiceFailure (was InternalFailure).
- Policy documents: stored as plain JSON in backend, percent-encoded ONLY at wire boundary via encodePolicyDocument().
- STS/assume-role cross-service linkage is out of services/iam scope (wired in cli.go).
