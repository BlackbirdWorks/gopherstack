---
service: iam
sdk_module: aws-sdk-go-v2/service/iam   # version: v1.55.0 (bumped from v1.54.7 in e51c0de9; no API-surface change for iam)
last_audit_commit: 6f19cb90
last_audit_date: 2026-07-11
overall: A   # ~1111 LOC genuine fixes incl. 2 disguised stubs (sweep 3) + RoleDetail.InstanceProfileList (sweep 4)
protocol: aws-query -> XML
families:
  users_groups_roles: {status: ok, note: CRUD + path/ARN verified; tags-at-creation FIXED (were dropped)}
  policies:      {status: ok, note: managed+inline, versions, default version; PolicyXML gained Tags field}
  access_keys:   {status: ok, note: PROVEN — create/rotate/status, secret only on create}
  providers:     {status: ok, note: PROVEN — SAML/OIDC CRUD, server certificates, login profile, password policy}
ops:
  ListInstanceProfilesForRole: {wire: ok, errors: ok, state: ok, persist: ok, note: FIXED disguised stub — ignored RoleName, hardcoded empty; now wired to real backend}
  GetAccountAuthorizationDetails: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED (sweep 4) RoleDetail.InstanceProfileList was entirely absent from RoleDetailXML — every role's instance-profile membership silently dropped; now populated from the same backend data as ListInstanceProfilesForRole, with nested Roles resolved to full RoleXML. Sweep 3 already FIXED fabricated fake v1 policy version -> real ListPolicyVersions. STILL no Marker/MaxItems/Filter request-side pagination (see gaps)."}
  GetRole/GetRolePolicy/GetUserPolicy/GetGroupPolicy/GetPolicyVersion: {wire: ok, note: FIXED policy documents now percent-encoded (RFC 3986) at wire boundary; stored plain}
gaps:
  - comprehensiveBackend uses own sync.Mutex alongside coarse lockmetrics.RWMutex — violates one-coarse-lock rule (bd: gopherstack-gjp). Re-examined sweep 4 — deliberate to avoid a nested b.mu lock-order (comp().snapshot()/restore() are documented to run outside any b.mu critical section); fixing requires folding comprehensiveBackend's maps into the main registry/lock, deferred as an architectural change, not a wire/correctness bug.
  - "GetAccountAuthorizationDetails: Marker/MaxItems/Filter request params are parsed but ignored — server always returns the full unfiltered/unpaginated dump with IsTruncated=false. Not a wire-shape violation (SDK's built-in paginator terminates correctly against this since Marker is always absent) and never silently drops data, but diverges from documented AWS behavior for large accounts or Filter-scoped calls (bd: gopherstack-gjp)."
leaks: {status: clean, note: persistence leaks FIXED — handler tags + comprehensive state (SSH keys, MFA links, access-advisor, last-accessed) now snapshotted; go test -race passes}
---

## Notes
- HTTP status codes FIXED: NoSuchEntity 404, EntityAlreadyExists/DeleteConflict/LimitExceeded 409 (were all 400); default code ServiceFailure (was InternalFailure).
- Policy documents: stored as plain JSON in backend, percent-encoded ONLY at wire boundary via encodePolicyDocument().
- STS/assume-role cross-service linkage is out of services/iam scope (wired in cli.go).
- Sweep 4 (2026-07-11): re-audit found local drift since 71cd5441 was entirely commit ce30166a ("Parity sweep 3"), whose fixes were already reflected in this ledger (last_audit_commit had gone stale — the ledger was written *by* that commit but never bumped its own pointer). The only other change in range was the e51c0de9 dependency bump (aws-sdk-go-v2/service/iam v1.54.7 -> v1.55.0); diffed the vendored module trees and confirmed no API-surface change (CHANGELOG.md/generated.json/go_module_metadata.go only). Real fix made: RoleDetail.InstanceProfileList (see ops above). Investigated the two pre-existing `gaps` rows; both remain genuinely deferred (see updated notes on each) rather than being re-flagged as unaddressed.
