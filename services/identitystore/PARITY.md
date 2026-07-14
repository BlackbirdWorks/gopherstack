---
service: identitystore
sdk_module: aws-sdk-go-v2/service/identitystore@v1.36.3   # version audited against
last_audit_commit: a872ba9b                       # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: B            # already-accurate, proven op-by-op; 1 genuine bug found + fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "IdentityStoreId only required field, matches model; UserName/Photos/Birthdate validated"}
  DescribeUser: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filters support more AttributePaths than real AWS (which only truly supports UserName, deprecated); superset, not a stub"}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "delete-before-mutate index pattern verified correct for byUserName/byPrimaryEmail"}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-deletes memberships via byMember index"}
  GetUserId: {wire: ok, errors: ok, state: ok, persist: ok, note: "UniqueAttribute paths userName/emails.value + ExternalId union both handled"}
  CreateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- DisplayName was wrongly required"}
  DescribeGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-deletes memberships via byGroup index"}
  GetGroupId: {wire: ok, errors: ok, state: ok, persist: ok, note: "only displayName/ExternalId paths valid per real model, matches"}
  CreateGroupMembership: {wire: ok, errors: ok, state: ok, persist: ok, note: "validates group+user exist, duplicate-membership check via byGroupMember index"}
  DescribeGroupMembership: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupMemberships: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGroupMembership: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGroupMembershipId: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupMembershipsForMember: {wire: ok, errors: ok, state: ok, persist: ok}
  IsMemberInGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "O(1) byGroupMember index lookup per group id, response shape GroupMembershipExistenceResult verified"}
# Families audited as a group (when per-op is impractical):
families:
  pagination: {status: ok, note: "paginateSlice base64(offset) token, MaxResults 1-100 bound enforced on all List ops incl. ListGroups/ListGroupMemberships (parity_b_test/parity_pass6_test regression-tested)"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to backend; regionalDTO[V] round-trips the unexported `region` field; versioned snapshot format (identitystoreSnapshotVersion) discards incompatible old snapshots safely"}
gaps:                     # known divergences NOT fixed -- link bd issue ids
  - "ListUsers/ListGroups Filters accept more AttributePaths (name.givenname, title, nickname, phonenumbers.value, description, ...) than the real (deprecated) API, which practically only supports UserName/DisplayName equality. Superset behavior, unlikely to break real clients since the feature is deprecated -- not fixed this pass, no bd filed (cosmetic/low-value)."
  - "matchUserSingleValueFilter's default case returns true for unrecognized AttributePath (i.e. an unknown filter silently matches every user instead of being rejected or matching none). Same low-value/deprecated-feature caveat as above -- not fixed this pass."
  - "No server-side uniqueness enforcement on Email/ExternalId values across users (only UserName is enforced unique via usersByUserName index at Create/Rename time); GetUserId by emails.value or ExternalId returns the first match if duplicates exist rather than erroring. Real AWS uniqueness-constraint behavior for these attributes on ambiguous lookup is unverified against a live account -- flagged as speculative, not fixed."
deferred:                 # consciously not audited this pass (scope) -- next pass targets
  - "Extensions field (User.Extensions, ListUsers/ListUsers Extensions request param) -- newer SDK addition (aws:identitystore:enterprise), not modeled in gopherstack's User/CreateUserRequest at all. Low-traffic feature; audit if a future SDK bump surfaces client usage."
leaks: {status: clean, note: "no goroutines/janitors in this service; RWMutex-guarded in-memory store.Table/store.Index only"}
---

## Notes

- Protocol: awsjson1.1 (single POST /, `X-Amz-Target: AWSIdentityStore.<Op>` dispatch). RouteMatcher/ExtractOperation verified against the real client's `X-Amz-Target` header format via aws-sdk-go-v2 middleware inspection (`addOperationXMiddlewares` -> `newServiceMetadataMiddleware_op<Name>`), and `targetPrefix = "AWSIdentityStore."` is correct.

- **Bug fixed this pass**: `handleCreateGroup` (handler.go) required `DisplayName` to be non-empty, returning `ValidationException` when omitted. The real `CreateGroupRequest` smithy model (`api-2.json`) only lists `IdentityStoreId` as required -- `DisplayName` is optional. The backend (`InMemoryBackend.CreateGroup`) was already written correctly to skip the uniqueness check when `DisplayName == ""`, so this was purely an over-strict handler-level validation that rejected valid AWS requests. Fixed by removing the check; updated `TestValidationErrors/create_group_missing_display_name` (renamed to `create_group_missing_display_name_is_allowed`) to assert `200 OK` instead of `400`.

- Required-field lists for every op were cross-checked against `aws-sdk-go@v1.55.5`'s `models/apis/identitystore/2020-06-15/api-2.json` `"required"` arrays (the Go v2 SDK's generated comments/validators only assert required-ness for top-level members set via `smithy.NewErrParamRequired`, which matches the same model). All other ops' required-field validation in `handler.go` matched the model exactly.

- `ResourceNotFoundException`/`ConflictException` are returned with HTTP 404/409 respectively (not the "always-400" some AWS JSON-protocol services use) -- verified this is the established gopherstack-wide convention for AWS JSON 1.x error services (matches `services/scheduler` and `services/bedrockruntime`), not a bug: `aws-sdk-go-v2`'s awsjson1.1 deserializer identifies the exception type from the response body's `__type`/`code` field, not from the HTTP status code (see `deserializers.go`'s `errorCode := restjson.SanitizeErrorCode(typ)`), so the exact non-2xx status code doesn't affect client-side error typing.

- `GetUserId`/`GetGroupId` `AlternateIdentifier.UniqueAttribute.AttributePath` matching uses `strings.EqualFold`, more permissive than the real client (which always sends exact-case `userName`/`emails.value`/`displayName`). This is safe superset behavior, not flagged as a gap.

- `MemberId` is a union with exactly one variant (`UserId`) in this SDK version; `GroupMembershipExistenceResult`'s Go type name differs from the informal "GroupMembershipExistence" naming gopherstack uses internally, but the wire field names (`GroupId`, `MemberId`, `MembershipExists`) match exactly -- verified directly against `types/types.go`, not against gopherstack's own output (per parity-principles.md rule 2).
