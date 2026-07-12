---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: transfer
sdk_module: aws-sdk-go-v2/service/transfer@v1.69.4   # version audited against (go.mod)
last_audit_commit: 1c6af314                          # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A                # genuine fixes found this pass (server initial state + tag-visibility class bug)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  RouteMatcher: {status: ok, note: "X-Amz-Target prefix \"TransferService.\" matches every real SDK serializer target (verified against all 66 api_op_*.go files in the vendored module); MatchPriority is header-exact. No unreachable ops."}
  Server: {status: ok, note: "CreateServer/DescribeServer/ListServers/StartServer/StopServer/DeleteServer/UpdateServer audited op-by-op. FIXED: CreateServerFull set initial State=ONLINE; real AWS creates servers OFFLINE and requires StartServer (confirmed via AWS docs). Start/StopServer async STARTING/STOPPING->ONLINE/OFFLINE transitions and idempotency are correct. DeleteServer correctly requires OFFLINE/STOPPING and cascades users/accesses/agreements/sshKeys/hostKeys."}
  User: {status: ok, note: "CreateUser/DescribeUser/ListUsers/DeleteUser/UpdateUser audited. FIXED: CreateUserFull never seeded the generic tagsStore, so ListTagsForResource on a freshly-created user's ARN returned empty despite Tags being set at creation (DescribeUser itself was already correct since it reads User.Tags directly, not the tagsStore)."}
  Access: {status: ok, note: "CreateAccess/DescribeAccess/ListAccesses/UpdateAccess/DeleteAccess audited. Real AWS CreateAccessInput/DescribedAccess/ListedAccess have NO Tags field and Access has no ARN (confirmed: not present in api_op_CreateAccess.go) -- gopherstack's Tags acceptance on CreateAccess is unused-by-real-clients dead surface, not a break (real SDK clients never populate it). FIXED: ListAccesses response was missing HomeDirectoryType, present on real ListedAccess."}
  Agreement: {status: ok, note: "CreateAgreement/DescribeAgreement/ListAgreements/UpdateAgreement/DeleteAgreement audited. FIXED: CreateAgreementFull never seeded tagsStore (same class as User)."}
  Connector: {status: ok, note: "CreateConnector/DescribeConnector/ListConnectors/UpdateConnector/DeleteConnector audited; already correctly calls initTagsStore at creation. TestConnection / StartFileTransfer / StartDirectoryListing / StartRemoteDelete / StartRemoteMove reviewed: real (non-stub) async-operation-record backend calls, not fabricated no-ops."}
  Profile: {status: ok, note: "CreateProfile/DescribeProfile/ListProfiles/UpdateProfile/DeleteProfile audited. FIXED: CreateProfile never seeded tagsStore (same class as User/Agreement)."}
  Workflow: {status: ok, note: "CreateWorkflow/DescribeWorkflow/ListWorkflows/DeleteWorkflow/Execution ops reviewed; already correctly calls initTagsStore at creation. Step-type validation (COPY/CUSTOM/DELETE/TAG/DECRYPT) matches real AWS enum."}
  Certificate: {status: ok, note: "ImportCertificate/DescribeCertificate/ListCertificates/UpdateCertificate/DeleteCertificate audited. FIXED: ImportCertificate never seeded tagsStore (same class)."}
  HostKey: {status: ok, note: "ImportHostKey/DescribeHostKey/ListHostKeys/UpdateHostKey/DeleteHostKey audited. FIXED: ImportHostKey never seeded tagsStore (same class); DescribeHostKey itself already surfaced Tags correctly, only the generic ListTagsForResource path was affected."}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource generic ops are correct given a seeded tagsStore. FIXED the root cause: 6 of 9 taggable-resource Create/Import paths (Agreement, Profile, User, WebApp, Certificate, HostKey) never called initTagsStore, so creation-time Tags were invisible to ListTagsForResource until a separate TagResource call -- a disguised no-op matching the exact bug class the pre-existing TestParity_ListTagsForResource_CreationTagsVisible test (Server-only) was written to catch, just not extended to the other 6 resource types. Server/Connector/Workflow were already correct."}
  WebApp: {status: partial, note: "CreateWebApp/DeleteWebApp/UpdateWebApp/WebAppCustomization ops present and real (backed by store, not fabricated). FIXED: DescribeWebApp/ListWebApps were dropping Arn (a *required* field on DescribedWebApp/ListedWebApp per the real SDK) and, for Describe, Tags and IdentityProviderDetails -- all three already existed in backend state but were never surfaced in the response map. NOT fixed (deferred, bd-tracked): CreateWebApp still doesn't accept IdentityProviderDetails at creation even though it is a required CreateWebAppInput field in real AWS (gopherstack.WebApp has no EndpointDetails/AccessEndpoint/WebAppEndpointPolicy/WebAppUnits fields at all, so those stay unset even in Describe/List) -- see gaps."}
  SSHPublicKey: {status: partial, note: "ImportSshPublicKey/DeleteSshPublicKey audited: 50-key-per-user limit and duplicate-body dedup are correctly enforced. Gap found but not fixed (uncertain real-AWS behavior, bd-tracked): ImportSshPublicKey only validates ServerId exists, not that UserName is a user on that server."}
  SecurityPolicy: {status: deferred, note: "DescribeSecurityPolicy/ListSecurityPolicies have an elaborate static catalog (SSH ciphers/kex/macs, TLS ciphers, FIPS/PQ variants) that was read but not diffed line-by-line against the real AWS security-policy catalog this pass -- next audit should verify the exact policy names and algorithm lists are current."}
  Execution/SendWorkflowStepState: {status: ok, note: "CreateExecution/DescribeExecution/ListExecutions/SendWorkflowStepState reviewed; Status enum correctly restricted to COMPLETE/EXCEPTION (real AWS WorkflowStepStatus), not the old SUCCESS/FAILURE placeholder values."}
  Persistence: {status: ok, note: "Handler.Snapshot/Restore already delegate to InMemoryBackend.Snapshot/Restore (persistence.go) -- the Handler-doesn't-expose-Snapshot bug class fixed elsewhere this cycle does NOT apply to transfer; it was already wired correctly."}
gaps:
  - CreateWebApp does not accept IdentityProviderDetails (required in real AWS) or EndpointDetails/AccessEndpoint/WebAppEndpointPolicy/WebAppUnits at creation; backend WebApp model has no fields for the latter four (bd: gopherstack-h2aa)
  - ImportSshPublicKey does not validate that UserName exists on the server before importing a key; real-AWS behavior unconfirmed (bd: gopherstack-ujj5)
deferred:
  - SecurityPolicy family: catalog contents not diffed against current real AWS policy names/algorithms this pass
  - StartFileTransfer/StartDirectoryListing/StartRemoteDelete/StartRemoteMove: reviewed for stub-vs-real but not wire-shape-diffed field-by-field against the real Output shapes
leaks: {status: clean, note: "Shutdown(ctx) stops the backend's worker (StartServer/StopServer async-transition timer) via Backend.Close(); no goroutine or timer outlives the service. leak_test.go / leak_main_test.go already cover this."}
---

## Notes

- **Server initial state**: AWS creates Transfer servers in the `OFFLINE` state; `StartServer` is
  required to transition to `ONLINE` (confirmed via
  https://docs.aws.amazon.com/transfer/latest/userguide/create-server.html). This is easy to get
  backwards because it "feels" like a newly-created server should be usable immediately -- it
  isn't, in real AWS. `AddServerInternal` (a test-only seed helper, not a routed op) intentionally
  still seeds `ONLINE` for test convenience and was left as-is.

- **Tags-creation-visibility bug class**: `InMemoryBackend` keeps two separate copies of a
  resource's tags: the resource struct's own `.Tags` field (used by that resource's own
  Describe/List handler) and a generic `tagsStore map[arn]map[string]string` (used only by the
  cross-resource `TagResource`/`UntagResource`/`ListTagsForResource` ops). `initTagsStore` seeds
  the latter from the former at creation time. Because these are two independent copies, a
  resource's own Describe output can look completely correct while `ListTagsForResource` on the
  same resource's ARN returns nothing -- exactly the "real-looking op may be a disguised stub"
  trap called out in parity-principles.md #4. Before this pass only Server/Connector/Workflow
  called `initTagsStore`; Agreement/Profile/User/WebApp/Certificate/HostKey did not. Any *new*
  taggable resource type added to this service must call `initTagsStore` at creation or reintroduce
  this bug.

- **Access has no ARN and no Tags in real AWS.** `CreateAccessInput`/`DescribedAccess`/
  `ListedAccess` in the real SDK have no `Tags` member and Access is not independently taggable
  (it's identified by `ServerId`+`ExternalId`, no ARN). gopherstack's `createAccessInput` still
  accepts a `Tags` field and the backend stores it, but since real SDK clients never populate that
  field, this is inert surface rather than a wire break -- left as-is rather than removed, to avoid
  unnecessary churn.

- **`ListedX` vs `DescribedX` shapes are intentionally different in real AWS** for
  Certificate/HostKey/Agreement (list variants omit `Tags`; some omit fields present on Describe).
  Don't "fix" a list handler to add `Tags` just because the matching Describe handler has it --
  check the real `ListedX` struct first. `ListedWebApp` and `ListedAccess` are exceptions worth
  remembering: `ListedWebApp.Arn` is *required* (fixed this pass) but `ListedAccess` has no `Arn`
  at all (correctly absent in gopherstack already).

- **Route matching**: transfer is single-endpoint AWS JSON 1.1 (`X-Amz-Target: TransferService.<Op>`,
  `Content-Type: application/x-amz-json-1.1`). `RouteMatcher` does a header-prefix match, which is
  the correct/only discriminator for this protocol (verified against all 66 real SDK serializers) --
  there is no path/method dimension to get wrong here, unlike REST-XML/REST-JSON services.
