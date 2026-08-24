---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: transfer
sdk_module: aws-sdk-go-v2/service/transfer@v1.75.4   # version audited against (go.mod)
last_audit_commit: b79595a99                         # HEAD when this manifest was written
last_audit_date: 2026-08-11
overall: A                # WebApp create/wire rewrite to real shape, SecurityPolicy catalog rewrite to real names/algos, Start* op wire fixes, epoch-timestamp bug class fixed across Certificate/HostKey/SSHPublicKey
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  RouteMatcher: {status: ok, note: "X-Amz-Target prefix \"TransferService.\" matches every real SDK serializer target (verified against all 66 api_op_*.go files in the vendored module); MatchPriority is header-exact. No unreachable ops."}
  Server: {status: ok, note: "CreateServer/DescribeServer/ListServers/StartServer/StopServer/DeleteServer/UpdateServer audited op-by-op (unchanged since 2026-07-12 audit; re-confirmed no timestamp fields exist on DescribedServer in the pinned SDK, so the epoch-seconds bug class does not apply here)."}
  User: {status: ok, note: "CreateUser/DescribeUser/ListUsers/DeleteUser/UpdateUser audited (unchanged since 2026-07-12). FIXED this pass: DescribeUser's embedded SshPublicKeys[].DateImported was a Format(time.RFC3339) string; real SshPublicKey.DateImported deserializes via smithytime.ParseEpochSeconds (JSON number) -- a real aws-sdk-go-v2 client would fail to parse the string. Now emits awstime.Epoch(...)."}
  Access: {status: ok, note: "CreateAccess/DescribeAccess/ListAccesses/UpdateAccess/DeleteAccess audited (unchanged since 2026-07-12). No Tags/ARN in real AWS for Access -- confirmed still correct."}
  Agreement: {status: ok, note: "unchanged since 2026-07-12 audit."}
  Connector: {status: ok, note: "CreateConnector/DescribeConnector/ListConnectors/UpdateConnector/DeleteConnector unchanged since 2026-07-12. TestConnection/StartFileTransfer/StartDirectoryListing/StartRemoteDelete/StartRemoteMove field-diffed 2026-07-24 -- see the dedicated Start* family entry below (was previously 'deferred'). FIXED this pass: IpAddressType (types.ConnectorsIpAddressType: IPV4/DUALSTACK, added to CreateConnectorInput/UpdateConnectorInput/DescribedConnector since v1.69.4) is now accepted on Create/UpdateConnector and echoed on DescribeConnector; not validated as an enum, matching the sibling Server.IpAddressType field in this same package, which also accepts any string. ListedConnector has no IpAddressType field in real AWS (confirmed via types.go and the deserializer), so ListConnectors correctly omits it."}
  Profile: {status: ok, note: "unchanged since 2026-07-12 audit."}
  Workflow: {status: ok, note: "unchanged since 2026-07-12 audit."}
  Certificate: {status: ok, note: "FIXED this pass (field-diffed against types.DescribedCertificate/ListedCertificate/ImportCertificateInput/UpdateCertificateInput): (1) epoch-seconds bug class -- NotBeforeDate/NotAfterDate were Format(time.RFC3339) strings, now awstime.Epoch(...) JSON numbers, matching the real smithytime.ParseEpochSeconds deserializer; (2) ActiveDate/InactiveDate existed on the backend Certificate struct but were never accepted by Import/UpdateCertificate nor surfaced on the wire -- both are now real ImportCertificateInput/UpdateCertificateInput fields (via new ImportCertificateFull/UpdateCertificateFull) and Status is computed the way AWS docs describe (ActiveDate/InactiveDate override NotBefore/NotAfter when set); (3) CertificateChain and PrivateKey were entirely unaccepted real ImportCertificateInput fields -- now accepted, with PrivateKey presence surfaced as the real 'Type' field (CERTIFICATE vs CERTIFICATE_WITH_PRIVATE_KEY) on Describe/List; (4) Serial is now extracted from parsed PEM certs and surfaced on Describe; (5) ListCertificates was emitting an invented 'Usage' field -- real ListedCertificate has no Usage member at all (only DescribedCertificate does) -- removed, and added the real ActiveDate/InactiveDate/Description/Type fields that were missing from the list response."}
  HostKey: {status: ok, note: "FIXED this pass: DateImported was a Format(time.RFC3339) string in both DescribeHostKey and ListHostKeys; real DescribedHostKey/ListedHostKey.DateImported deserializes via smithytime.ParseEpochSeconds (JSON number) -- same epoch-seconds bug class as sagemaker/glue/ssm/iot/cloudtrail. Now emits awstime.Epoch(hk.CreatedAt)."}
  Tags: {status: ok, note: "unchanged since 2026-07-12 audit."}
  WebApp: {status: ok, note: "FIXED this pass (gaps gopherstack-h2aa, closed): CreateWebApp previously only accepted Tags and silently dropped the *required* CreateWebAppInput.IdentityProviderDetails field; the backend WebApp model had no EndpointDetails/AccessEndpoint/WebAppEndpointPolicy/WebAppUnits fields at all. Rewrote the whole family against the real SDK: (1) DELETED the invented WebAppIdentityProviderDetails shape (IdentityProviderType/InstanceArn/Role/Url/Directory/Function) -- real Transfer web apps support ONLY IdentityCenterConfig{InstanceArn,Role} as an identity provider (a completely different, narrower shape than the multi-IdP-type shape Transfer *servers* use, which this code had copy-pasted); replaced with WebAppIdentityCenterConfig matching real IdentityCenterConfig (create)/DescribedIdentityCenterConfig (describe, adds server-generated ApplicationArn)/UpdateWebAppIdentityCenterConfig (update, Role only -- InstanceArn is immutable post-creation). (2) Added WebAppVpcConfig (SecurityGroupIds/SubnetIds/VpcId on create, server-generates VpcEndpointId; DescribedWebAppVpcConfig on describe deliberately omits SecurityGroupIds -- confirmed via real SDK type, not a bug) plus AccessEndpoint/WebAppEndpoint(synthesized)/WebAppEndpointPolicy(STANDARD default)/WebAppUnits(Provisioned, defaults to 1)/EndpointType(PUBLIC/VPC derived from VpcConfig presence). (3) CreateWebApp now validates IdentityProviderDetails.IdentityCenterConfig{InstanceArn,Role} as required, matching the real 'This member is required' contract. (4) UpdateWebApp now only allows updating the real-AWS-mutable subset: AccessEndpoint, VPC SubnetIds (not VpcId/SecurityGroupIds), IdentityCenterConfig.Role (not InstanceArn), WebAppUnits. (5) DescribeWebApp/ListWebApps now emit DescribedIdentityProviderDetails.IdentityCenterConfig / DescribedEndpointDetails.Vpc under their real nested-union wire keys instead of the old flat invented shape. FIXED this pass: WebAppVpcConfig (create) and UpdateWebAppVpcConfig (update) both gained IpAddressType (types.WebAppVpcEndpointIpAddressType: IPV4/DUALSTACK) since v1.69.4; now accepted on both CreateWebApp's EndpointDetails.Vpc and UpdateWebApp's EndpointDetails.Vpc, stored on the backend's WebAppVpcConfig. DescribedWebAppVpcConfig (the Describe-side shape) genuinely has no IpAddressType member in real AWS -- same asymmetry already documented above for SecurityGroupIds -- so it is deliberately never echoed on DescribeWebApp/ListWebApps; pinned by TestHandler_CreateWebAppVpcEndpoint and TestHandler_UpdateWebAppVpcIPAddressType."}
  SSHPublicKey: {status: ok, note: "FIXED this pass (gap gopherstack-ujj5, closed): ImportSshPublicKey now validates UserName is an existing user on ServerId (ResourceNotFoundException / ErrUserNotFound) before importing a key, matching the same not-found-parent validation pattern used by CreateAccess/CreateAgreement elsewhere in this service. 50-key-per-user limit and duplicate-body dedup (audited 2026-07-12) remain correct."}
  SecurityPolicy: {status: ok, note: "FULLY REWRITTEN this pass against the current AWS docs (docs.aws.amazon.com/transfer/latest/userguide/security-policies.html and .../security-policies-connectors.html, fetched live 2026-07). FOUND AND DELETED gopherstack-invented catalog entries that never existed in real AWS: 'TransferSecurityPolicy-Connector-2023-05' and 'TransferSecurityPolicy-FIPS-Connector-2023-05' used the wrong naming pattern entirely -- real SFTP-connector security policies use the 'TransferSFTPConnectorSecurityPolicy-' prefix, not 'TransferSecurityPolicy-*Connector*'; 'TransferSecurityPolicy-PQ-SSH-2023-04'/'-PQ-SSH-FIPS-2023-04' used fabricated KEX algorithm names (e.g. a made-up 'ecdh-sha2-nistp256-kyber-512r3-sha256-d00@openquantumsafe.org' identifier) -- the real (now-deprecated) names were '-PQ-SSH-Experimental-2023-04'/'-PQ-SSH-FIPS-Experimental-2023-04' and are superseded by the real 2025 mlkem-hybrid-KEX policies, which are what the catalog now contains. Catalog now has 12 real SERVER policies (2018-11 through 2025-03, plus AS2Restricted-2025-07 and SshAuditCompliant-2025-02) and 3 real CONNECTOR policies (2023-07/2024-03/FIPS-2024-10), each with SshCiphers/SshKexs/SshMacs/TlsCiphers (or SshHostKeyAlgorithms for connectors) transcribed field-for-field from the real per-policy JSON documented by AWS. Also added ContentEncryptionCiphers/HashAlgorithms (AS2) to SERVER policy responses -- these exist in real AWS's actual wire JSON but are not yet modeled as typed fields on the pinned go SDK's DescribedSecurityPolicy struct (SDK modeling lag), so they're additive/harmless extra JSON, not a wire break."}
  StartOperations: {status: ok, note: "FULLY WIRE-DIFFED this pass (previously deferred, un-diffed) against api_op_Start{FileTransfer,DirectoryListing,RemoteDelete,RemoteMove}.go. FOUND AND FIXED real wire-shape bugs, not just stub-vs-real: StartDirectoryListingInput.RemoteDirectoryPath is singular+required (gopherstack had an invented plural 'RemoteDirectoryPaths' array, unvalidated); output key is 'ListingId' (gopherstack returned 'DirectoryListingId', which does not exist in real AWS) and was missing the required 'OutputFileName' field entirely (now synthesized as '<connectorId>-<listingId>.json' per AWS docs). StartRemoteDeleteInput.DeletePath is singular+required (gopherstack had an invented plural 'DeletePaths' array); output key is 'DeleteId' (gopherstack returned 'TransferId', which does not exist on StartRemoteDeleteOutput). StartRemoteMoveInput.SourcePath/TargetPath are singular+required (gopherstack had an invented plural 'SourcePaths' array); output key is 'MoveId' (gopherstack returned 'TransferId', which does not exist on StartRemoteMoveOutput). All four ops now validate their real required fields and return InvalidRequestException when missing. StartFileTransfer was already correct (TransferId matches real StartFileTransferOutput)."}
  Execution/SendWorkflowStepState: {status: ok, note: "unchanged since 2026-07-12 audit."}
  ListFileTransferResults: {status: ok, note: "gopherstack-tp8x (2026-08-21), fixed: was one row per TRANSFER with a 'FilePaths' array of every file (this backend's r.Files); real types.ConnectorFileTransferResult's member is the singular 'FilePath' -- one row per file, not a list. Also: TransferId is a required ListFileTransferResultsInput member (api_op_ListFileTransferResults.go) and the handler was ignoring it entirely, listing every transfer for the connector instead of the one specified -- added GetFileTransferResult(connectorID, transferID) and required-field validation for both ConnectorId and TransferId. Locked by TestListFileTransferResults_OneRowPerFile_RealClient (3-file transfer, real SDK client), TestListFileTransferResults_SingleFile_RealClient, TestHandler_StartFileTransferPersistsRecord."}
  Persistence: {status: ok, note: "unchanged since 2026-07-12 audit; new WebApp/Certificate fields ride the existing store.Table[T] generic Snapshot/Restore, no manual persistence.go wiring needed (confirmed via TestPersistence_FullStateRoundTrip)."}
gaps: []
deferred: []
leaks: {status: clean, note: "Shutdown(ctx) stops the backend's worker (StartServer/StopServer async-transition timer) via Backend.Close(); no goroutine or timer outlives the service. leak_test.go / leak_main_test.go already cover this. No new goroutines/tickers were introduced this pass."}
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

- **Epoch-seconds timestamp bug class (2026-07-24 pass)**: transfer is awsjson1.1, and every
  `time.Time`-typed field on this service's real SDK response types (`Certificate.ActiveDate` /
  `InactiveDate` / `NotBeforeDate` / `NotAfterDate`, `HostKey.DateImported`, `SshPublicKey.DateImported`)
  deserializes via `smithytime.ParseEpochSeconds` (confirmed by reading `deserializers.go` in the
  vendored SDK module) -- i.e. the wire value must be a JSON *number* of seconds since epoch, not an
  RFC3339 string. gopherstack had `.Format(time.RFC3339)` on all of these; a real `aws-sdk-go-v2`
  client hitting this mock would fail every `DescribeCertificate`/`DescribeHostKey`/`ListHostKeys`/
  `DescribeUser` (embedded SshPublicKeys) call with "expected X to be a JSON Number, got string
  instead". Fixed via `pkgs/awstime.Epoch(...)`, the same helper used to fix this exact bug class in
  sagemaker/glue/ssm/iot/cloudtrail. `Server`/`Connector`/`Agreement`/`Profile`/`Workflow` have no
  timestamp fields on their real Described*/Listed* SDK types (confirmed), so this bug class does
  NOT apply to them despite each having an internal `CreatedAt` -- don't "fix" those by inventing a
  `CreatedDate` wire field that doesn't exist in real AWS.

- **WebApp identity provider is IdentityCenterConfig-only, NOT the server multi-IdP shape.**
  Transfer *servers* support SERVICE_MANAGED/API_GATEWAY/AWS_DIRECTORY_SERVICE/AWS_LAMBDA identity
  providers (see `IdentityProviderDetails` in models.go, still correct for servers). Transfer *web
  apps* are a completely separate, narrower resource that supports only IAM Identity Center as an
  identity provider (`WebAppIdentityProviderDetails` is a smithy union with exactly one member,
  `IdentityCenterConfig`). The pre-2026-07-24 code had copy-pasted the server's multi-field
  IdentityProviderDetails shape onto WebApp, which doesn't correspond to anything in the real SDK.
  If a future audit needs to touch WebApp identity again: Create takes
  `IdentityCenterConfig{InstanceArn,Role}` (both required), Describe returns
  `DescribedIdentityCenterConfig{ApplicationArn,InstanceArn,Role}` (ApplicationArn is
  server-assigned), Update takes `UpdateIdentityCenterConfig{Role}` only -- InstanceArn is immutable
  post-creation.

- **Real AWS security-policy names for SFTP connectors use a different prefix than server
  policies**: `TransferSFTPConnectorSecurityPolicy-*`, not `TransferSecurityPolicy-*Connector*`.
  The pre-2026-07-24 catalog had invented names in the latter (wrong) pattern that never existed in
  real AWS. If extending the catalog again, get the exact current name/algorithm list from
  `docs.aws.amazon.com/transfer/latest/userguide/security-policies.html` (servers) or
  `.../security-policies-connectors.html` (connectors) -- the per-policy JSON blocks on those pages
  are the ground truth; don't guess at names or algorithm lists.

## gopherstack-y1zn (2026-08-21): unknown-key sweep, 1 fixed, 1 deferred, 2 false positives

Part of the gopherstack-us9u/g479 map-literal scanner's 526-key unknown-key
bucket triage.

- `CreateWorkflow`/`DescribeWorkflow`: {wire: fixed} -- workflowStepToMap
  emitted "Timeout" for a CUSTOM step; real member
  (types.CustomStepDetails) is "TimeoutSeconds". Proven via
  `TestDescribeWorkflow_CustomStepTimeoutSecondsKey_RealClient`
  (wire_field_fixes_test.go), hand-reverted/confirmed-failing/restored/
  `md5sum`-verified byte-identical.
- `ListFileTransferResults`: {wire: fixed, note: "confirmed real bug, then
  deferred, now fixed by gopherstack-tp8x (2026-08-21) -- see the
  ListFileTransferResults family entry above for the full fix and its
  multi-file-cardinality proof."}
- `DescribeSecurityPolicy`: rejected, not a bug. `ContentEncryptionCiphers`/
  `HashAlgorithms` (AS2) look unknown to the pinned SDK's
  `types.DescribedSecurityPolicy` struct, but a prior pass (see
  `securityPolicyDef`'s doc comment, handler_security_policies.go) already
  verified against current AWS documentation that real AWS sends these
  fields on the wire -- the pinned SDK's Go struct just hasn't caught up yet.
  Left as-is per that prior, already-documented decision.

## gopherstack-fabricated-enum sweep (2026-08-23): 1 fixed, request-side fabricated enum values

Part of a repo-wide sweep for enum *values* gopherstack invents that don't
exist in the pinned SDK's `types/enums.go` (distinct from the field-name/
map-key class above) -- see `iam`'s `SummaryKeyType: "SAMLProviders"` for
the instance that started the campaign.

- `SendWorkflowStepState`: {wire: fixed, severity: unmatchable state --
  highest}. `SendWorkflowStepStateInput.Status` is `types.CustomStepStatus`
  (transfer@v1.75.4 `api_op_SendWorkflowStepState.go`), whose only real
  values are `SUCCESS`/`FAILURE`. gopherstack required `COMPLETE`/
  `EXCEPTION` instead -- values `CustomStepStatus` does not define -- so
  **no real client could ever successfully call this operation**: the SDK
  can only send `CustomStepStatusSuccess`/`CustomStepStatusFailure`, and
  gopherstack rejected both with a 400 `InvalidRequestException`. A prior
  test (`workflows_test.go`) had this backwards, explicitly commenting
  `SUCCESS`/`FAILURE` as "old non-AWS value[s]". Fixed in `models.go`
  (`workflowStepStatusSuccess`/`workflowStepStatusFailure` constants) and
  `workflows.go`. Proven via
  `TestSendWorkflowStepState_CustomStepStatus_RealClient`
  (wire_field_fixes_test.go), a real `transfersdk.Client` round-trip
  asserting `DescribedExecution.Status` decodes to
  `types.ExecutionStatusCompleted`; hand-reverted, confirmed failing
  against unfixed code (`InvalidRequestException: Status must be COMPLETE
  or EXCEPTION, got "SUCCESS"`), restored, `md5sum`-verified
  byte-identical.
