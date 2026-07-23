---
# PARITY MANIFEST SCHEMA — see services/_PARITY_TEMPLATE.md for the schema doc.
service: datasync
sdk_module: aws-sdk-go-v2/service/datasync@v1.59.2
last_audit_commit: 8379d347
last_audit_date: 2026-07-23
overall: A            # systemic field-diff sweep: 20+ genuine wire-shape bugs found & fixed
ops:
  CreateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgents: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationS3: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added AgentArns (Outposts) input, real member -- FIXED this sweep"}
  DescribeLocationS3: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented S3BucketArn/Subdirectory fields (not on real wire), added AgentArns -- FIXED this sweep"}
  UpdateLocationS3: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLocations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationAzureBlob: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added required AuthenticationType field + validation -- FIXED this sweep"}
  DescribeLocationAzureBlob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ContainerUrl field (not on real wire; LocationUri IS the container URL), added AuthenticationType -- FIXED this sweep"}
  UpdateLocationAzureBlob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added AuthenticationType -- FIXED this sweep"}
  CreateLocationEfs: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationEfs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented EfsFilesystemArn/Subdirectory fields (not on real wire) -- FIXED this sweep"}
  UpdateLocationEfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationFsxLustre: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationFsxLustre: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented FsxFilesystemArn/Subdirectory fields (not on real wire); LocationUri scheme fixed \"lustre://\" -> \"fsxl://\" (bare \"lustre://\" definitively violates AWS's published LocationUri pattern ^(efs|nfs|s3|smb|hdfs|fsx[a-z0-9-]+)://...$ -- confirmed via API doc page; \"fsxl://\" chosen by analogy with confirmed \"fsxz://\" for OpenZFS, not independently confirmed, see gaps) -- FIXED this sweep"}
  UpdateLocationFsxLustre: {wire: fixed, errors: ok, state: ok, persist: ok, note: "LocationUri scheme fix propagates to Update path -- FIXED this sweep"}
  CreateLocationFsxOntap: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now derives FsxFilesystemArn from StorageVirtualMachineArn (real API never accepts FsxFilesystemArn on Create, only returns it on Describe); LocationUri scheme fixed \"ontap://\" (also violates the published pattern) -> protocol-driven \"nfs://\"/\"smb://\" (ONTAP has no distinct fsx-prefixed scheme like Lustre/OpenZFS -- it reuses the underlying NFS/SMB protocol's own scheme, matching how FSx Windows reuses \"smb://\") -- FIXED this sweep"}
  DescribeLocationFsxOntap: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented Subdirectory field, added FsxFilesystemArn (real field, was entirely missing) -- FIXED this sweep"}
  UpdateLocationFsxOntap: {wire: fixed, errors: ok, state: ok, persist: ok, note: "LocationUri scheme now recomputed on protocol change (NFS<->SMB flips nfs://<->smb://) -- FIXED this sweep"}
  CreateLocationFsxOpenZfs: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationFsxOpenZfs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented FsxFilesystemArn/Subdirectory fields (not on real wire) -- FIXED this sweep"}
  UpdateLocationFsxOpenZfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationFsxWindows: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationFsxWindows: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented FsxFilesystemArn/Subdirectory fields (not on real wire) -- FIXED this sweep"}
  UpdateLocationFsxWindows: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationHdfs: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationHdfs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented Subdirectory field (not on real wire) -- FIXED this sweep"}
  UpdateLocationHdfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationNfs: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationNfs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ServerHostname/Subdirectory fields (not on real wire; real output is CreationTime/LocationArn/LocationUri/MountOptions/OnPremConfig only) -- FIXED this sweep"}
  UpdateLocationNfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationObjectStorage: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationObjectStorage: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ServerHostname/BucketName/Subdirectory fields (not on real wire) -- FIXED this sweep"}
  UpdateLocationObjectStorage: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationSmb: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added AuthenticationType field (defaults NTLM) -- FIXED this sweep"}
  DescribeLocationSmb: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ServerHostname/Subdirectory fields (not on real wire), added AuthenticationType (real field, was entirely missing) -- FIXED this sweep"}
  UpdateLocationSmb: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added AuthenticationType -- FIXED this sweep"}
  CreateTask: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added Options/Schedule/Excludes/Includes/ManifestConfig/TaskReportConfig/TaskMode -- all real CreateTaskInput members that were previously silently dropped on the floor -- FIXED this sweep"}
  DescribeTask: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "added same fields to output, echoed verbatim; Status still tracks RUNNING/AVAILABLE execution lifecycle (prior sweep) -- FIXED this sweep"}
  UpdateTask: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added same fields with AWS's \"only supplied fields change\" semantics (nil = untouched, non-nil = replace, matching the documented \"specify empty to remove\" behavior for ManifestConfig/TaskReportConfig) -- FIXED this sweep"}
  DeleteTask: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  StartTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "terminal-state re-cancel behavior still unconfirmed against real AWS, see gaps (unchanged from prior sweep)"}
  DescribeTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTaskExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: fixed, persist: ok, note: "storedLocation.Tags/storedTask.Tags now kept in sync (previously only storedAgent.Tags was, leaving a dead-code asymmetry) -- FIXED this sweep"}
  UntagResource: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same Tags-sync fix as TagResource -- FIXED this sweep"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Agent: {status: ok, note: "CRUD + list verified against real SDK; AgentStatus/EndpointType wire-accurate"}
  Location: {status: fixed, note: "systemic field-diff sweep across all 11 location types found the prior audit's \"partial: extra Subdirectory field\" note was only the tip of the iceberg: 7 of 11 DescribeLocation*Output types also had OTHER invented fields not on the real wire (S3BucketArn, EfsFilesystemArn, FsxFilesystemArn x3, BucketName, ServerHostname x3, ContainerUrl), 3 types were missing real fields entirely (AgentArns on S3, AuthenticationType on AzureBlob+Smb, FsxFilesystemArn on Ontap), and 2 LocationUri schemes (Lustre \"lustre://\", ONTAP \"ontap://\") definitively violated AWS's own published LocationUri regex. All fixed this sweep; ObjectStorage/AzureBlob scheme prefixes remain unconfirmed (see gaps)"}
  Task: {status: fixed, note: "CreateTask/UpdateTask/DescribeTask previously modeled only 4 of 11 real CreateTaskInput members (SourceLocationArn/DestinationLocationArn/Name/CloudWatchLogGroupArn) -- Options, Schedule, Excludes, Includes, ManifestConfig, TaskReportConfig, and TaskMode were silently accepted-and-dropped on Create and never appeared on Describe. Now modeled as pass-through fields (opaque map[string]any for Options/ManifestConfig/TaskReportConfig, typed FilterRule/TaskSchedule for Excludes/Includes/Schedule) with AWS's documented Update semantics"}
  TaskExecution: {status: ok, note: "state machine unchanged this sweep (single-in-flight-execution guard, Task.Status RUNNING/AVAILABLE lifecycle, CancelTaskExecution enum handling, ListTaskExecutions all-tasks listing -- all fixed in the prior 2026-07-12 sweep)"}
  Tags: {status: fixed, note: "TagResource/UntagResource now sync storedLocation.Tags and storedTask.Tags in addition to storedAgent.Tags and the canonical b.tags map, closing the dead-code asymmetry flagged (but not fixed) in the prior sweep"}
gaps:
  - "LocationUri scheme prefixes for ObjectStorage (\"object-storage://\") and AzureBlob (\"azure-blob://\") technically violate AWS's own published LocationUri pattern (^(efs|nfs|s3|smb|hdfs|fsx[a-z0-9-]+)://...$, identical text on every DescribeLocation*Output doc page including these two), same as the now-fixed Lustre/ONTAP bugs -- but no positive evidence exists for what AWS actually returns for these two location types (both are comparatively recent additions; the shared regex may itself be stale doc-generation cruft that predates them and isn't enforced server-side for newer types, unlike the FSx family where the regex was clearly extended on purpose to add the fsx[a-z0-9-]+ alternative). Left unchanged pending confirmation via real AWS output or LocalStack; do not \"fix\" to a guessed scheme without evidence."
  - "LocationUri scheme \"fsxl://\" for FSx Lustre (fixed this sweep from the confirmed-wrong \"lustre://\") was chosen by analogy with FSx OpenZFS's confirmed \"fsxz://\" (real AWS CLI doc example: fsxz://us-west-2.fs-.../fsx/folderA/folder) but is not independently confirmed against real AWS output. Medium confidence: matches the regex, matches the single-letter-suffix convention, but Lustre could plausibly use a different fsx-prefixed string."
  - "CancelTaskExecution succeeds unconditionally, including on an already-terminal (SUCCESS/ERROR) execution, overwriting its terminal status with ERROR. Real AWS likely rejects cancelling a finished execution, but the exact error behavior was not confirmed and existing test coverage (TestDataSync_TaskExecution) exercises cancel-after-success expecting a 200. Left unfixed pending confirmation of the real error contract (unchanged from prior sweep)."
  - "Managed-secret config fields (CmkSecretConfig/CustomSecretConfig/ManagedSecretConfig) present on the real DescribeLocation*Output for Smb/Hdfs/ObjectStorage/AzureBlob/FsxWindows are not modeled at all -- gopherstack only supports the plaintext Password/SecretKey/SasToken credential flow, not the KMS/Secrets-Manager-backed secret-config flow. Honestly omitted from output (never fabricated) rather than implemented; a real feature gap for a future sweep."
  - "SMB Kerberos authentication is not modeled: AuthenticationType now accepts/echoes \"KERBEROS\" as a string (added this sweep), but the corresponding real fields (KerberosPrincipal, DnsIpAddresses on DescribeLocationSmbOutput; KerberosKeytab-equivalent secret config) are not collected or returned -- only the NTLM (User/Password) flow is functionally implemented."
  - "DescribeTaskOutput omits ErrorCode, ErrorDetail, DestinationNetworkInterfaceArns, SourceNetworkInterfaceArns, and ScheduleDetails, all present on the real output. gopherstack doesn't simulate task-execution failures or ENI provisioning, so these would always be empty/absent -- an honest omission rather than a fabricated value, but a gap if a future sweep adds failure simulation."
deferred:
  - "StorageSystem / DiscoveryJob / TaskReport-as-standalone-ops: not present in aws-sdk-go-v2/service/datasync v1.59.2 at all (re-verified this sweep: full api_op_*.go listing has no such ops -- grepped for storagesystem/discoveryjob/taskreport case-insensitively across services/datasync/*.go, zero matches). TaskReportConfig itself IS a real member of CreateTaskInput/UpdateTaskInput/DescribeTaskOutput and is now modeled (see Task family fix) -- it's a configuration blob on the task, not a standalone set of ops. The family mentioned in the audit brief (dedicated StorageSystem/DiscoveryJob CRUD + Start/Stop/DescribeDiscoveryJob ops) does not exist in the pinned SDK."
leaks: {status: clean, note: "no goroutines/timers/janitors in this service; all ops synchronous under the single lockmetrics.RWMutex; Snapshot/Restore round-trip covers every store.Table plus the raw tags map (persistence_test.go, extended this sweep to also cover Task Options/Schedule/Excludes)"}
---

## Notes

- **Protocol**: awsjson1.1, single POST endpoint, `X-Amz-Target: FmrsService.<Op>` --
  confirmed byte-for-byte against `aws-sdk-go-v2/service/datasync@v1.59.2/serializers.go`
  (every operation's `SetHeader("X-Amz-Target").String("FmrsService.<Op>")`). gopherstack's
  `datasyncTargetPrefix = "FmrsService."` in handler.go matches exactly.
- **Timestamps**: epoch-seconds JSON numbers (`smithytime.ParseEpochSeconds` in the real
  deserializer), matching gopherstack's `CreationTime.Unix()` / `StartTime.Unix()` on the
  wire. No `awstime.Epoch` usage needed since the handler already emits raw epoch ints.
- **Default internal error type fixed this sweep**: `handler.go`'s fallback 500 response
  emitted `__type: "InternalServiceError"`, a string that does not exist in the real
  DataSync API surface. The real, documented value (present on literally every operation's
  error list) is `InternalException`. Fixed to `internalExceptionType = "InternalException"`.
  Low real-world impact (this path only fires on genuine internal bugs, not normal error
  flows -- ResourceNotFoundException/InvalidRequestException were already correct), but a
  genuine wire-shape bug per the audit brief's error-code-exactness requirement.
- **Systemic "extra field on Describe" bug class found and fixed this sweep**: the prior
  2026-07-12 audit found and flagged (but left unfixed pending a "next audit") that every
  DescribeLocation*Output except AzureBlob had an invented `Subdirectory` field not on the
  real wire. This sweep's full field-diff against the vendored SDK found that bug class was
  far more widespread than just `Subdirectory`: S3 (`S3BucketArn`), EFS
  (`EfsFilesystemArn`), FSx Lustre/OpenZFS/Windows (`FsxFilesystemArn` x3), ObjectStorage
  (`ServerHostname`, `BucketName`), and NFS/SMB (`ServerHostname` x2) all had additional
  invented fields that don't exist on the corresponding real `DescribeLocation*Output`
  struct -- the underlying storage-system identifier is *always* folded into `LocationUri`
  only, never echoed as a separate top-level field, across every location type. All were
  stripped this sweep; see the `ops` block for the per-operation list.
- **LocationUri scheme validation via AWS's own published pattern**: every
  `DescribeLocation*Output.LocationUri` doc page publishes the identical constraint pattern
  `^(efs|nfs|s3|smb|hdfs|fsx[a-z0-9-]+)://[a-zA-Z0-9.:/\-]+$`. This directly disproves two
  scheme prefixes gopherstack was emitting: FSx Lustre's `"lustre://"` and FSx ONTAP's
  `"ontap://"` -- neither matches any alternative in the pattern (not a literal `hdfs`-style
  match, and doesn't start with `fsx`). Fixed to `"fsxl://"` (Lustre, by analogy with the
  already-confirmed `"fsxz://"` for OpenZFS) and a protocol-driven `"nfs://"`/`"smb://"`
  (ONTAP, since it has a real NFS-or-SMB protocol choice to key off of, matching how FSx
  Windows already correctly reuses `"smb://"` rather than a distinct `fsx`-prefixed scheme).
  ObjectStorage's `"object-storage://"` and AzureBlob's `"azure-blob://"` *also* technically
  violate the same pattern, but were left unchanged -- see gaps for why.
- **AzureBlob AuthenticationType**: the prior sweep's gaps note flagged this as "larger than
  a validation-only fix, deferred." Implemented this sweep: `AuthenticationType` (required
  on Create per the real SDK's `"This member is required"` annotation, `SAS`/`NONE` enum) is
  now accepted, validated, persisted (`storedAzureBlobConfig.AuthenticationType`), and
  returned on Describe -- plumbed through `CreateLocationAzureBlob`/`UpdateLocationAzureBlob`
  backend signatures per the prior sweep's own assessment of what the fix required.
- **Smb AuthenticationType**: added as a new field (real SDK: `SmbAuthenticationType`,
  `NTLM`/`KERBEROS`, defaults to `NTLM` when omitted per the real API doc's "DataSync
  supports NTLM (default) and KERBEROS authentication"). Only the NTLM flow is functionally
  implemented end-to-end (User/Password); see gaps for the Kerberos-specific fields that
  remain unmodeled.
- **CreateTask/UpdateTask/DescribeTask real member coverage**: the prior sweep's ops table
  marked all three `ok`/`fixed` on a wire basis, but that was never actually field-diffed
  against the real `CreateTaskInput`/`UpdateTaskInput`/`DescribeTaskOutput` structs -- doing
  so this sweep found 7 of 11 real `CreateTaskInput` members were completely unmodeled
  (`Options`, `Schedule`, `Excludes`, `Includes`, `ManifestConfig`, `TaskReportConfig`,
  `TaskMode`): a client sending any of them got no error, but the value was silently
  discarded and never appeared on `DescribeTask`. This is the largest fix in this sweep.
  Implementation approach: `Options`/`ManifestConfig`/`TaskReportConfig` are stored and
  echoed as opaque `map[string]any` (these AWS shapes are deep --  e.g. `TaskReportConfig`
  nests `Destination.S3`/`Overrides` -- and advisory/config-only rather than FK-validated by
  DataSync itself, so round-tripping the client's exact JSON verbatim is both simpler and
  more wire-accurate than hand-modeling every nested field and risking a name mismatch);
  `Excludes`/`Includes`/`Schedule` are typed (`FilterRule{FilterType,Value}`,
  `TaskSchedule{ScheduleExpression,Status}`) since those shapes are small and stable.
  `UpdateTask` follows the real API's documented "only supplied fields change" semantics,
  which Go's JSON decoding gives for free: a `nil` map/slice/pointer means the JSON key was
  absent (leave unchanged), a non-nil (possibly empty) value means it was explicitly
  supplied (replace) -- this also correctly implements the documented "specify this
  parameter as empty to remove" behavior for `ManifestConfig`/`TaskReportConfig` without
  needing extra pointer-wrapping.
  `TaskMode` defaults to `BASIC` on Create (the real API's documented default) and, per the
  real `UpdateTaskInput` (confirmed: no `TaskMode` member), cannot be changed via Update --
  gopherstack's `UpdateTask` wire input has no `TaskMode` field either.
- **Tags dead-code asymmetry fixed**: the prior sweep flagged (but didn't fix)
  `storedLocation.Tags`/`storedTask.Tags` not being kept in sync by `TagResource`/
  `UntagResource` (only `storedAgent.Tags` was). Fixed for consistency; still harmless in
  practice since `ListTagsForResource` sources from the canonical `b.tags` map for every
  resource type, not from the per-resource `.Tags` field.
- **TaskExecutionStatus enum**: real AWS values are exactly `QUEUED, CANCELLING, LAUNCHING,
  PREPARING, TRANSFERRING, VERIFYING, SUCCESS, ERROR` -- there is **no `CANCELLED` value**
  (fixed in the prior 2026-07-12 sweep, unchanged this sweep).
- **CurrentTaskExecutionArn semantics**: "the ARN of **the most recent** task execution", not
  "the currently running" one (fixed in the prior sweep, unchanged this sweep).
- **Task.Status lifecycle**: `RUNNING` while executing, `AVAILABLE` when idle (fixed in the
  prior sweep, unchanged this sweep).
- **One execution per task**: `StartTaskExecution` rejects starting a second execution while
  one is non-terminal (fixed in the prior sweep, unchanged this sweep).
- **ListTaskExecutions without TaskArn**: lists across every task, since `TaskArn` is an
  optional filter (fixed in the prior sweep, unchanged this sweep).
- **DescribeLocationAzureBlob secret leak**: `SasConfiguration` never echoed back (fixed in
  the prior sweep, unchanged this sweep -- and now additionally has `ContainerUrl` removed
  and `AuthenticationType` added, see above).
- **Required-field validation**: cross-checked in the prior sweep for all 11
  `CreateLocation*Input` types (unchanged this sweep).
