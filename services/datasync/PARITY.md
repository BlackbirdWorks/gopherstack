---
# PARITY MANIFEST SCHEMA — see services/_PARITY_TEMPLATE.md for the schema doc.
service: datasync
sdk_module: aws-sdk-go-v2/service/datasync@v1.61.4
last_audit_commit: 58b3ad76d
last_audit_date: 2026-08-28
overall: A            # systemic field-diff sweep: 20+ genuine wire-shape bugs found & fixed
ops:
  CreateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgents: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationS3: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added AgentArns (Outposts) input, real member (prior sweep); AgentArns now validated to reference agents that actually exist in this backend instead of accepting any ARN and succeeding -- FIXED this sweep"}
  DescribeLocationS3: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented S3BucketArn/Subdirectory fields (not on real wire), added AgentArns -- FIXED this sweep"}
  UpdateLocationS3: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLocations: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "removed invented 'CreationTime' field from each LocationListEntry -- real types.LocationListEntry (datasync@v1.61.4 api_op_ListLocations.go) has exactly two members, LocationArn and LocationUri; harmless to a typed client (unknown JSON keys ignored) but not on the real wire -- FIXED prior sweep (2026-08-28, gopherstack-wrapper-key-sweep). Filters (LocationFilter: Name/Operator/Values, types.go) was declared on the input but never read at all -- every filter silently ignored, returning all locations regardless of the request. Now applies LocationUri/LocationType by Operator (Equals/NotEquals/In/Contains/NotContains/BeginsWith/Less*/Greater*) before pagination; CreationTime is compared as a UTC RFC3339 string since neither the SDK nor its doc comments settle the filter value's wire format -- FIXED this sweep (2026-08-29, wrapper-key-sweep-rds-cloudwatch-sqs-sns). RE-CONFIRMED 2026-08-30 (redshift/personalize/datasync leg of this sweep): re-diffed against ListLocationsInput and LocationFilterName's enum (LocationUri/LocationType/CreationTime) -- filter names, cardinality, and Operator handling all still correct, no regression."}
  CreateLocationAzureBlob: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added required AuthenticationType field + validation; added CmkSecretConfig/CustomSecretConfig (real, previously silently dropped) + mutual-exclusion validation; AgentArns now validated to reference existing agents -- FIXED this sweep"}
  DescribeLocationAzureBlob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ContainerUrl field (not on real wire; LocationUri IS the container URL), added AuthenticationType; added CmkSecretConfig/CustomSecretConfig echo -- FIXED this sweep"}
  UpdateLocationAzureBlob: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added AuthenticationType; added CmkSecretConfig/CustomSecretConfig; AgentArns existence validation -- FIXED this sweep"}
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
  CreateLocationFsxWindows: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added CmkSecretConfig/CustomSecretConfig (real, previously silently dropped) + mutual-exclusion validation -- FIXED this sweep"}
  DescribeLocationFsxWindows: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented FsxFilesystemArn/Subdirectory fields (not on real wire); added CmkSecretConfig/CustomSecretConfig echo -- FIXED this sweep"}
  UpdateLocationFsxWindows: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added CmkSecretConfig/CustomSecretConfig -- FIXED this sweep"}
  CreateLocationHdfs: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added CmkSecretConfig/CustomSecretConfig (real, previously silently dropped) + mutual-exclusion validation; AgentArns now validated to reference existing agents -- FIXED this sweep"}
  DescribeLocationHdfs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented Subdirectory field (not on real wire); added CmkSecretConfig/CustomSecretConfig echo -- FIXED this sweep"}
  UpdateLocationHdfs: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added CmkSecretConfig/CustomSecretConfig; AgentArns existence validation -- FIXED this sweep"}
  CreateLocationNfs: {wire: ok, errors: fixed, state: ok, persist: ok, note: "OnPremConfig.AgentArns (already correctly nested, not flat -- see corrected gaps note) now validated to reference agents that actually exist in this backend instead of accepting any ARN and succeeding -- FIXED this sweep"}
  DescribeLocationNfs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ServerHostname/Subdirectory fields (not on real wire; real output is CreationTime/LocationArn/LocationUri/MountOptions/OnPremConfig only) -- FIXED this sweep"}
  UpdateLocationNfs: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "OnPremConfig.AgentArns existence validation; added missing ServerHostname member, now rebuilds LocationUri (previously silently dropped) -- FIXED this sweep"}
  CreateLocationObjectStorage: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added CmkSecretConfig/CustomSecretConfig (real, previously silently dropped) + mutual-exclusion validation; AgentArns now validated to reference existing agents -- FIXED this sweep"}
  DescribeLocationObjectStorage: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ServerHostname/BucketName/Subdirectory fields (not on real wire); added CmkSecretConfig/CustomSecretConfig echo -- FIXED this sweep"}
  UpdateLocationObjectStorage: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added CmkSecretConfig/CustomSecretConfig; AgentArns existence validation; added missing ServerHostname member, now rebuilds LocationUri (previously silently dropped) -- FIXED this sweep"}
  CreateLocationSmb: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added AuthenticationType field (defaults NTLM); added DnsIpAddresses/KerberosPrincipal/KerberosKeytab/KerberosKrb5Conf (real, previously silently dropped) and CmkSecretConfig/CustomSecretConfig + mutual-exclusion validation; AuthenticationType now validated against the real NTLM|KERBEROS enum instead of accepting any string; AgentArns now validated to reference existing agents -- FIXED this sweep"}
  DescribeLocationSmb: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed invented ServerHostname/Subdirectory fields (not on real wire), added AuthenticationType (real field, was entirely missing); added DnsIpAddresses/KerberosPrincipal echo (KerberosKeytab/KerberosKrb5Conf correctly stay write-only, matching the real response) and CmkSecretConfig/CustomSecretConfig echo -- FIXED this sweep"}
  UpdateLocationSmb: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added AuthenticationType; added DnsIpAddresses/KerberosPrincipal/KerberosKeytab/KerberosKrb5Conf and CmkSecretConfig/CustomSecretConfig; AuthenticationType enum validation; AgentArns existence validation; added missing ServerHostname member, now rebuilds LocationUri (previously silently dropped) -- FIXED this sweep"}
  CreateTask: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added Options/Schedule/Excludes/Includes/ManifestConfig/TaskReportConfig/TaskMode -- all real CreateTaskInput members that were previously silently dropped on the floor -- FIXED this sweep"}
  DescribeTask: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "added same fields to output, echoed verbatim; Status still tracks RUNNING/AVAILABLE execution lifecycle (prior sweep). Re-verified this sweep: ErrorCode/ErrorDetail/Source+DestinationNetworkInterfaceArns omission is still correct -- the backend holds no execution-failure text anywhere (CancelTaskExecution only sets a coarse ERROR status enum, never a message) and no ENI state at all, so populating them would mean fabricating content, not surfacing state the backend already has"}
  UpdateTask: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added same fields with AWS's \"only supplied fields change\" semantics (nil = untouched, non-nil = replace, matching the documented \"specify empty to remove\" behavior for ManifestConfig/TaskReportConfig) -- FIXED this sweep"}
  DeleteTask: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTasks: {wire: ok, errors: ok, state: fixed, persist: ok, note: "Filters (TaskFilter: Name/Operator/Values, types.go) was declared on the input but never read -- every filter silently ignored. Now applies LocationId (matches either SourceLocationArn or DestinationLocationArn) and CreationTime (UTC RFC3339 string comparison, format not settled by the SDK) by Operator before pagination -- FIXED this sweep (2026-08-29, wrapper-key-sweep-rds-cloudwatch-sqs-sns). RE-CONFIRMED 2026-08-30: re-diffed against ListTasksInput and TaskFilterName's enum (LocationId/CreationTime) -- still correct, no regression."}
  StartTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelTaskExecution: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "now rejects cancelling an execution already in a terminal state (SUCCESS/ERROR) with InvalidRequestException instead of silently overwriting it to ERROR, matching the identical guard UpdateTaskExecution already had -- FIXED this sweep (gopherstack-g8k9)"}
  DescribeTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTaskExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: fixed, persist: ok, note: "storedLocation.Tags/storedTask.Tags now kept in sync (previously only storedAgent.Tags was, leaving a dead-code asymmetry) -- FIXED this sweep"}
  UntagResource: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same Tags-sync fix as TagResource -- FIXED this sweep"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Agent: {status: ok, note: "CRUD + list verified against real SDK; AgentStatus/EndpointType wire-accurate"}
  Location: {status: fixed, note: "systemic field-diff sweep across all 11 location types found the prior audit's \"partial: extra Subdirectory field\" note was only the tip of the iceberg: 7 of 11 DescribeLocation*Output types also had OTHER invented fields not on the real wire (S3BucketArn, EfsFilesystemArn, FsxFilesystemArn x3, BucketName, ServerHostname x3, ContainerUrl), 3 types were missing real fields entirely (AgentArns on S3, AuthenticationType on AzureBlob+Smb, FsxFilesystemArn on Ontap), and 2 LocationUri schemes (Lustre \"lustre://\", ONTAP \"ontap://\") definitively violated AWS's own published LocationUri regex. All fixed this sweep; ObjectStorage/AzureBlob scheme prefixes remain unconfirmed (see gaps). This sweep additionally found CmkSecretConfig/CustomSecretConfig -- real, settable CreateLocation*/UpdateLocation* members for AzureBlob/FsxWindows/Hdfs/ObjectStorage/Smb -- were unmodeled entirely (accepted then silently dropped, no error); now stored and echoed on Describe with mutual-exclusion validation. SMB DnsIpAddresses/KerberosPrincipal/KerberosKeytab/KerberosKrb5Conf were the same class of gap for the KERBEROS auth flow; now modeled (Keytab/Krb5Conf correctly stay write-only, matching the real Describe response)"}
  Task: {status: fixed, note: "CreateTask/UpdateTask/DescribeTask previously modeled only 4 of 11 real CreateTaskInput members (SourceLocationArn/DestinationLocationArn/Name/CloudWatchLogGroupArn) -- Options, Schedule, Excludes, Includes, ManifestConfig, TaskReportConfig, and TaskMode were silently accepted-and-dropped on Create and never appeared on Describe. Now modeled as pass-through fields (opaque map[string]any for Options/ManifestConfig/TaskReportConfig, typed FilterRule/TaskSchedule for Excludes/Includes/Schedule) with AWS's documented Update semantics"}
  TaskExecution: {status: fixed, note: "single-in-flight-execution guard, Task.Status RUNNING/AVAILABLE lifecycle, CancelTaskExecution enum handling, ListTaskExecutions all-tasks listing unchanged (fixed in the prior 2026-07-12 sweep); this sweep (gopherstack-g8k9) closed the terminal-state re-cancel gap -- CancelTaskExecution now rejects an already-SUCCESS/ERROR execution instead of silently overwriting it, matching UpdateTaskExecution's existing identical guard"}
  Tags: {status: fixed, note: "TagResource/UntagResource now sync storedLocation.Tags and storedTask.Tags in addition to storedAgent.Tags and the canonical b.tags map, closing the dead-code asymmetry flagged (but not fixed) in the prior sweep"}
gaps:
  - "LocationUri scheme prefixes for ObjectStorage (\"object-storage://\") and AzureBlob (\"azure-blob://\") technically violate AWS's own published LocationUri pattern (^(efs|nfs|s3|smb|hdfs|fsx[a-z0-9-]+)://...$, identical text on every DescribeLocation*Output doc page including these two), same as the now-fixed Lustre/ONTAP bugs -- but no positive evidence exists for what AWS actually returns for these two location types (both are comparatively recent additions; the shared regex may itself be stale doc-generation cruft that predates them and isn't enforced server-side for newer types, unlike the FSx family where the regex was clearly extended on purpose to add the fsx[a-z0-9-]+ alternative). Re-checked this sweep from two independent sources -- the installed botocore 1.43.56 model (data/datasync/2018-11-09/service-2.json.gz, LocationUri shape, pinned to the one version directory present) and AWS's live API_DescribeLocationObjectStorage.html/API_DescribeLocationAzureBlob.html doc pages -- both give the identical pattern text with no scheme-prefix example anywhere for either location type, so the \"no positive evidence\" verdict stands: there is proof the current prefixes violate the published pattern, but no proof of what a compliant replacement should be (both prefixes follow the same type-name-as-scheme convention as every other location type, including the fsxl:// fix below, which is itself only an analogy-based guess -- see next gap). Left unchanged; do not \"fix\" to a guessed scheme without evidence."
  - "LocationUri scheme \"fsxl://\" for FSx Lustre (fixed this sweep from the confirmed-wrong \"lustre://\") was chosen by analogy with FSx OpenZFS's confirmed \"fsxz://\" (real AWS CLI doc example: fsxz://us-west-2.fs-.../fsx/folderA/folder) but is not independently confirmed against real AWS output. Medium confidence: matches the regex, matches the single-letter-suffix convention, but Lustre could plausibly use a different fsx-prefixed string."
  - "ManagedSecretConfig (distinct from CmkSecretConfig/CustomSecretConfig, which are now modeled -- see Location family note) stays absent from every DescribeLocation*Output this sweep confirmed it on (Smb/Hdfs/ObjectStorage/AzureBlob/FsxWindows). This is correct, not a gap: the botocore model's own CmkSecretConfig-in-FsxProtocolSmb documentation states outright \"Do not provide this for a CreateLocation request. ManagedSecretConfig is a ReadOnly property and is only be populated in the DescribeLocation response\" -- AWS populates it itself when the client supplies a plaintext credential (Password/SecretKey/SasConfiguration) without CmkSecretConfig/CustomSecretConfig, by auto-provisioning a Secrets Manager secret. gopherstack has no Secrets Manager integration to back a real SecretArn, and fabricating one would violate the no-fabricated-IDs rule -- correctly left absent."
  - "DescribeTaskOutput omits ErrorCode, ErrorDetail, DestinationNetworkInterfaceArns, SourceNetworkInterfaceArns, and ScheduleDetails, all present on the real output. Re-checked this sweep, including whether CancelTaskExecution's ERROR-status path (the one place this backend models a task-execution outcome other than SUCCESS) gives ErrorCode/ErrorDetail anything to surface: it doesn't -- CancelTaskExecution only flips storedTaskExecution.Status to the bare \"ERROR\" enum value, it never records failure text anywhere, so there is no error-message state to promote, and DataSync's own ErrorCode strings (e.g. its internal troubleshooting codes) aren't published anywhere this sweep could cite. gopherstack also has no ENI provisioning state at all. Both stay an honest omission rather than a fabricated value; a gap only if a future sweep adds real failure-text tracking or ENI simulation."
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
- **CmkSecretConfig/CustomSecretConfig now modeled** (AzureBlob, FsxWindows, Hdfs,
  ObjectStorage, Smb): confirmed via the botocore datasync 2018-11-09 model that both are
  real, settable members of every one of these five `CreateLocation*Request`/
  `UpdateLocation*Request` shapes (plus `DescribeLocation*Response`) -- previously entirely
  absent from gopherstack's input structs, so a client supplying either got no error and the
  value vanished (classic accepted-then-silently-dropped). Now parsed, stored
  (`storedCmkSecretConfig`/`storedCustomSecretConfig`, `omitempty`, additive -- no snapshot
  version bump needed), and echoed on Describe, plus request-level mutual-exclusion
  validation ("Do not provide both ... parameters for the same request", repeated verbatim
  on every one of these shapes in the model). `ManagedSecretConfig` deliberately still
  absent -- see gaps.
- **SMB KERBEROS fields now modeled**: `DnsIpAddresses`, `KerberosPrincipal`,
  `KerberosKeytab`, `KerberosKrb5Conf` are real members of
  `CreateLocationSmbRequest`/`UpdateLocationSmbRequest` (confirmed in the botocore model)
  that were completely absent from gopherstack's SMB input structs even though
  `AuthenticationType: "KERBEROS"` was already accepted -- the auth mode was selectable but
  none of its supporting fields existed. `DnsIpAddresses`/`KerberosPrincipal` now round-trip
  through Describe; `KerberosKeytab`/`KerberosKrb5Conf` are stored but correctly stay
  write-only (the real `DescribeLocationSmbResponse` has neither).
- **AgentArns existence validation added** (CreateLocationS3/AzureBlob/Hdfs/ObjectStorage/Smb/Nfs
  and their Update counterparts): none of these operations checked that a supplied
  `AgentArns` entry referred to an agent this backend actually knows about -- any string
  succeeded and was stored/echoed verbatim, the same "operation accepting an ARN for a
  resource that doesn't exist and reports success" bug class as `CreateTask`'s
  source/destination location check (already enforced before this sweep, and the model this
  fix follows). Now rejected with `InvalidRequestException` before any state mutation via a
  shared `validateAgentArns` helper, called immediately after acquiring the lock and before
  building/writing the stored location -- keeps state-mutated-before-validation out of these
  paths too. FSx Windows/ONTAP/OpenZFS/Lustre and EFS were not touched: none of their real
  `CreateLocation*Request`/`UpdateLocation*Request` shapes have an `AgentArns` member (they
  connect over VPC security groups instead), so there was nothing to validate. NFS was fixed
  in the same pass, correcting a prior sweep's gaps note that mischaracterized this as a
  wire-shape problem ("NFS carries a flat AgentArns the real request nests under
  OnPremConfig"): `createLocationNfsInput`/`updateLocationNfsInput` in
  `handler_locations_nfs.go` already correctly required `OnPremConfig.AgentArns` (confirmed
  against `aws-sdk-go-v2/service/datasync@v1.61.4` `types.OnPremConfig`, an `AgentArns
  []string` member required on both `CreateLocationNfsInput.OnPremConfig` and, when present,
  `UpdateLocationNfsInput.OnPremConfig`); the only real gap was that `CreateLocationNfs`/
  `UpdateLocationNfs` never called `validateAgentArns` on the (correctly nested) value, the
  same missing call the other five had. Covered by table-driven tests in
  `handler_locations_agentarns_test.go`
  (`TestDataSync_AgentArns_RejectsPhantomAgent`/`_UpdateRejectsPhantomAgent`/
  `_AcceptsRealAgent`, now six cases including `nfs`, whose Describe response extracts
  `AgentArns` from `OnPremConfig` instead of the top level); verified by neutering
  `validateAgentArns` to `return nil` unconditionally and confirming the reject-path tests
  fail for all six location types (the accept-path positive control keeps the suite from
  passing on a validator that rejects everything, or one whose call sites have been silently
  unwired).
- **SmbAuthenticationType enum validation added**: `AuthenticationType` on
  `CreateLocationSmb`/`UpdateLocationSmb` previously accepted and echoed any string.
  `SmbAuthenticationType`'s real enum is exactly `["NTLM", "KERBEROS"]` (botocore model);
  any other value (including empty, which now explicitly defaults to `NTLM`) is now rejected
  with `InvalidRequestException` instead of silently succeeding -- a more-permissive-than-AWS
  bug class.
