---
# PARITY MANIFEST SCHEMA — see services/_PARITY_TEMPLATE.md for the schema doc.
service: datasync
sdk_module: aws-sdk-go-v2/service/datasync@v1.59.2
last_audit_commit: 8379d347
last_audit_date: 2026-07-12
overall: A            # genuine fixes found (task-execution state machine, security leak, wire shape)
ops:
  CreateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgents: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationS3: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationS3: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field not on real wire (harmless, see gaps)"}
  UpdateLocationS3: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLocations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationAzureBlob: {wire: partial, errors: partial, state: ok, persist: ok, note: "AuthenticationType required member unsupported (not accepted, not returned); see gaps"}
  DescribeLocationAzureBlob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "removed SasConfiguration (secret leak) and Subdirectory (not on real wire) from output -- FIXED this sweep"}
  UpdateLocationAzureBlob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationEfs: {wire: ok, errors: fixed, state: ok, persist: ok, note: "added missing required-field check for Ec2Config -- FIXED this sweep"}
  DescribeLocationEfs: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field (see gaps)"}
  UpdateLocationEfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationFsxLustre: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "added missing required-field check for SecurityGroupArns -- FIXED this sweep"}
  DescribeLocationFsxLustre: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field; LocationUri scheme (\"lustre://\") unverified against real AWS, see gaps"}
  UpdateLocationFsxLustre: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationFsxOntap: {wire: ok, errors: fixed, state: ok, persist: ok, note: "added missing required-field checks for Protocol, SecurityGroupArns -- FIXED this sweep"}
  DescribeLocationFsxOntap: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field; LocationUri scheme (\"ontap://\") unverified, see gaps"}
  UpdateLocationFsxOntap: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationFsxOpenZfs: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "LocationUri scheme fixed openzfs:// -> fsxz:// (confirmed via SDK doc example); added required-field checks for Protocol, SecurityGroupArns -- FIXED this sweep"}
  DescribeLocationFsxOpenZfs: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field (see gaps)"}
  UpdateLocationFsxOpenZfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationFsxWindows: {wire: ok, errors: fixed, state: ok, persist: ok, note: "added missing required-field checks for User, SecurityGroupArns -- FIXED this sweep"}
  DescribeLocationFsxWindows: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field; LocationUri scheme (\"smb://\") unverified, see gaps"}
  UpdateLocationFsxWindows: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationHdfs: {wire: ok, errors: fixed, state: ok, persist: ok, note: "added missing required-field checks for AuthenticationType, AgentArns -- FIXED this sweep"}
  DescribeLocationHdfs: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field (see gaps)"}
  UpdateLocationHdfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationNfs: {wire: ok, errors: fixed, state: ok, persist: ok, note: "added missing required-field checks for Subdirectory, OnPremConfig.AgentArns -- FIXED this sweep"}
  DescribeLocationNfs: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field (see gaps)"}
  UpdateLocationNfs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationObjectStorage: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLocationObjectStorage: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field; SecretKey correctly withheld (see gaps)"}
  UpdateLocationObjectStorage: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLocationSmb: {wire: ok, errors: fixed, state: ok, persist: ok, note: "added missing required-field checks for Subdirectory, AgentArns -- FIXED this sweep"}
  DescribeLocationSmb: {wire: partial, errors: ok, state: ok, persist: ok, note: "extra Subdirectory field; Password correctly withheld (see gaps)"}
  UpdateLocationSmb: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTask: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTask: {wire: ok, errors: ok, state: fixed, persist: ok, note: "Status now tracks RUNNING/AVAILABLE with execution lifecycle -- FIXED this sweep"}
  UpdateTask: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTask: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  StartTaskExecution: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "rejects starting a second execution while one is in progress (AWS: one execution per task at a time); sets Task.Status=RUNNING -- FIXED this sweep"}
  CancelTaskExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "was emitting non-enum status \"CANCELLED\"; now settles to ERROR (real TaskExecutionStatus has no CANCELLED value) and no longer clears CurrentTaskExecutionArn; reverts Task.Status to AVAILABLE -- FIXED this sweep"}
  DescribeTaskExecution: {wire: ok, errors: ok, state: fixed, persist: ok, note: "lazy LAUNCHING->SUCCESS advance now also reverts Task.Status to AVAILABLE -- FIXED this sweep"}
  ListTaskExecutions: {wire: ok, errors: ok, state: fixed, persist: ok, note: "omitted TaskArn now lists across all tasks instead of silently returning empty (TaskArn is an optional filter, not required) -- FIXED this sweep"}
  UpdateTaskExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Agent: {status: ok, note: "CRUD + list verified against real SDK; AgentStatus/EndpointType wire-accurate"}
  Location: {status: ok, note: "all 11 location types (S3/AzureBlob/Efs/FsxLustre/FsxOntap/FsxOpenZfs/FsxWindows/Hdfs/Nfs/ObjectStorage/Smb) audited op-by-op; secrets (Password/SecretKey) already correctly withheld on Describe except AzureBlob SAS token (fixed)"}
  Task: {status: ok, note: "CreateTask FK validation against real location ARNs verified"}
  TaskExecution: {status: fixed, note: "state machine rewritten: single-in-flight-execution guard, Task.Status RUNNING/AVAILABLE lifecycle, CancelTaskExecution enum fix, ListTaskExecutions all-tasks listing"}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against agent/location/task ARNs (task executions are not taggable in real AWS either)"}
gaps:
  - "DescribeLocation* outputs (all types except AzureBlob, fixed this sweep) include an extra `Subdirectory` field that does not exist on the real AWS wire (confirmed: grepped every DescribeLocation*Output struct in aws-sdk-go-v2/service/datasync v1.59.2 -- none have a Subdirectory member; the path is folded into LocationUri only). Harmless (real JSON-1.1 clients ignore unknown fields) but not wire-exact. Left unfixed this sweep to bound the diff to genuine-impact bugs; next audit should strip the field from the other 10 location output structs the same way AzureBlob's was fixed."
  - "CreateLocationAzureBlob / UpdateLocationAzureBlob / DescribeLocationAzureBlob have no AuthenticationType field at all (real API: required on Create, returned on Describe). gopherstack only supports the SAS-token flow implicitly; adding real support requires plumbing a new field through the Create/Update input structs, storedAzureBlobConfig, the backend method signatures (interfaces.go), and the Describe output -- larger than a validation-only fix, deferred."
  - "LocationUri scheme prefixes for FSx Lustre (\"lustre://\"), FSx ONTAP (\"ontap://\"), FSx Windows (\"smb://\"), and several other location types were NOT independently confirmed against real AWS output (only S3 \"s3://\" and FSx OpenZFS \"fsxz://\" were confirmed via SDK/doc evidence this sweep, and OpenZFS was fixed from \"openzfs://\"). The real scheme names for the others are plausible but unverified guesses inherited from before this audit -- worth confirming against actual `aws datasync describe-location-fsx-*` output or LocalStack's implementation in a future pass."
  - "CancelTaskExecution succeeds unconditionally, including on an already-terminal (SUCCESS/ERROR) execution, overwriting its terminal status with ERROR. Real AWS likely rejects cancelling a finished execution (the API doc frames Cancel as stopping something \"in progress\"), but the exact error behavior was not confirmed and existing test coverage (TestDataSync_TaskExecution) exercises cancel-after-success expecting a 200. Left unfixed pending confirmation of the real error contract."
  - "storedAgent.Tags is kept in sync by TagResource/UntagResource but storedLocation.Tags/storedTask.Tags are not (only the canonical b.tags map is updated for those). Harmless dead-code asymmetry since no Describe output reads *.Tags and ListTagsForResource sources from b.tags for every resource type, but worth cleaning up for consistency in a future pass."
deferred:
  - StorageSystem / DiscoveryJob operations: not present in aws-sdk-go-v2/service/datasync v1.59.2 at all (verified: full api_op_*.go listing has no such ops), so there is nothing to implement against this SDK version -- the family mentioned in the audit brief does not exist in the pinned SDK.
leaks: {status: clean, note: "no goroutines/timers/janitors in this service; StartTaskExecution/DescribeTaskExecution/CancelTaskExecution all synchronous under the single lockmetrics.RWMutex; Snapshot/Restore round-trip covers every store.Table plus the raw tags map (persistence_test.go)"}
---

## Notes

- **Protocol**: awsjson1.1, single POST endpoint, `X-Amz-Target: FmrsService.<Op>` --
  confirmed byte-for-byte against `aws-sdk-go-v2/service/datasync@v1.59.2/serializers.go`
  (every operation's `SetHeader("X-Amz-Target").String("FmrsService.<Op>")`). gopherstack's
  `datasyncTargetPrefix = "FmrsService."` in handler.go matches exactly.
- **Timestamps**: epoch-seconds JSON numbers (`smithytime.ParseEpochSeconds` in the real
  deserializer), matching gopherstack's `CreationTime.Unix()` / `StartTime.Unix()` on the
  wire. No `awstime.Epoch` usage needed since the handler already emits raw epoch ints.
- **TaskExecutionStatus enum**: real AWS values are exactly `QUEUED, CANCELLING, LAUNCHING,
  PREPARING, TRANSFERRING, VERIFYING, SUCCESS, ERROR` -- there is **no `CANCELLED` value**.
  Before this sweep, `CancelTaskExecution` emitted the string `"CANCELLED"` directly onto the
  wire, which is not a value any real DataSync client would ever see and would show as an
  unrecognized enum to strongly-typed SDKs. Fixed to settle into `ERROR`, matching the
  documented behavior ("some transient states... interrupt a task execution" / "DataSync
  successfully completes the transfer when you start the next task execution").
- **CurrentTaskExecutionArn semantics**: the real `DescribeTaskOutput.CurrentTaskExecutionArn`
  doc string says "The ARN of **the most recent** task execution" -- not "the currently
  running" one. Before this sweep, `CancelTaskExecution` cleared this field to `""`, which
  is a "looks-wrong-but-correct-to-flag" trap in the other direction: the docs say "most
  recent", so clearing it on cancel was actually a divergence, not the reverse. Fixed to
  leave it pointing at the (now-ERROR) execution. Note for the next auditor: don't
  "fix" this back to clearing on cancel without re-reading the doc string.
- **Task.Status lifecycle**: confirmed via AWS documentation that `TaskStatus` (`AVAILABLE`,
  `CREATING`, `QUEUED`, `RUNNING`, `UNAVAILABLE`) reflects whether a task currently has an
  execution in progress -- `RUNNING` while executing, `AVAILABLE` when idle -- distinct from
  `TaskExecutionStatus` on the execution itself. Before this sweep, gopherstack's `Task.Status`
  was pinned to `AVAILABLE` forever after `CreateTask`. Fixed: `StartTaskExecution` sets
  `RUNNING`; the lazy LAUNCHING->SUCCESS advance in `DescribeTaskExecution` and the ERROR
  settle in `CancelTaskExecution` both revert to `AVAILABLE` (only when the execution being
  resolved is still the task's current one, to avoid clobbering a newer execution's state).
- **One execution per task**: confirmed via the `StartTaskExecution` SDK doc comment verbatim:
  "For each task, you can only run one task execution at a time." Before this sweep,
  `StartTaskExecution` had no such guard and would silently overwrite
  `CurrentTaskExecutionArn` on a second call. Fixed to reject with `InvalidRequestException`
  while the task's current execution is non-terminal. `TestDataSync_UpdateTaskExecution` in
  handler_audit2_test.go previously relied on double-starting the *same* task to get two
  independent non-terminal executions for its parallel subtests; updated to use two separate
  tasks instead (the underlying intent -- two independently-updatable LAUNCHING executions --
  is unaffected).
- **ListTaskExecutions without TaskArn**: `TaskArn` is an optional filter on the real
  `ListTaskExecutionsInput` (no "This member is required" in the SDK doc), so omitting it
  should list executions across every task -- analogous to how `ListLocations`/`ListTasks`
  list everything when unfiltered. Before this sweep, the backend looked up the
  `executionsByTask` secondary index keyed on `""`, which no task ever has, so the call
  silently returned an empty list instead of erroring *or* listing everything -- a disguised
  no-op for the account-wide-listing case. Fixed to fall back to `b.executions.Snapshot()`.
- **DescribeLocationAzureBlob secret leak**: confirmed via the real
  `DescribeLocationAzureBlobOutput` struct that it has **no `SasConfiguration` field at all**
  -- AWS never echoes access credentials back on a Describe call (consistent with every other
  location type: SMB/FsxWindows Password, ObjectStorage SecretKey, HDFS KerberosKeytab are
  all correctly withheld already). gopherstack's AzureBlob Describe was the one exception,
  returning the raw SAS token. Fixed by removing the field from the output type entirely
  (also dropped the now-unused `azureBlobSasConfigOutput` type). While in that struct, also
  dropped the phantom `Subdirectory` field (see gaps entry for the other 10 location types
  that still have it).
- **Required-field validation**: cross-checked every `CreateLocation*Input`'s "This member is
  required" doc annotations against gopherstack's validation and found 8 of 11 location types
  silently accepted requests missing required members (e.g. `CreateLocationEfs` without
  `Ec2Config`, `CreateLocationFsxOntap` without `Protocol`/`SecurityGroupArns`,
  `CreateLocationSmb` without `Subdirectory`/`AgentArns`). Real AWS rejects these with
  `InvalidRequestException`; gopherstack was creating a location with a nil/empty
  network-access config, silently diverging from AWS's actual contract. Added the missing
  checks for Efs, FsxLustre, FsxOntap, FsxOpenZfs, FsxWindows, Hdfs, Nfs, Smb (S3, Agent,
  Task, ObjectStorage, AzureBlob's checkable fields were already correct). Verified every
  existing happy-path test already supplies these fields, so no test bodies needed changes
  beyond the CANCELLED->ERROR and SasConfiguration/openzfs assertion updates.
