---
service: ocifilestorage
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every control-plane operation in the service tests
last_audit_commit: 500a2a6aa
last_audit_date: 2026-11-01
overall: B
ops:
  CreateFileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "availabilityDomain and compartmentId required (compartment checked against OCI Identity), displayName default FileSystem-<timestamp>, opc-retry-token replay, CREATING then ACTIVE on the injectable clock, etag. sourceSnapshotId clones: the snapshot must be ACTIVE and in the clone's availability domain (assumed, the docs page does not say), the parent gets isCloneParent and cloneCount, the snapshot isCloneSource, the clone isHydrated=false until hydration ends (4 transition periods, state only) and cloneAttachStatus ATTACHED (DETACH detaches after hydration). A snapshot policy must exist in the same AD. With the nfs engine the clone is a real copy of the snapshot tree."}
  GetFileSystem: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag; meteredBytes is 0 in stub mode and the live tree plus snapshots (apparent file sizes) with the nfs engine; isTargetable is computed (unexported, not a replication target, no user snapshots). Deleted file systems stay readable as DELETED for one hour."}
  ListFileSystems: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Bare array of FileSystemSummary (projection of the full record). compartmentId and availabilityDomain required; displayName, lifecycleState, id, sourceSnapshotId, parentFileSystemId, filesystemSnapshotPolicyId filters; sortBy TIMECREATED (default, newest first) or DISPLAYNAME."}
  UpdateFileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags, kmsKeyId (stored, not checked against OCI Vault), filesystemSnapshotPolicyId (empty string detaches). If-Match 412, FULL lock 409 unless isLockOverride."}
  DeleteFileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "204, DELETING then DELETED. Refused while exported, while in a replication, with attached clones unless canDetachChildFileSystem (then detached), and when a snapshot or the file system is locked. Snapshots and quota rules are deleted with it (the docs only list them as associated resources). The nfs engine removes the data."}
  ChangeFileSystemCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body; the target compartment is checked against OCI Identity."}
  DetachClone: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body; DETACHING then DETACHED; 409 IncorrectState unless an attached clone. The parent's cloneCount drops."}
  EstimateReplication: {wire: ok, errors: ok, state: ok, persist: n/a, note: "changeRateInMBps echoed (default 1), minimumSupportedIntervalInMinutes 15, estimatedBaseCopyTimeInMinutes from meteredBytes at an assumed 100 MB/s, allowedTargetRegions lists only the emulator region (us-ashburn-1). The figures are the emulator's model, not Oracle's."}
  ToggleQuotaRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body (SDK ToggleQuotaRulesResponse); sets areQuotaRulesEnabled and quotaEnforcementState ENABLED or DISABLED at once."}
  AddFileSystemLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "FULL or DELETE; duplicate type and relatedResourceId is 409 Conflict (code unverified); returns the file system and etag."}
  RemoveFileSystemLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes the matching lock; 404 when none matches."}
  CreateQuotaRule: {wire: ok, errors: ok, state: partial, persist: ok, note: "principalType FILE_SYSTEM_LEVEL, DEFAULT_GROUP, DEFAULT_USER, INDIVIDUAL_GROUP or INDIVIDUAL_USER; isHardQuota and quotaLimitInGigabytes required; principalId required for INDIVIDUAL_* and refused otherwise; one rule per principal (409). Stored only, not enforced."}
  GetQuotaRule: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Rule of the addressed file system only."}
  ListQuotaRules: {wire: ok, errors: ok, state: ok, persist: n/a, note: "principalType and principalId filters; usageInBytes is always 0 (no per-user accounting); areViolatorsOnly=true therefore lists nothing."}
  UpdateQuotaRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName and quotaLimitInGigabytes; timeUpdated moves."}
  DeleteQuotaRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "204."}
  CreateMountTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "availabilityDomain, compartmentId, subnetId required. The subnet must be live in OCI Core and match its availability domain; a real VNIC without a public IP is created in OCI Core, so privateIpIds resolve through the Core API, ipAddress and hostnameLabel are validated by Core (the hostname needs a subnet dnsLabel), NSGs are passed to Core. An export set (maxFsStatBytes 23843202333929 and maxFsStatFiles 7156920187 defaults are recalled, not in the SDK) is created with it. idmapType NONE or LDAP (LDAP needs connector 1, userSearchBase and groupSearchBase; connectors must be ACTIVE; schemaType default RFC2307 and 300 second cache defaults are assumed). Kerberos needs an uppercase realm and a keytab secret that parses and matches the realm (checked through OCI Vault). requestedThroughput must be 1 at creation (SDK doc). With the nfs engine the NFS server starts when the mount target is ACTIVE."}
  GetMountTarget: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag; exportSetId, privateIpIds, requestedThroughput, observedThroughput, timeBillingCycleEnd (set from the first upgrade)."}
  ListMountTargets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId and availabilityDomain required; exportSetId, displayName, id, lifecycleState filters."}
  UpdateMountTarget: {wire: ok, errors: ok, state: partial, persist: ok, note: "displayName, idmapType and ldapIdmap, kerberos, nsgIds (at most 5; stored, not re-applied to the VNIC), tags, securityAttributes."}
  DeleteMountTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "204. The export set, its exports and the VNIC go with it; the NFS server stops."}
  ChangeMountTargetCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body; the export set moves too."}
  UpgradeShapeMountTarget: {wire: ok, errors: ok, state: partial, persist: ok, note: "requestedThroughput 2 to 20 Gbps and above the current value (the ceiling of 20 is assumed); UPDATING then ACTIVE; sets timeBillingCycleEnd 30 days out."}
  ScheduleDowngradeShapeMountTarget: {wire: ok, errors: ok, state: partial, persist: ok, note: "Must be below the current throughput; applied at timeBillingCycleEnd on the clock."}
  CancelDowngradeShapeMountTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 IncorrectState when nothing is scheduled."}
  ValidateKeyTabs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Parses the MIT keytab (format version 2) stored as the secret content through OCI Vault and reports principal, encryption type and key version; 400 for invalid keytabs or a realm other than the mount target's. The check does not test the keys against a KDC."}
  AddMountTargetLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As AddFileSystemLock."}
  RemoveMountTargetLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As RemoveFileSystemLock."}
  GetExportSet: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag; created with its mount target."}
  ListExportSets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId and availabilityDomain required."}
  UpdateExportSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, maxFsStatBytes, maxFsStatFiles (non-negative). The nfs engine reports them through FSSTAT."}
  CreateExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "exportSetId, fileSystemId and path required. Path: starts with /, no trailing slash, no . or .. element, 1024 bytes in total and 255 per element (File System Paths docs); an export may not equal, contain or sit inside another export path of the set, / excludes all others, a file system is exported once per set, 100 exports per set. File system and set must share an availability domain. exportOptions: up to 100, source single IPv4/IPv6 address or CIDR, access READ_WRITE (default) or READ_ONLY, identitySquash NONE/ROOT/ALL, anonymousUid and anonymousGid 0 to 4294967295 (default 65534), allowedAuth SYS/KRB5/KRB5I/KRB5P (Kerberos only when the mount target has Kerberos enabled). Omitted options default to 0.0.0.0/0, READ_WRITE, NONE, no privileged port, SYS (SDK docs); an empty array hides the export; unspecified fields of a given option default to requirePrivilegedSourcePort true and identitySquash ROOT (SDK docs)."}
  GetExport: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag."}
  ListExports: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId (through the export set), exportSetId, fileSystemId, lifecycleState, id filters; sortBy PATH."}
  UpdateExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "exportOptions are replaced (path is immutable); isIdmapGroupsForSysAuth. The nfs engine re-evaluates options on every mount; running mounts keep the access they were granted."}
  DeleteExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; the data stays on the file system."}
  AddExportLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As AddFileSystemLock."}
  RemoveExportLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As RemoveFileSystemLock."}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fileSystemId and name required; name unique among non-deleted snapshots of the file system (409 Conflict), at most 255 bytes, no slash, not . or ..; expirationTime must be in the future and deletes the snapshot on the clock; lockDurationDetails GOVERNANCE or COMPLIANCE with lockDuration 0 to 36500 and coolOffDuration 0 to 365 days (default 14), COMPLIANCE only for coolOff. The nfs engine copies the tree at creation (not crash-consistent while writes are in flight)."}
  GetSnapshot: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag; provenanceId is the snapshot's own id (clones inherit the parent's)."}
  ListSnapshots: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fileSystemId, filesystemSnapshotPolicyId or id required; compartmentId, lifecycleState filters."}
  UpdateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "tags, expirationTime, lockDurationDetails; a COMPLIANCE lock past its cool-off can only be extended; replication snapshots are immutable."}
  DeleteSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "204. Refused while a resource lock or a duration lock holds (GOVERNANCE with isLockOverride, COMPLIANCE after cool-off never); replication snapshots are managed by the replication. How lockDuration and cool-off interact with timeLocked follows the SDK field docs; the exact service behaviour is unverified."}
  AddSnapshotLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As AddFileSystemLock."}
  RemoveSnapshotLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As RemoveFileSystemLock."}
  CreateFilesystemSnapshotPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "availabilityDomain and compartmentId required; up to 10 schedules (SDK doc); period HOURLY/DAILY/WEEKLY/MONTHLY/YEARLY, timeZone UTC (default) or REGIONAL_DATA_CENTER_TIME (America/New_York for the emulator region), hourOfDay 0 to 23, dayOfWeek, dayOfMonth 1 to 31, month, retentionDurationInSeconds, lockDurationDetails; fields the service chooses are filled from the creation time (SDK docs)."}
  GetFilesystemSnapshotPolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag."}
  ListFilesystemSnapshotPolicies: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId and availabilityDomain required."}
  UpdateFilesystemSnapshotPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, policyPrefix, schedules (replacing them restarts the schedule clock), tags."}
  DeleteFilesystemSnapshotPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; attached file systems lose the policy (assumed), existing snapshots stay."}
  PauseFilesystemSnapshotPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "ACTIVE to INACTIVE; 409 IncorrectState otherwise. A paused policy takes no snapshots."}
  UnpauseFilesystemSnapshotPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "INACTIVE to ACTIVE; missed occurrences are not replayed."}
  ChangeFilesystemSnapshotPolicyCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body."}
  AddFilesystemSnapshotPolicyLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As AddFileSystemLock."}
  RemoveFilesystemSnapshotPolicyLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As RemoveFileSystemLock."}
  CreateReplication: {wire: ok, errors: ok, state: partial, persist: ok, note: "sourceId and targetId required and different; the target must be targetable (unexported, not a target already, no user snapshots), the source may not be a target nor the target a source. replicationInterval minutes, at least 15 (assumed from EstimateReplication's minimum), default 60 (assumed). Creates the replication target. Cycles on the clock: CAPTURING, TRANSFERRING, APPLYING, IDLE, one transition period each; with the nfs engine each cycle snapshots the source and replaces the target's data (target exports are forced read-only while the replication lives)."}
  GetReplication: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag; lastSnapshotId and recoveryPointTime after the first completed cycle."}
  ListReplications: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId and availabilityDomain required; fileSystemId matches source or target."}
  UpdateReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName (mirrored to the target), replicationInterval, tags."}
  DeleteReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; deleteMode FINISH_CYCLE_IF_CAPTURING_OR_APPLYING (default), ONE_MORE_CYCLE, FINISH_CYCLE_IF_APPLYING decide whether a running cycle (or one extra cycle) completes before DELETING ends; the replication target goes too and the target file system becomes targetable again."}
  ChangeReplicationCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body."}
  AddReplicationLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As AddFileSystemLock."}
  RemoveReplicationLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As RemoveFileSystemLock."}
  GetReplicationTarget: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag; mirrors the replication's delta fields."}
  ListReplicationTargets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId and availabilityDomain required."}
  DeleteReplicationTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 Conflict while its replication exists (SDK-implied); otherwise DELETING then DELETED."}
  CreateOutboundConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "connectorType LDAPBIND only (the SDK's only value). bindDistinguishedName, passwordSecretId and 1 to 5 endpoints (hostname, port 1 to 65535) required; secrets are checked in OCI Vault when wired. Polymorphic wire shape with the connectorType discriminator."}
  GetOutboundConnector: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag."}
  ListOutboundConnectors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId and availabilityDomain required."}
  UpdateOutboundConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName and tags (all the SDK allows)."}
  DeleteOutboundConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 Conflict while a mount target's ldapIdmap uses it."}
  ChangeOutboundConnectorCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body."}
  AddOutboundConnectorLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As AddFileSystemLock."}
  RemoveOutboundConnectorLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "As RemoveFileSystemLock."}
families:
  auth: {status: partial, note: "pkgs/ociauth verifies draft-cavage signatures for registered API keys (POST /_gopherstack/oci/keys or OCI Identity); accept-any otherwise; --oci-filestorage-enforce-auth rejects unsigned requests."}
  error_envelope: {status: partial, note: "{code,message}: InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch, IncorrectState, Conflict, LimitExceeded. Conflict and LimitExceeded statuses and the use of 409 for locked resources are unverified. File Storage has no work request API: every operation returns the resource (or nothing) and progress is the lifecycleState."}
  data_plane: {status: partial, note: "--oci-filestorage-engine=nfs keeps one directory per file system under --oci-filestorage-data-dir (a temporary directory by default) and starts an NFSv3 server (github.com/willscott/go-nfs, files through go-billy bound to the file system directory) on a loopback port per ACTIVE mount target; GET /_gopherstack/oci/nfs lists mount target id to host:port. Mount checks export path, first matching exportOptions source (IPv4/IPv6 or CIDR), requirePrivilegedSourcePort (source port below 1024) and READ_ONLY access. Snapshots are directory copies served read-only under the hidden /.snapshot/<name> at the export root (File Storage docs). Verified with the willscott NFS client (create, write, read, snapshot isolation, clone, replication, export option changes). Not a mounted-kernel NFS test: no root mount is done. Stub mode (default) has no data."}
  lifecycle: {status: ok, note: "CREATING, ACTIVE, UPDATING (mount target shape), DELETING, DELETED, INACTIVE (paused policy) and FAILED (NFS start failure) on the injectable clock; snapshot expiry, policy snapshots, hydration, clone detach, shape downgrades and replication cycles are driven by the same clock; deleted records are purged after an hour."}
  persistence: {status: partial, note: "Snapshot carries every resource table, retry tokens, API keys and the sequence counter. Not persisted: NFS listeners (restarted for ACTIVE mount targets on restore, possibly on new ports) and, unless --oci-filestorage-data-dir is set, file data."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-filestorage-port / OCI_FILESTORAGE_PORT (default 8171); never on the shared AWS router; cli.go reserves the port. NFS ports come from the shared allocator."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "Not published (checked FSreplication.htm, managingmounttargets.htm, managingsnapshots.htm and the filestorage SDK comments): default replicationInterval of 60, export set maxFsStat defaults (the SDK says only to use the default), LDAP idmap cache defaults and schemaType default, the same-AD rule for clones, deleting a file system together with its snapshots, policy deletion detaching file systems, REGIONAL_DATA_CENTER_TIME mapped to America/New_York, and the EstimateReplication figures (all emulator choices)"
  - "Quota rules are stored but not enforced: no per-user accounting, usageInBytes is 0 and hard quotas never block writes."
  - "identitySquash, anonymousUid/anonymousGid, isAnonymousAccessAllowed, allowedAuth and Kerberos or LDAP id mapping are stored and validated but not applied by the NFS server (it runs as the emulator's user; go-nfs exposes no credentials to the file system); chown/chmod through NFS act as the emulator's user."
  - "NFS data plane is NFSv3 over TCP on a loopback port per mount target without portmapper or NLM; clients mount with explicit port and mountport (mount -o port=N,mountport=N,nolock,tcp). The mount target's private IP lives in OCI Core only. Snapshots are full copies taken while writes may be in flight (not atomic) and meteredBytes counts apparent sizes with no deduplication; exclusiveBytes is always 0."
  - "Replication cycle phases and deltaProgress are timer-driven state (structural): no byte counters. With the nfs engine each cycle copies the whole snapshot tree to the target rather than a delta. Cross-region targets are not modelled (the emulator has one region)."
  - "Hydration of clones is a timer (4 transition periods); the data is copied at creation with the nfs engine."
  - "Not modelled: IAM policy enforcement and quotas, NSG rule enforcement on the mount target, security attributes (ZPR) beyond storage, opc-dry-run, and Kerberos ticket validation against keytabs."
---

# OCI File Storage parity

Control plane for File Storage API 20171215 (74 operations, all of oci-go-sdk filestorage.FileStorageClient) on its own port (default 8171) with an opt-in NFSv3 data plane.

## 2026-11-13

- Mount target shapes are 1, 20, 40 and 80 Gbps (Content/File/Tasks/managingmounttargets.htm, performance table); UpgradeShape and ScheduleDowngradeShape reject any other value (the old ceiling of 20 was wrong).
- Replication: minimum interval 15 minutes, at most 3 replications per source file system, and a file system with a snapshot policy attached cannot be a target (Content/File/Tasks/FSreplication.htm, "Limitations and Considerations"; LimitExceeded is 400 per Content/API/References/apierrors.htm, which also lists Conflict as 409).

Pass 2026-11-13: kmsKeyId on CreateFileSystem and UpdateFileSystem is validated against OCI Vault when wired (404 NotAuthorizedOrNotFound for an unknown key, 409 IncorrectState when it is not ENABLED), as the other OCI services do.

## Primary-source checks (2026-11-13)
- ocifilestorage: FSreplication.htm states the interval is chosen at replication creation; the SDK documents the FSSTAT reporting rule for maxFsStatBytes and maxFsStatFiles.
