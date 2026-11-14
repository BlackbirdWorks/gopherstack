---
service: azurefile
sdk_module: azure-sdk-for-go/sdk/storage/azfile@v1.5.4
last_audit_commit: 770f2a6d6
last_audit_date: 2026-10-25
overall: B
ops:
  ListShares: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>?comp=list with prefix, marker, maxresults and include=metadata,snapshots,deleted. Deleted shares carry Deleted/Version/DeletedTime/RemainingRetentionDays. Markers are opaque index tokens (pkgs/page)."}
  GetFileServiceProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "Defaults match a fresh account (hour metrics on, minute off, empty CORS, SMB multichannel off); CORS, metrics and SMB/NFS protocol settings are stored."}
  SetFileServiceProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "202. Only the sections present in the body replace stored ones. Validates CORS (max 5 rules, methods) and metrics version/retention; 400 InvalidXmlDocument otherwise. Unknown elements are ignored; share soft delete is not a data-plane setting."}
  CreateShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "201. Quota (1-102400), access tier, enabled protocols (SMB/NFS), root squash (NFS only), metadata and the provisioning/bursting headers are validated and stored; 409 ShareAlreadyExists, 400 InvalidResourceName."}
  GetShareProperties: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET/HEAD ?restype=share, with sharesnapshot support, x-ms-meta-*, lease headers and an optional x-ms-lease-id check."}
  SetShareProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "Quota, tier (updates AccessTierChangeTime), root squash and provisioning options; absent options are left unchanged."}
  SetShareMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces all metadata; 400 InvalidMetadata for non-identifier names."}
  DeleteShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "202. 409 ShareHasSnapshots unless x-ms-delete-snapshots is include/include-leased; sharesnapshot deletes one snapshot; soft-deleted (restorable) when the account's ARM shareDeleteRetentionPolicy is enabled; lease enforced."}
  RestoreShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=undelete with x-ms-deleted-share-name/-version; 409 ShareAlreadyExists when a live share holds the name; expired retention behaves as not found."}
  SnapshotShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "201 with x-ms-snapshot (100ns timestamp id); deep, read-only copy of the tree; metadata from headers or the share; 200 snapshot cap."}
  GetShareACL: {wire: ok, errors: ok, state: ok, persist: n/a, note: "SignedIdentifiers body with Start/Expiry/Permission."}
  SetShareACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stored access policies, max 5, unique ids; stored; a SAS carrying si takes sp/st/se from the policy (fields present in both are refused), an unknown or deleted id is 403 AuthenticationFailed."}
  GetShareStats: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ShareUsageBytes is the sum of file content lengths, the same figure the quota check uses."}
  LeaseShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "acquire/renew/release/change/break with infinite or 15-60s leases, break periods, expiry via the handler clock. Write operations on a leased share require the lease id."}
  CreatePermission: {wire: ok, errors: ok, state: ok, persist: ok, note: "JSON {permission, format}; the key is derived from the SDDL so equal SDDL gives equal keys; 8 KiB cap."}
  GetPermission: {wire: ok, errors: ok, state: ok, persist: n/a, note: "JSON {permission, format}; an unknown key answers 400 FileInvalidPermissionKey (Azure Files error codes table)."}
  CreateDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "201 with SMB headers; attributes, creation/last-write/change times (ISO or now/preserve), permission or permission key (inherit/preserve), metadata; 404 ParentNotFound, 409 ResourceAlreadyExists, names are case-insensitive but case-preserving."}
  DeleteDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; 409 DirectoryNotEmpty; the share root cannot be deleted."}
  GetDirectoryProperties: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET/HEAD; also serves the share root (?restype=directory on the share)."}
  SetDirectoryProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "SMB attributes, times and permission key; omitted values are preserved."}
  SetDirectoryMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces metadata."}
  ListFilesAndDirectories: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Directories first then files, each sorted; prefix, marker, maxresults, include=Timestamps,ETag,Attributes,PermissionKey and x-ms-file-extended-info (FileId); share snapshots."}
  RenameDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "Source is a URL or share-relative path in the same account/share; replace-if-exists (files only), ignore-readonly, destination/source lease ids, metadata and SMB overrides; moving a directory into itself is 400 InvalidInput."}
  ListHandles: {wire: ok, errors: ok, state: partial, persist: n/a, note: "The emulator holds no SMB sessions, so the list is always empty (404 for a missing path)."}
  ForceCloseHandles: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Always reports 0 closed / 0 failed."}
  CreateFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "201; content length up to 4 TiB (sparse extents), content headers, metadata, SMB properties; re-creating an existing file resets it (lease and read-only checked); default attributes Archive."}
  DeleteFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; 409 ReadOnlyAttribute, lease enforced."}
  GetFile: {wire: ok, errors: ok, state: ok, persist: n/a, note: "200/206 with x-ms-range or Range, Content-Range, Accept-Ranges, x-ms-range-get-content-md5 (<=4 MiB), SAS rsc* header overrides, copy and lease headers; bodies over 4 MiB stream from the extents; 416 InvalidRange."}
  GetFileProperties: {wire: ok, errors: ok, state: ok, persist: n/a, note: "HEAD with Content-Length, content headers, SMB headers, metadata, lease and copy status."}
  SetFileProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "Content headers replace as a set when any is sent; x-ms-content-length resizes (truncate or extend sparsely); SMB properties."}
  SetFileMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces metadata; lease enforced."}
  PutRange: {wire: ok, errors: ok, state: ok, persist: ok, note: "update and clear writes, 4 MiB cap, Content-MD5 verification (400 Md5Mismatch), returns Content-MD5; last-write-time now/preserve; 416 beyond the file length."}
  PutRangeFromURL: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sources: this emulator's file URLs and its Blob service (path or <account>.blob. host style). Other hosts get 403 CannotVerifyCopySource; an expired SAS (se) is refused."}
  ListRanges: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Range list with optional x-ms-range window and prevsharesnapshot diff (Range plus ClearRange); x-ms-content-length header."}
  CopyFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; small same-account copies from an emulator file or blob finish inside the request (success); cross-account or over-4-MiB sources answer x-ms-copy-status pending and turn success on the handler clock (2 s + 1 s per 32 MiB); metadata/SMB overrides with the source token; copy status appears on Get Properties."}
  AbortCopyFile: {wire: ok, errors: ok, state: ok, persist: n/a, note: "204 on a pending copy: status aborted, destination left zero-length with its metadata; a finished copy is 409 NoPendingCopyOperation, a wrong id 409 CopyIdMismatch."}
  RenameFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "Same rules as RenameDirectory; keeps content, metadata, id and lease."}
  LeaseFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "Infinite leases only (as in Azure); acquire/release/change/break."}
  CreateHardLink: {wire: ok, errors: partial, state: ok, persist: ok, note: "PUT ?restype=hardlink with x-ms-file-target-file on NFS shares: 201 with x-ms-link-count, file type, mode, owner, group; links share size, content and metadata (writes, properties, metadata sync every link by file id) and deleting one lowers the count. Symlink or directory targets answer 400 XNfs4FileTypeUnsupportedException; the non-NFS-share refusal (400 InvalidUri) is unverified."}
  CreateSymbolicLink: {wire: ok, errors: partial, state: ok, persist: ok, note: "PUT ?restype=symboliclink with x-ms-link-text (max 2 KiB), owner, group, times, metadata; type SymLink, mode 0777, size = link text length. 409 ResourceAlreadyExists; non-NFS refusal unverified."}
  GetSymbolicLink: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET ?restype=symboliclink returns x-ms-link-text, ETag and Last-Modified; 400 ResourceTypeMismatch on a non-symlink."}
families:
  routing: {status: ok, note: "Dedicated listener on a fixed port (10009, --azure-file-port/AZURE_FILE_PORT), no fallback pool, reserved in the shared PortAlloc pool. The account comes from the first path segment or from an <account>.file.<host> Host header; both address one backend, and the shared storage vhost listener forwards .file. hosts here. Ports 10000-10011 are now all assigned to Azure services."}
  auth: {status: partial, note: "Anonymous, Shared Key, Shared Key Lite, SAS and Bearer requests are accepted by default (repo-wide permissive stance). With --azure-file-verify-auth: Shared Key for devstoreaccount1 is verified with pkgs/azureauth (the path-style account is repeated in the signed resource, as the SDK signs it); every SAS gets start/expiry (stored access policy resolved via si), sip, spr and per-operation checks, and devstoreaccount1 service and account SAS signatures are verified. Failures are 403 AuthenticationFailed, AuthorizationPermissionMismatch, AuthorizationResourceTypeMismatch, AuthorizationServiceMismatch, AuthorizationSourceIPMismatch or AuthorizationProtocolMismatch (fileerror constants of the pinned SDK). Service SAS cannot drive share-level operations (create-service-sas permission tables). Bearer needs x-ms-file-request-intent=backup (400 MissingRequiredHeader / InvalidHeaderValue); the token itself is not validated."}
  cors: {status: ok, note: "OPTIONS preflight and actual-request headers follow the Azure Storage CORS reference: rules in order, case-sensitive origin with * wildcards, method list, literal and prefix* headers, 403 CorsPreflightFailure when nothing matches, 400 when Origin or Access-Control-Request-Method is missing, Vary: Origin for GET/HEAD per the response matrix. The two error codes and the Allow-Credentials header are recalled, not verified."}
  quota: {status: ok, note: "Create File, Set File Properties (resize) and Copy File growing usage past the share quota answer 403 ShareSizeLimitReached (Azure Files error codes table). Usage is the sum of content lengths, not written bytes."}
  persistence: {status: ok, note: "Accounts, shares, snapshots, soft-deleted shares, trees, file extents, leases, ACLs and the permission store are in the snapshot (versioned; an unknown version starts empty, null entries are rejected)."}
  wire_headers: {status: ok, note: "x-ms-version 2026-02-06 (the pinned SDK's), x-ms-request-id, Date on every response; failures carry x-ms-error-code plus the XML Error body (no body on HEAD)."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler with operation names, lockmetrics on the backend lock, logger.WithWorker for the listener."}
gaps: []
deferred: []
items_still_open:
  - "NFS: Rename, ListFilesAndDirectories and snapshot diffs ignore NFS properties; file types other than Regular, SymLink and Directory are not modelled; the Create Hard Link and Create Symbolic Link references document no error code for SMB shares (400 InvalidUri is kept, unverified) and no default link count for directories (2 is kept, unverified)."
  - "SMB handles are never modelled, so List Handles is empty and Force Close Handles is a no-op."
  - "Metrics and logging settings are stored only."
  - "Get User Delegation Key and user delegation SAS are unsupported; SAS and Shared Key signatures are only verifiable for devstoreaccount1, verification is off by default, and the Bearer token is not validated. The codes for an unknown si, a service SAS used on a share-level operation (AuthorizationResourceTypeMismatch) and sr=f path mismatch on other accounts are unverified."
  - "Access tier changes are instant, and provisioned IOPS/bandwidth are validated only against the union of the documented ranges (500-102,400 IOPS, 60-10,340 MiB/s): the emulator keeps no account tier (HDD 500-50,000 IOPS and 60-5,120 MiB/s, SSD 3,000-102,400 IOPS and 100-10,340 MiB/s), no provisioned v2 gating and no recommendation formula for unset values."
  - "Metadata keys sent as headers come back in Go's canonical case (Foo) rather than the exact case sent: net/http canonicalises request header names before the handler runs, so the case is unrecoverable without a custom connection reader; list XML returns them as stored."
  - "Copy File only reads emulator file and blob URLs; x-ms-copy-source-authorization is ignored and no CRC64 source check exists (the Azure CRC64 polynomial is unverified)."
  - "LastAccessTime is reported as LastWriteTime and ChangeTime is only set explicitly or on mutation; x-ms-source-allow-trailing-dot (copy and rename sources) is ignored, and structured-body/CRC64 transactional validation is ignored."
  - "ARM fileServices/default accepts cors and protocolSettings but does not apply them, and its soft-delete default is disabled rather than 7 days. ARM fileServices/shares implements Create, Update, Get ($expand=stats, x-ms-snapshot), List ($maxpagesize, $skipToken, $expand=snapshots,deleted), Delete ($include) and Restore; not implemented: the shares lease action, $filter on List, nextAllowed*DowngradeTime, includedBurstIops and maxBurstCreditsForIops (provisioned v2 only), accessTierStatus, and the dashboard UI."
  - "Share snapshots are full in-memory deep copies (metadata and tree), so large trees cost memory per snapshot."
leaks: {status: clean, note: "StartWorker's http.Server is stopped by Shutdown (graceful, then Close); TestMain runs testleak.VerifyTestMain over the SDK-driven suite."}
---

## 2026-11-05 enforcement pass (history)

Evidence (Microsoft Learn, fetched 2026-11-05): Azure Files error codes (lease, FileInvalidPermissionKey, ShareSizeLimitReached, XNfs4FileTypeUnsupportedException), Create a service SAS (file and share permissions, string-to-sign, stored access policy), Create an account SAS (File service operation table, string-to-sign), CORS support for Azure Storage (rule evaluation, Vary matrix, preflight status codes). SDK: azfile v1.5.4 fileerror and generated file client (NFS headers, hardlink/symboliclink restypes).

ARM file shares: learn.microsoft.com/rest/api/storagerp/file-shares (Create, List, Delete pages) and armstorage v1.8.1 FileShareProperties; Delete $include default snapshots, leased-snapshots deletes leased snapshots too, none fails on any snapshot (409 ShareHasSnapshots), 204 when the share does not exist. Create Share provisioned ranges: learn.microsoft.com/rest/api/storageservices/create-share.

## 2026-11-13 parity pass (history)

Evidence (Microsoft Learn, fetched 2026-11-13): Create Hard Link and Create Symbolic Link (share snapshot is 400 InvalidQueryParameterValue; NFS only), Create Share and Set Share Properties (paid bursting max IOPS/bandwidth need x-ms-share-paid-bursting-enabled true, or an already enabled share on Set), Naming and referencing shares, directories, files, and metadata (reserved names, 2,048-character path, SMB trailing dots trimmed unless x-ms-allow-trailing-dot is true, NFS keeps them).
