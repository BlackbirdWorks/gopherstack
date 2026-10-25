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
  SetShareACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stored access policies, max 5, unique ids; stored only, policies are not consulted when authorising SAS."}
  GetShareStats: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ShareUsageBytes is the sum of file content lengths."}
  LeaseShare: {wire: ok, errors: partial, state: ok, persist: ok, note: "acquire/renew/release/change/break with infinite or 15-60s leases, break periods, expiry via the handler clock. Write operations on a leased share require the lease id."}
  CreatePermission: {wire: ok, errors: ok, state: ok, persist: ok, note: "JSON {permission, format}; the key is derived from the SDDL so equal SDDL gives equal keys; 8 KiB cap."}
  GetPermission: {wire: ok, errors: partial, state: ok, persist: n/a, note: "JSON {permission, format}; an unknown key answers 400 InvalidHeaderValue (real code not verified)."}
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
  CopyFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; completes synchronously (status success) from an emulator file or blob; metadata/SMB overrides with the source token; copy status appears on Get Properties."}
  AbortCopyFile: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Copies finish inside the request, so a known id answers 409 NoPendingCopyOperation and a wrong id 409 CopyIdMismatch."}
  RenameFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "Same rules as RenameDirectory; keeps content, metadata, id and lease."}
  LeaseFile: {wire: ok, errors: partial, state: ok, persist: ok, note: "Infinite leases only (as in Azure); acquire/release/change/break."}
families:
  routing: {status: ok, note: "Dedicated listener on a fixed port (10009, --azure-file-port/AZURE_FILE_PORT), no fallback pool, reserved in the shared PortAlloc pool. The account comes from the first path segment or from an <account>.file.<host> Host header; both address one backend, and the shared storage vhost listener forwards .file. hosts here. Ports 10000-10011 are now all assigned to Azure services."}
  auth: {status: partial, note: "Anonymous, Shared Key, Shared Key Lite, SAS and Bearer requests are accepted by default (repo-wide permissive stance). With --azure-file-verify-auth, Shared Key for devstoreaccount1 is verified with pkgs/azureauth (the path-style account is repeated in the signed resource, as the SDK signs it) and SAS tokens get start/expiry checks plus, for devstoreaccount1 service SAS, signature verification against the share/file resource."}
  persistence: {status: ok, note: "Accounts, shares, snapshots, soft-deleted shares, trees, file extents, leases, ACLs and the permission store are in the snapshot (versioned; an unknown version starts empty, null entries are rejected)."}
  wire_headers: {status: ok, note: "x-ms-version 2026-02-06 (the pinned SDK's), x-ms-request-id, Date on every response; failures carry x-ms-error-code plus the XML Error body (no body on HEAD)."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler with operation names, lockmetrics on the backend lock, logger.WithWorker for the listener."}
gaps: []
deferred: []
items_still_open:
  - "NFS shares: hard links, symbolic links, x-ms-owner/-group/-mode and NFS file types are not modelled (the routes answer 400 InvalidUri); NFS only changes the stored protocol and root squash."
  - "SMB handles are never modelled, so List Handles is empty and Force Close Handles is a no-op."
  - "CORS rules are stored but not enforced and OPTIONS preflight is unsupported; metrics and logging settings are stored only."
  - "SAS permissions (sp), stored access policy references (si), account SAS signatures, IP/protocol restrictions and Bearer tokens are not enforced; Get User Delegation Key is unsupported; verification is off by default and limited to the development account."
  - "Share quota is not enforced (no ShareSizeLimitReached), access tier changes are instant, provisioned/bursting options are echoed without validation."
  - "Lease error codes for share operations (LeaseIdMismatchWithLeaseOperation, LeaseNotPresentWithLeaseOperation as 412) and the code for an unknown permission key follow the file-operation names and are not verified against the live service."
  - "Metadata keys sent as headers come back in Go's canonical case (Foo) rather than the exact case sent; list XML returns them as stored."
  - "Copy File is synchronous and only reads emulator file and blob URLs; x-ms-copy-source-authorization and CRC64 source checks are ignored, so Abort Copy never finds a pending copy."
  - "LastAccessTime is reported as LastWriteTime and ChangeTime is only set explicitly or on mutation; trailing-dot handling (x-ms-allow-trailing-dot) and structured-body/CRC64 transactional validation are ignored."
  - "Share soft delete follows ARM PUT/PATCH/GET storageAccounts/{a}/fileServices/default (shareDeleteRetentionPolicy only; cors and protocolSettings sent through ARM are accepted but not applied, and the default is disabled rather than 7 days). ARM fileServices/shares resources and the dashboard UI do not exist yet."
  - "Share snapshots are full in-memory deep copies (metadata and tree), so large trees cost memory per snapshot."
leaks: {status: clean, note: "StartWorker's http.Server is stopped by Shutdown (graceful, then Close); TestMain runs testleak.VerifyTestMain over the SDK-driven suite."}
---
