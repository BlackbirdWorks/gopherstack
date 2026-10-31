---
service: ociobjectstorage
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 3490bdbb3
last_audit_date: 2026-10-25
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetNamespace: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /n returns the single configured namespace (default gopherstack) as a JSON string."}
  GetNamespaceMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "Default S3 and Swift compartment ids; a namespace other than the configured one is 404 NamespaceNotFound."}
  UpdateNamespaceMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /n/{ns} sets defaultS3CompartmentId and defaultSwiftCompartmentId."}
  CreateBucket: {wire: ok, errors: partial, state: ok, persist: ok, note: "compartmentId required; name 1-256 of [A-Za-z0-9._-]; publicAccessType, storageTier (Standard or Archive), versioning (Enabled or Disabled), objectEventsEnabled, freeformTags, definedTags, metadata, kmsKeyId, autoTiering stored. Returns etag and Location. Duplicate is 409 BucketAlreadyExists. Bucket OCIDs are ocid1.bucket.oc1.us-ashburn-1.<hex>."}
  GetBucket: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fields=approximateCount,approximateSize computed on demand; If-Match / If-None-Match (304)."}
  HeadBucket: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header only."}
  UpdateBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "Partial update with If-Match (412 NoEtagMatch); Enabled to Suspended allowed, back to Disabled is rejected; name change renames the bucket."}
  DeleteBucket: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 BucketNotEmpty while any object version, delete marker or multipart upload remains."}
  ListBuckets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; limit and page via pkgs/page, opc-next-page header."}
  MakeBucketWritable: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Accepted for an existing bucket; replication is not emulated so no bucket is ever read-only."}
  PutObject: {wire: ok, errors: partial, state: ok, persist: ok, note: "Content-MD5 verified (400 InvalidDigest on mismatch), opc-content-md5 returned, opc-meta-*, content-type/language/encoding/disposition, cache-control, storage-tier, If-Match / If-None-Match (412). Versioned buckets append a version; Suspended uses version null; retention rules block overwrites."}
  GetObject: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Single Range (bytes=a-b, a-, -n; 416 InvalidRange), versionId, conditionals incl. 304, httpResponse* query overrides, opc-meta-*, content-md5 / opc-multipart-md5, archival-state. Archived or Restoring objects are 409 NotRestored."}
  HeadObject: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Same headers as GetObject without a body; works on archived objects."}
  DeleteObject: {wire: ok, errors: partial, state: ok, persist: ok, note: "Unversioned removes; versioned adds a delete marker (is-delete-marker, version-id headers); versionId removes one version permanently; retention rules return 409 ObjectUnderRetention."}
  ListObjects: {wire: ok, errors: ok, state: ok, persist: n/a, note: "prefix, delimiter (prefixes), start (inclusive), end (exclusive), startAfter, limit, fields=name,size,etag,md5,timeCreated,timeModified,storageTier,archivalState; nextStartWith when truncated."}
  ListObjectVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Versions and delete markers, newest first per name; page paging via opc-next-page."}
  CopyObject: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id; the copy runs synchronously so the work request is already COMPLETED. Same-region only (destinationRegion must be us-ashburn-1). Source/destination If-Match and destination If-None-Match, sourceVersionId, destinationObjectMetadata, destinationObjectStorageTier."}
  RenameObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "Preconditions on source and new name; the source is deleted (marker in a versioned bucket)."}
  BatchDeleteObjects: {wire: ok, errors: partial, state: ok, persist: ok, note: "Per-object deleted[] and failed[] (statusCode, errorMessage); isSkipDeletedResult honoured."}
  RestoreObjects: {wire: ok, errors: partial, state: ok, persist: ok, note: "Archive tier only; hours 1-240 (default 24). Archived becomes Restoring, becomes Restored one hour later, and returns to Archived when the window closes; all transitions follow the injectable backend clock."}
  UpdateObjectStorageTier: {wire: ok, errors: ok, state: ok, persist: ok, note: "Standard, InfrequentAccess or Archive; an archived object must be restored first."}
  CreateMultipartUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "Object metadata, content headers and storageTier captured at create; uploads survive snapshot/restore."}
  UploadPart: {wire: ok, errors: ok, state: ok, persist: ok, note: "Part numbers 1-10000, Content-MD5 check, etag and opc-content-md5 returned. No minimum part size is enforced."}
  CommitMultipartUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "Parts validated against partNum+etag, assembled in order; opc-multipart-md5 is base64(md5(concat part md5s))-N. 400 InvalidUploadPart, 404 NoSuchUpload."}
  AbortMultipartUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ListMultipartUploads: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged."}
  ListMultipartUploadParts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged, ordered by part number."}
  CreatePreauthenticatedRequest: {wire: ok, errors: partial, state: ok, persist: ok, note: "All six accessTypes, bucketListingAction ListObjects for AnyObjectRead(Write). Returns id, accessUri (/p/<token>/n/<ns>/b/<bucket>/o/<name>), fullPath. Expiry must be in the future."}
  GetPreauthenticatedRequest: {wire: ok, errors: partial, state: ok, persist: n/a, note: "No accessUri, as in OCI."}
  ListPreauthenticatedRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "objectNamePrefix filter, paged."}
  DeletePreauthenticatedRequest: {wire: ok, errors: partial, state: ok, persist: ok, note: ""}
  PARAccess: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Unsigned GET/HEAD/PUT through /p/<token>/...; scoped by access type and object name (or prefix for AnyObject*), expiry follows the backend clock. Every denial is 404 NotAuthorizedOrNotFound."}
  PutObjectLifecyclePolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "Rules with DELETE/ARCHIVE/INFREQUENT_ACCESS/ABORT, targets objects, previous-object-versions, multipart-uploads, DAYS/YEARS, name filters (inclusion/exclusion patterns and prefixes). Returns etag."}
  GetObjectLifecyclePolicy: {wire: ok, errors: partial, state: ok, persist: n/a, note: ""}
  DeleteObjectLifecyclePolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: ""}
  CreateRetentionRule: {wire: ok, errors: partial, state: ok, persist: ok, note: "Duration in DAYS/YEARS (omitted means indefinite); timeRuleLocked must be at least 14 days ahead."}
  GetRetentionRule: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListRetentionRules: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateRetentionRule: {wire: ok, errors: partial, state: ok, persist: ok, note: "If-Match; a locked rule can only be extended."}
  DeleteRetentionRule: {wire: ok, errors: partial, state: ok, persist: ok, note: "A rule whose lock time has passed cannot be deleted (409)."}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "Copy work requests only; always COMPLETED."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required."}
  CancelWorkRequest: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Always 409 because requests finish synchronously."}
  ListWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always empty."}
  ListWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "One completion entry."}
families:
  object_events: {status: partial, note: "Buckets with objectEventsEnabled (CreateBucket, UpdateBucket) report createobject, updateobject and deleteobject to the sink set with SetObjectEventSink (PutObject, CommitUploadedParts, CopyObject destination, DeleteObject, BatchDeleteObjects); internal/app wires it to OCI Events (docs.oracle.com/en-us/iaas/Content/Events/Reference/eventsproducers.htm). RenameObject and bucket events are not emitted."}
  auth: {status: ok, note: "pkgs/ociauth parses the draft-cavage Authorization header (keyId tenancy/user/fingerprint). Accept-any by default. A request signed with a key registered through POST /_gopherstack/oci/keys {tenancy,user,publicKey} is verified for real: (request-target), host and date must be signed, date within 5 minutes, a signed x-content-sha256 must match the body, RSA PKCS1v15 or PSS SHA-256. --oci-objectstorage-enforce-auth additionally rejects unsigned requests and unknown keys, except anonymous reads of ObjectRead / ObjectReadWithoutList buckets. Registered keys persist."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. Documented codes (InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NamespaceNotFound, NoEtagMatch) follow the OCI error reference; BucketNotFound, ObjectNotFound, BucketAlreadyExists, BucketNotEmpty, NoSuchUpload, InvalidUploadPart, InvalidDigest, InvalidRange, IfNoneMatchFailed, NotRestored, ObjectUnderRetention and LifecyclePolicyNotFound are best-effort and not verified against the live service."}
  versioning: {status: ok, note: "Enabled, Suspended (version id null) and Disabled; delete markers; listing with ListObjectVersions."}
  lifecycle_enforcement: {status: ok, note: "SweepLifecycle applies every enabled rule against the injectable backend clock; the listener runs it every minute."}
  retention: {status: ok, note: "Objects younger than the longest rule duration cannot be deleted or overwritten."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-objectstorage-port / OCI_OBJECTSTORAGE_PORT (default 8150); never on the shared AWS router; cli.go reserves the port in the shared pool. Point an OCI SDK client at it with client.Host = http://localhost:8150."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "Resource principal and instance principal tokens (and delegation/session tokens) are not understood; only API-key signatures are verified."
  - "Cross-region and cross-tenancy CopyObject, replication policies and replication sources are not implemented."
  - "Customer-supplied (opc-sse-customer-*) and KMS keys are accepted but not applied; bucket reencrypt and ReencryptObject are not routed."
  - "Private endpoints (/n/{ns}/pe) are not implemented."
  - "The OCI Amazon S3 compatibility API and Swift API are out of scope."
  - "Checksum headers opc-content-crc32c/sha256/sha384 and opc-checksum-algorithm are ignored; only MD5 is computed and verified."
  - "Multipart: no minimum part size, no part overwrite conflicts, and the multipart md5 header is the only multipart checksum."
  - "Restore completes one fixed hour after the request regardless of tier; the time-of-archival header is not returned."
  - "Several error codes are unverified against the live service (see families.error_envelope)."
  - "Namespaces are single-tenant: only one namespace is served and no compartment or IAM policy checks apply."
  - "Tag values and defined-tag namespaces are stored verbatim, not validated against a tag namespace."
