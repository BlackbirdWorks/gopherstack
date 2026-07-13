---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mediastoredata
sdk_module: aws-sdk-go-v2/service/mediastoredata@v1.29.19   # version audited against
last_audit_commit: 669b02ee0e53a5fb8796a317745e41f80638e107
last_audit_date: 2026-07-13
overall: B            # already-accurate service; one confirmed wire-shape bug fixed op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutObject: {wire: ok, errors: partial, state: ok, persist: ok, note: "fixed: StorageClass 'STANDARD' was wrongly accepted; only 'TEMPORAL' is a real MediaStore Data StorageClass (STANDARD is an UploadAvailability value). errors=partial: InvalidPathException/InvalidStorageClassException/InvalidContentSHA256Exception are not in the real SDK's 4-error model (ContainerNotFoundException, InternalServerError, ObjectNotFoundException, RequestedRangeNotSatisfiableException) -- harmless (SDK falls back to GenericAPIError on unknown __type) but not a modeled AWS error name; see gaps."}
  GetObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "body, Content-Type/Length/Range, ETag, Last-Modified, Cache-Control, X-Amz-Content-Sha256, Accept-Ranges all verified against deserializers.go httpBindings for GetObjectOutput. Range (single-range bytes=a-b, suffix, open) -> 206 + Content-Range; unsatisfiable -> 416. Conditional headers (If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since) implemented, not part of the modeled GetObjectInput but harmless/standard-HTTP."}
  DeleteObject: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "HEAD /{Path+}; correctly does NOT support Range (DescribeObjectInput has no Range field in the real SDK) and does not set StatusCode (DescribeObjectOutput has no StatusCode field, unlike GetObjectOutput)."}
  ListItems: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET / always (Path/MaxResults/NextToken are query params, never part of the URL path -- confirmed via serializers.go: opPath is literally \"/\" for ListItems). LastModified emitted as epoch-seconds JSON number (matches deserializeDocumentItem's ParseEpochSeconds), MaxResults bounded 1-1000, folder synthesis from path prefixes with object/folder name-collision dedup verified by TestBackend_ListItems_NoNameCollision."}
# Families audited as a group (when per-op is impractical):
families:
  routing: {status: ok, note: "RouteMatcher matches on User-Agent substring \"mediastoredata\" (msdMatchPriority=87 > S3's 0). Verified real aws-sdk-go-v2 UA marker is literally \"api/mediastoredata#<version>\" (api_client.go addClientUserAgent -> AddSDKAgentKeyValue(APIMetadata, \"mediastoredata\", ...)), so the substring match is correct and won't collide with plain \"mediastore\" (no data suffix). ExtractOperation/Handler() dispatch on method (PUT/GET/DELETE/HEAD) then disambiguate GET ListItems-vs-GetObject purely by URL.Path == \"/\" -- this is CORRECT per the real SDK: ListItems always serializes to path \"/\" with Path as a query param, it is never a GET on a folder path (confirmed via awsRestjson1_serializeOpListItems: opPath, opQuery := httpbinding.SplitURI(\"/\")). GetObject/DescribeObject/PutObject/DeleteObject all serialize Path via {Path+} greedy URI capture, matched via r.URL.Path directly (no separate matcher regex to verify)."
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to backend; backendSnapshot versioned (mediastoredataSnapshotVersion=1), region-nested via store.Table[Object], round-tripped in persistence_test.go including all Object fields."}
gaps:
  - "PutObject's ad-hoc validation error codes (InvalidPathException, InvalidStorageClassException, InvalidContentSHA256Exception) are not part of the real mediastoredata SDK's error model (types/errors.go only defines ContainerNotFoundException, InternalServerError, ObjectNotFoundException, RequestedRangeNotSatisfiableException). A conformant SDK client can never trigger these paths anyway (client-side validators.go rejects nil/empty Path before the request is even sent), so this only affects raw-HTTP/curl-style callers; the SDK itself degrades gracefully to smithy.GenericAPIError on an unrecognized __type. Left as-is (no known correct replacement code exists in the model); not fixed this pass."
  - "x-amz-upload-availability STREAMING is stored/echoed but has no real chunked/progressive-download semantics (an object is only ever visible after PutObject fully returns) -- real MediaStore streams partial reads to STREAMING objects while still uploading and ignores Range for such objects mid-upload. Not modeled; would require a bigger feature (partial/chunked PutObject) to emulate faithfully."
  - "ContainerNotFoundException (a real modeled error) is never returned by this handler -- mediastoredata has no notion of containers in gopherstack's per-region flat object store (containers are provisioned by the separate mediastore service, not mediastoredata). Deferred: would require cross-referencing services/mediastore's container registry, which is out of scope for a mediastoredata-only pass (cross-service change)."
deferred:
  - cross-service container-existence validation against services/mediastore (see gaps)
leaks: {status: clean, note: "no goroutines/janitors; region map lazily allocated under Lock, read via non-allocating stateRO under RLock -- no torn-state risk found."}
---

## Notes

- Protocol: restjson1. PutObject/GetObject/DeleteObject/DescribeObject use `/{Path+}`
  (greedy URI capture of the object path, no JSON request/response body except PutObject's
  raw byte stream and PutObject's small JSON output). ListItems is the only op with a
  JSON request-shape-like surface, and even it is GET `/` with everything as query params
  (Path, MaxResults, NextToken) -- it is NEVER a GET on a folder-shaped path. This
  confirms the codebase's `ExtractOperation`/`Handler()` disambiguation of ListItems vs
  GetObject via `r.URL.Path == "/"` is correct, not a bug (a plausible-looking bug that
  turned out to be right after checking the real serializer).

- **StorageClass has exactly one real value: `TEMPORAL`.** `STANDARD` is easily confused
  with it because it IS a valid value -- but of the unrelated `x-amz-upload-availability`
  header (`UploadAvailability` enum: `STANDARD` | `STREAMING`), not `x-amz-storage-class`
  (`StorageClass` enum: `TEMPORAL` only). This was gopherstack's one real bug this pass:
  `isValidStorageClass` accepted both, and `PutObject` would happily store and echo back
  `StorageClass: "STANDARD"`, which cannot happen against real AWS. Fixed in backend.go;
  test cases in backend_test.go/handler_test.go/persistence_test.go that previously
  asserted "STANDARD storage class is accepted" were updated to assert rejection (or
  switched their fixture's StorageClass to TEMORAL where the test was about something
  else, e.g. UploadAvailability or the persistence round-trip).

- `GetObjectOutput`/`DescribeObjectOutput` `LastModified` is an HTTP-date header
  (`smithytime.ParseHTTPDate`, RFC1123-ish `http.TimeFormat`), NOT epoch seconds --
  correctly implemented via `obj.LastModified.UTC().Format(http.TimeFormat)`. Contrast
  with `ListItems`' `Item.LastModified`, which IS epoch-seconds as a JSON number
  (`smithytime.ParseEpochSeconds` in the JSON-body deserializer) -- also correctly
  implemented (`float64(item.LastModified.Unix())`). Don't conflate the two: same field
  name, different wire representation depending on whether it's a header or a JSON body
  field.

- `PutObjectOutput.ETag` is JSON-body-only in the real SDK (no HTTP header binding
  exists for it in deserializers.go) -- gopherstack also sets an `ETag` response header
  on PutObject, which is extra/unused by the SDK but harmless. Left in place; comment
  corrected so a future auditor doesn't mistake it for a documented requirement.

- The 4 real error types (`ContainerNotFoundException`, `InternalServerError`,
  `ObjectNotFoundException`, `RequestedRangeNotSatisfiableException`) are all correctly
  wired for the paths that can actually produce them today (ObjectNotFoundException on
  missing object for Get/Describe/Delete, RequestedRangeNotSatisfiableException on bad
  Range, InternalServerError as the catch-all default). `ContainerNotFoundException` is
  unreachable because this service has no container-existence concept (see gaps).
