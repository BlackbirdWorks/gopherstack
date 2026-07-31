---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mediastoredata
sdk_module: aws-sdk-go-v2/service/mediastoredata@v1.29.19   # version audited against
last_audit_commit: f0a0c951412c5ff4f0122ab4503605c44c2fef49
last_audit_date: 2026-07-31
overall: A            # all 4 fabricated error __type strings replaced with real AWS error names; one genuine wire bug fixed (Range 416 __type); this pass fixed a real browser-routing bug (see routing family note) -- held at A because it is fixed, not deferred
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "PutObjectOutput{ContentSHA256,ETag,StorageClass} JSON body fields verified against deserializeOpDocumentPutObjectOutput. errors: this pass replaced 3 fabricated, non-existent exception names (InvalidPathException, InvalidStorageClassException, InvalidContentSHA256Exception) with real AWS error names -- ValidationException (path/storage-class validation, matching the AWS-wide/gopherstack-wide convention already used by this same handler's ListItems MaxResults check) and XAmzContentSHA256Mismatch (the real S3-family error for a declared X-Amz-Content-Sha256 that doesn't match the actual body, verified via AWS SDK GitHub issues across multiple language SDKs). Per the real deserializeOpErrorPutObject switch, PutObject's OWN narrow modeled-error list is only {ContainerNotFoundException, InternalServerError} -- ValidationException/XAmzContentSHA256Mismatch are real AWS names but not officially enumerated for this specific op in the public smithy model (see gaps); this is the closest-to-correct choice achievable without live-AWS access, and a definite improvement over inventing new strings."}
  GetObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "body, Content-Type/Length/Range, ETag, Last-Modified, Cache-Control, X-Amz-Content-Sha256, Accept-Ranges all verified against deserializers.go httpBindings for GetObjectOutput. Range (single-range bytes=a-b, suffix, open) -> 206 + Content-Range; unsatisfiable -> 416. BUG FIXED this pass: the 416 response's __type was the fabricated \"InvalidRangeException\" even though the HTTP status (416) was already correct -- the real modeled name is \"RequestedRangeNotSatisfiableException\" (types.RequestedRangeNotSatisfiableException), confirmed via deserializeOpErrorGetObject's switch. A real client's errors.As(&types.RequestedRangeNotSatisfiableException{}) would NOT have matched gopherstack's old response; now it does. Conditional headers (If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since) implemented, not part of the modeled GetObjectInput but harmless/standard-HTTP. Per-op modeled errors confirmed via deserializeOpErrorGetObject: {ContainerNotFoundException, InternalServerError, ObjectNotFoundException, RequestedRangeNotSatisfiableException} -- all 4 real names, all correctly used."}
  DeleteObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "Per-op modeled errors confirmed via deserializeOpErrorDeleteObject: {ContainerNotFoundException, InternalServerError, ObjectNotFoundException}. ObjectNotFoundException correctly used; DeleteObjectOutput is void (matches c.NoContent(200))."}
  DescribeObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "HEAD /{Path+}; correctly does NOT support Range (DescribeObjectInput has no Range field in the real SDK) and does not set StatusCode (DescribeObjectOutput has no StatusCode field, unlike GetObjectOutput). Per-op modeled errors confirmed via deserializeOpErrorDescribeObject: {ContainerNotFoundException, InternalServerError, ObjectNotFoundException}."}
  ListItems: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET / always (Path/MaxResults/NextToken are query params, never part of the URL path -- confirmed via serializers.go: opPath is literally \"/\" for ListItems). Item{Name,Type,ContentLength,ContentType,ETag,LastModified} field set and JSON key names verified byte-for-byte against deserializeDocumentItem AND types.Item -- gopherstack's internal Item struct carries extra CacheControl/StorageClass/SHA256 fields but these correctly never reach the wire (handler.go's itemEntry struct only serializes the 6 real fields). LastModified emitted as epoch-seconds JSON number (matches ParseEpochSeconds), MaxResults bounded 1-1000, folder synthesis from path prefixes with object/folder name-collision dedup verified by TestInMemoryBackend_ListItems_NoNameCollision. Per-op modeled errors confirmed via deserializeOpErrorListItems: {ContainerNotFoundException, InternalServerError} only (no ObjectNotFoundException) -- gopherstack never returns ObjectNotFoundException from ListItems, correct."}
# Families audited as a group (when per-op is impractical):
families:
  routing: {status: ok, note: "RouteMatcher matches on the \"mediastoredata\" marker (msdMatchPriority=87 > S3's 0). Verified real aws-sdk-go-v2 UA marker is literally \"api/mediastoredata#<version>\" (api_client.go addClientUserAgent -> AddSDKAgentKeyValue(APIMetadata, \"mediastoredata\", ...)), so the substring match is correct and won't collide with plain \"mediastore\" (no data suffix). BUG FOUND AND FIXED this pass: the prior audit's \"verified correct\" claim above was true only for aws-sdk-go-v2 and did not hold for a browser. RouteMatcher checked only the User-Agent header; the Fetch spec forbids browser JS from setting User-Agent, so the AWS SDK for JavaScript in a browser puts its SDK identification exclusively in X-Amz-User-Agent instead -- every browser-originated MediaStore Data request (the dashboard's own UI, via @aws-sdk/client-mediastore-data) fell through to S3's priority-0 catch-all and was rejected with an S3 XML error the dashboard then failed to parse as JSON. Confirmed against the actual installed npm package (ui/node_modules/@aws-sdk/core's userAgentMiddleware: `if (options.runtime !== \"browser\") { headers[USER_AGENT] = ... } else { headers[X_AMZ_USER_AGENT] = ... }`) that the real browser marker is ALSO not simply \"mediastoredata\" in a different header: MediaStore Data's JS SDK serviceId is \"MediaStore Data\" (with a space), which the SDK's user-agent escaping turns into \"MediaStore-Data\" (hyphenated, PascalCase) -- a different literal string from aws-sdk-go-v2's module-path-derived \"mediastoredata\", not just a case difference. Fixed via the new pkgs/service.MatchesUserAgentMarker helper (shared with the same bug class in docdb/neptune/appsync), which checks both User-Agent and X-Amz-User-Agent case-insensitively; mediastoredata passes it both the \"mediastoredata\" and \"mediastore-data\" markers to cover both SDKs' spellings. ExtractOperation/Handler() dispatch on method (PUT/GET/DELETE/HEAD) then disambiguate GET ListItems-vs-GetObject purely by URL.Path == \"/\" -- this is CORRECT per the real SDK: ListItems always serializes to path \"/\" with Path as a query param, it is never a GET on a folder path (confirmed via awsRestjson1_serializeOpListItems: opPath, opQuery := httpbinding.SplitURI(\"/\")). GetObject/DescribeObject/PutObject/DeleteObject all serialize Path via {Path+} greedy URI capture, matched via r.URL.Path directly (no separate matcher regex to verify). TestSDKCompleteness confirms GetSupportedOperations() == exactly the 5 real SDK ops, no invented ops."
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to backend; backendSnapshot versioned (mediastoredataSnapshotVersion=1), region-nested via store.Table[Object], round-tripped in persistence_test.go including all Object fields."}
gaps:
  - "PutObject/GetObject's ValidationException and PutObject's XAmzContentSHA256Mismatch, while real AWS error names (unlike the fabricated names they replaced this pass), are not officially enumerated in mediastoredata's own narrow per-op error models (deserializeOpErrorPutObject only lists ContainerNotFoundException/InternalServerError; deserializeOpErrorGetObject adds ObjectNotFoundException/RequestedRangeNotSatisfiableException but still no ValidationException). Both conditions ARE reachable by a real (non-conformant-only) SDK caller though: client-side validators.go only checks Path != nil (an empty non-nil Path, a Path containing '..', or an out-of-range StorageClass string all pass client-side validation and would hit a real server), so this isn't dead/unreachable code. Without live-AWS access there is no way to confirm the exact wire name real AWS uses for these cases; ValidationException/XAmzContentSHA256Mismatch are the closest verified-real AWS error names (established gopherstack-wide convention / confirmed real S3-family error respectively) and are a strict improvement over the wholly-invented names they replaced. The empty-Path sub-case specifically IS unreachable via a real SDK client (client serializers reject Path==nil or len(Path)==0 with a local SerializationError before the request is ever sent), so that one branch can never be observed on the wire at all."
  - "x-amz-upload-availability STREAMING is stored/echoed but has no real chunked/progressive-download semantics (an object is only ever visible after PutObject fully returns) -- real MediaStore streams partial reads to STREAMING objects while still uploading and ignores Range for such objects mid-upload. Not modeled; would require a bigger feature (partial/chunked PutObject) to emulate faithfully."
  - "ContainerNotFoundException (a real modeled error, present in every op's model) is never returned by this handler -- mediastoredata has no notion of containers in gopherstack's per-region flat object store (containers are provisioned by the separate mediastore service, not mediastoredata). Deferred: would require cross-referencing services/mediastore's container registry, which is out of scope for a mediastoredata-only pass (cross-service change)."
deferred:
  - cross-service container-existence validation against services/mediastore (see gaps)
leaks: {status: clean, note: "no goroutines/janitors; region map lazily allocated under Lock, read via non-allocating stateRO under RLock -- no torn-state risk found. Every b.mu.Lock/RLock in objects.go, items.go, store.go, persistence.go is immediately followed by a defer Unlock/RUnlock; no early-return bypasses found this pass."}
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
  (`StorageClass` enum: `TEMPORAL` only). Fixed in an earlier pass; still correct.

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
  on PutObject, which is extra/unused by the SDK but harmless.

- The 4 real error types (`ContainerNotFoundException`, `InternalServerError`,
  `ObjectNotFoundException`, `RequestedRangeNotSatisfiableException`) are all correctly
  wired for the paths that can actually produce them today (ObjectNotFoundException on
  missing object for Get/Describe/Delete, RequestedRangeNotSatisfiableException on bad
  Range -- fixed this pass, see ops.GetObject -- InternalServerError as the catch-all
  default). `ContainerNotFoundException` is unreachable because this service has no
  container-existence concept (see gaps).

- **This pass's fixes (2026-07-24):** the previous audit (2026-07-13) identified but left
  in place 3 fabricated exception names (`InvalidPathException`, `InvalidStorageClassException`,
  `InvalidContentSHA256Exception`) as a known gap, reasoning "no known correct replacement
  code exists in the model." This pass replaced them with real AWS error names instead of
  leaving them fabricated: `ValidationException` (the established AWS-wide/gopherstack-wide
  convention for parameter validation, already used elsewhere in this same handler for the
  ListItems MaxResults bound check -- so this also fixes a same-file internal
  inconsistency) and `XAmzContentSHA256Mismatch` (confirmed via AWS SDK issue trackers as
  the real error S3-family services return for a declared-vs-actual body hash mismatch;
  this is a generic SigV4-payload-integrity check, not mediastoredata app logic, so it's
  reasonable it isn't in mediastoredata's own narrow per-op model, and it is NOT redundant
  with `pkgs/httputils.SigV4Validator`, which only checks that the request signature is
  self-consistent with whatever hash the client declared -- it never independently
  recomputes the hash of the actually-received bytes). Separately, and NOT previously
  flagged: `GetObject`'s 416 response had the fabricated `InvalidRangeException` for its
  `__type` even though the HTTP status code (416) was already correct -- fixed to the real
  modeled `RequestedRangeNotSatisfiableException`, since a real client's typed error
  assertion depends on that exact string, not just the status code. All 3 error-related
  edits are locked in by new test assertions on the wire `__type` field (previously the
  tests only checked HTTP status, not the error body) in handler_test.go's
  TestMediaStoreData_PathValidation, TestMediaStoreData_StorageClassValidation,
  TestMediaStoreData_ContentSHA256Verification, and TestMediaStoreData_RangeRequests.
