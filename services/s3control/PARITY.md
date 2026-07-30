service: s3control
sdk_module: aws-sdk-go-v2/service/s3control@v1.73.0
last_audit_commit: fbba4962d
last_audit_date: 2026-07-30
overall: B            # DOWNGRADED from A this pass (gopherstack-tir4). A field-by-field diff of
                       # ~35 of the ~55 previously-unaudited response/request types against
                       # deserializers.go/serializers.go found a dense cluster of severe wire-shape
                       # bugs the prior pass's route-level audit could not have caught (it
                       # explicitly flagged response-body shape as "sampled, not exhaustive").
                       # Several are REQUEST-side bugs that would make a real aws-sdk-go-v2 client
                       # unable to successfully call the op at all (wrong payload root element:
                       # PutBucketTagging, PutBucketVersioning, PutStorageLensConfiguration; wrong
                       # field name entirely: SubmitMultiRegionAccessPointRoutes's "RouteUpdates"
                       # vs the emulator's "Routes"; wrong transport entirely: UntagResource's
                       # TagKeys travel as repeated query params in the real API, not an XML body,
                       # so every real UntagResource call was silently deleting zero tags). Others
                       # are RESPONSE-side envelope bugs that would make a real client see an
                       # empty list every time (ListCallerAccessGrants wrapped under the wrong key;
                       # ListStorageLensConfigurations/ListStorageLensGroups wrapped under a
                       # nonexistent list element when the real SDK flattens them; ListMultiRegionAccessPoints
                       # used member name "item" instead of "AccessPoint"). Others are fabricated
                       # fields with no real counterpart (GetAccessPointForObjectLambda's
                       # ObjectLambdaAccessPointArn, GetBucket's BucketArn/OutpostId,
                       # ListCallerAccessGrants' AccessGrantId). All fixed this pass with new
                       # regression tests asserting the literal nested envelope (not substrings).
                       # See the "Wire-shape field-diff audit (gopherstack-tir4)" section below for
                       # the full per-type accounting, including what was NOT reached.

# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetBucketLifecycleConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "route suffix was '/lifecycle', real SDK is '/lifecycleconfiguration' -- op was UNREACHABLE via real SDK clients; handler body already used the correct suffix, only the route matcher was wrong. Fixed."}
  PutBucketLifecycleConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same route bug as GetBucketLifecycleConfiguration, fixed"}
  DeleteBucketLifecycleConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same route bug as GetBucketLifecycleConfiguration, fixed"}
  PutMultiRegionAccessPointPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "route used '/put_policy' (underscore); real SDK URI is '/put-policy' (hyphen) -- UNREACHABLE via real SDK. Fixed."}
  GetMultiRegionAccessPointPolicyStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "route suffix was '/policyStatus'; real SDK uses all-lowercase '/policystatus' for MRAP specifically (unlike AccessPoint/ObjectLambda, which really do use camelCase '/policyStatus' -- verified both, only MRAP was wrong). UNREACHABLE via real SDK. Fixed."}
  ListAccessPointsForDirectoryBuckets: {wire: ok, errors: ok, state: ok, persist: ok, note: "route was '/accesspointfordirectories' (plural); real SDK URI is '/accesspointfordirectory' (singular). UNREACHABLE via real SDK. Fixed."}
  ListCallerAccessGrants: {wire: ok, errors: ok, state: ok, persist: ok, note: "route was '/accessgrantsinstance/caller-grants'; real SDK URI is '/accessgrantsinstance/caller/grants' (path segment, not hyphenated). UNREACHABLE via real SDK. Fixed."}
  ListAccessGrants: {wire: ok, errors: ok, state: ok, persist: ok, note: "was routed on the same singular path as CreateAccessGrant ('/accessgrantsinstance/grant'); real SDK ListAccessGrants URI is plural '/accessgrantsinstance/grants'. UNREACHABLE via real SDK. Added pathAccessGrantsList const, fixed both extract+dispatch."}
  ListAccessGrantsLocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "same singular-vs-plural bug as ListAccessGrants ('/location' vs real '/locations'). UNREACHABLE via real SDK. Added pathAccessGrantsLocationsList const, fixed."}
  UpdateJobPriority: {wire: ok, errors: ok, state: ok, persist: ok, note: "route required http.MethodPut; real SDK sends POST for this op (it's not a pure REST-semantic PUT). UNREACHABLE via real SDK. Fixed method check to MethodPost in both extract+dispatch. THIS PASS: also fixed GetJob/UpdateJobDetails/UpdateJobPriority/UpdateJobStatus returning the wrong AWS error code (generic ErrNotFound == \"NoSuchPublicAccessBlockConfiguration\") on a missing job -- now errJobNotFound (\"NoSuchJob\"). See jobs.go."}
  UpdateJobStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "same PUT-vs-POST bug as UpdateJobPriority. UNREACHABLE via real SDK. Fixed. See UpdateJobPriority note for the error-code fix in this pass."}
  CreateAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: real bug -- GetAccessPointOutput carries PublicAccessBlockConfiguration inline per aws-sdk-go-v2 (there is no standalone Get/Put/DeleteAccessPointPublicAccessBlock op; see families.access-point-crud), but the response never included it even though CreateAccessPoint already stored it. Fixed: handleGetAccessPoint now reads the per-AP PAB and includes PublicAccessBlockConfiguration when set. Also fixed: wrong error code on missing AP (generic ErrNotFound == \"NoSuchPublicAccessBlockConfiguration\") -- now errAccessPointNotFound (\"NoSuchAccessPoint\")."}
  DeleteAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: fixed wrong error code on missing AP (see GetAccessPoint), and fixed a ghost-map-row leak -- delete only cleaned accessPointPolicies, leaving scope/per-AP-PAB/generic-resource-tags behind forever. Now cascade-cleans all four."}
  ListAccessPoints: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessPointPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: fixed wrong error code on missing AP; split the \"AP missing\" case (errAccessPointNotFound / NoSuchAccessPoint) from the \"AP exists but no policy set\" case, which now correctly returns the new errAccessPointPolicyNotFound sentinel (\"NoSuchAccessPointPolicy\") instead of also claiming NoSuchAccessPoint."}
  PutAccessPointPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: fixed wrong error code on missing AP (see GetAccessPoint note)."}
  DeleteAccessPointPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: fixed wrong error code on missing AP (see GetAccessPoint note)."}
  GetAccessPointPolicyStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessPointScope: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: now round-trips through Snapshot/Restore -- see families.persistence-gap."}
  PutAccessPointScope: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccessPointScope: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessPointPublicAccessBlock: {status: DELETED, note: "THIS PASS: this op, PutAccessPointPublicAccessBlock, and DeleteAccessPointPublicAccessBlock were gopherstack-invented -- aws-sdk-go-v2/service/s3control has no such standalone operations. DELETED per parity policy (no fabricated AWS surface): removed from GetSupportedOperations(), removed the '/publicAccessBlock' sub-resource route and its 3 handler funcs. The underlying real feature (PublicAccessBlockConfiguration travels INLINE on CreateAccessPoint/GetAccessPoint, confirmed via aws-sdk-go-v2's CreateAccessPointInput/GetAccessPointOutput) is preserved: the backend storage methods (Get/Put/DeleteAccessPointPublicAccessBlock as internal Go methods, not routed HTTP ops) survive and now correctly feed GetAccessPoint's response (see GetAccessPoint note) -- this was itself a real, previously-unfixed gap."}
  PutAccessPointPublicAccessBlock: {status: DELETED, note: "see GetAccessPointPublicAccessBlock"}
  DeleteAccessPointPublicAccessBlock: {status: DELETED, note: "see GetAccessPointPublicAccessBlock"}
  GetPublicAccessBlock: {wire: ok, errors: ok, state: ok, persist: ok, note: "simplified to delegate to handleBackendError instead of a redundant hand-rolled plain-text 404/500; now emits proper XML"}
  PutPublicAccessBlock: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePublicAccessBlock: {wire: ok, errors: ok, state: ok, persist: ok, note: "same simplification as GetPublicAccessBlock"}
  CreateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "real state mutation (JobID/Status/CreationTime), not a disguised no-op"}
  DescribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: fixed wrong error code on missing job (see UpdateJobPriority note)."}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccessGrantsLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAccessGrantsLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessGrantsLocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "see route fix above"}
  CreateAccessGrant: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessGrants: {wire: ok, errors: ok, state: ok, persist: ok, note: "see route fix above"}
  DeleteAccessGrant: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: ghost-map-row leak fix -- delete left generic resourceTags behind forever; now cascade-cleaned via the grant's AccessGrantArn."}
  DeleteAccessGrantsLocation: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: same resourceTags cascade-cleanup fix as DeleteAccessGrant."}
  DeleteAccessGrantsInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: cascade-cleans accessGrantsInstancePolicies and resourceTags (previously left behind forever). Deliberately does NOT cascade-delete AccessGrants/AccessGrantsLocations -- the real op's doc comment requires the caller delete those first; see items_still_open for the un-enforced precondition."}
  DeleteMultiRegionAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "LEAK FOUND AND FIXED THIS PASS -- see leaks below. Also fixed: wrong error code on missing MRAP (generic ErrNotFound == \"NoSuchPublicAccessBlockConfiguration\") -- now errMRAPNotFound (\"NoSuchMultiRegionAccessPoint\"), matching every other MRAP op in this file."}
  GetMultiRegionAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: fixed wrong error code on missing MRAP (see DeleteMultiRegionAccessPoint note)."}
  SetMRAPRegions: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: fixed wrong error code on missing MRAP (see DeleteMultiRegionAccessPoint note)."}
  DeleteStorageLensGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: ghost-map-row leak fix -- delete left generic resourceTags behind forever; now cascade-cleaned via the group's StorageLensGroupArn."}
  DeleteAccessPointForObjectLambda: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: ghost-map-row leak fix -- delete only removed the OLAP row itself, leaving its policy, configuration, and generic resource tags behind forever. Now cascade-cleans all three."}
  DeleteBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "THIS PASS: ghost-map-row leak fix -- delete only removed the bucket row itself, leaving lifecycle/policy/tagging/versioning/replication/generic-resource-tags behind forever. Now cascade-cleans all five."}

families:
  access-point-crud: {status: ok, note: "CRUD + policy + scope + PAB all backed by real store.Table state, XML wire shapes spot-checked against deserializers.go (GetAccessPointResult root, member names). THIS PASS: the 3 fabricated GetAccessPointPublicAccessBlock/PutAccessPointPublicAccessBlock/DeleteAccessPointPublicAccessBlock ops (confirmed absent from aws-sdk-go-v2/service/s3control -- PublicAccessBlockConfiguration is account-level-only as a standalone op; the per-AP variant travels inline on CreateAccessPoint/GetAccessPoint) were DELETED: removed from GetSupportedOperations(), removed the '/publicAccessBlock' route and 3 handler funcs (see ops above). The real underlying feature was NOT deleted -- GetAccessPoint's response now correctly includes inline PublicAccessBlockConfiguration when set (previously a genuine gap: CreateAccessPoint stored it but GetAccessPoint never echoed it back). Also fixed THIS PASS: DeleteAccessPoint's ghost-map-row leak (scope/PAB/tags survived delete) and 7 instances of the wrong AWS error code (NoSuchPublicAccessBlockConfiguration instead of NoSuchAccessPoint) across Get/Delete/Put AccessPoint*, plus split GetAccessPointPolicy's \"AP missing\" vs \"policy not set\" cases into distinct correctly-coded errors."}
  bucket-outposts: {status: ok, note: "CRUD + lifecycle + policy + replication + tagging + versioning; lifecycle route bug fixed (see ops above), rest spot-checked ok. THIS PASS: DeleteBucket ghost-map-row leak fixed (see ops above)."}
  job-batch-ops: {status: ok, note: "CreateJob/DescribeJob/ListJobs/tagging real; UpdateJobPriority/UpdateJobStatus route method bug fixed (see ops above). THIS PASS: fixed 4 instances of the wrong AWS error code (NoSuchPublicAccessBlockConfiguration instead of NoSuchJob) across Get/UpdateJobDetails/UpdateJobPriority/UpdateJobStatus."}
  storage-lens: {status: ok, note: "config + group + tagging CRUD backed by real maps (storageLensConfigs, storageLensConfigTags); routes verified against real SDK paths, no mismatches found. THIS PASS: DeleteStorageLensGroup ghost-map-row leak fixed (generic resourceTags survived delete; storageLensConfigTags for config-tagging was already correctly cascade-cleaned by DeleteStorageLensConfiguration, unaffected)."}
  multi-region-access-point: {status: ok, note: "async Create/Delete/PutPolicy + Describe + instance CRUD; PutMultiRegionAccessPointPolicy path and GetMultiRegionAccessPointPolicyStatus suffix bugs fixed (see ops above). LEAK FOUND AND FIXED THIS PASS: see leaks below. Also removed the dead/unused mrapPolicies map (declared, reset, but never once written to -- MRAP policy always lived on the MultiRegionAccessPoint.Policy struct field instead; this was pure dead state, not a live bug, but is gone now). Note: gopherstack still also exposes a synchronous DELETE on /mrap/instances/{Name} mapped to the same 'DeleteMultiRegionAccessPoint' op name -- the real API only has the async POST /async-requests/mrap/delete variant. Both routes now correctly delete the resource (the leak fix applies to the shared backend method regardless of which route drives it), but the sync-DELETE route itself remains dead surface from a real client's perspective; left as-is, see gaps."}
  access-grants: {status: ok, note: "instance + grant + location + identity-center + data-access CRUD; ListAccessGrants/ListAccessGrantsLocations singular-vs-plural route bugs and caller/grants hyphen bug fixed (see ops above). THIS PASS: ghost-map-row leaks fixed on DeleteAccessGrant, DeleteAccessGrantsLocation, and DeleteAccessGrantsInstance (generic resourceTags, and for the instance also accessGrantsInstancePolicies, all previously survived delete forever). DeleteAccessGrantsInstance deliberately does NOT cascade-delete grants/locations -- see items_still_open for the unenforced real-API precondition (instance delete should require grants/locations be deleted first)."}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource backed by real resourceTags map, prefix-matched route ok. THIS PASS: every resource-delete path that has a generic ARN (AccessPoint, ObjectLambda AP, Outposts Bucket, AccessGrant, AccessGrantsLocation, AccessGrantsInstance, StorageLensGroup) now cascade-cleans resourceTags[arn] on delete -- previously only AccessPoint's OWN policy map was cleaned by DeleteAccessPoint and nothing else cleaned tags anywhere, so a delete/recreate cycle under the same name/ARN could silently resurrect a prior resource's tags."}
  error-wire-shape: {status: ok, note: "SERVICE-WIDE bug: every error response (handleBackendError + ~30 ad-hoc 'invalid request body'/'not found' sites) returned c.String(status, plainText) instead of the AWS REST-XML <Error><Code>/<Message> envelope. Fixed prior pass via pkgs/awserr.Write. THIS PASS found a SECOND, narrower service-wide bug of the same class: 15 call sites across access_points.go (7), multi_region_access_points.go (4), and jobs.go (4) used the generic `ErrNotFound` sentinel (code \"NoSuchPublicAccessBlockConfiguration\") for AccessPoint-not-found / MRAP-not-found / Job-not-found errors instead of the resource-specific sentinel (errAccessPointNotFound/\"NoSuchAccessPoint\", errMRAPNotFound/\"NoSuchMultiRegionAccessPoint\", errJobNotFound/\"NoSuchJob\"). HTTP status (404) was correct in every case -- only the XML <Code> body was wrong -- so status-code-only tests never caught it; a real SDK client doing typed error matching (err.Code(), errors.As against a specific exception) on any of these paths got the wrong exception class. All 15 fixed; also added a new errAccessPointPolicyNotFound (\"NoSuchAccessPointPolicy\") sentinel to distinguish \"AP doesn't exist\" from \"AP exists but has no policy\" in GetAccessPointPolicy, which the prior pass had conflated under NoSuchAccessPoint."}
  persistence-gap: {status: ok, note: "NEW FAMILY THIS PASS -- found via reading persistence.go against store.go's field list. backendSnapshot only ever round-tripped the 'batch2' raw maps (bucketReplication, storageLensConfigs, storageLensConfigTags, resourceTags, accessPointPolicies) plus the store.Table-backed resources; the 10 'batch1' raw maps (accessPointScopes, objectLambdaAPPolicies, objectLambdaAPConfigs, bucketPolicies, bucketTagging, bucketLifecycle, bucketVersioning, mrapRoutes, accessGrantsInstancePolicies, jobTags) were declared on InMemoryBackend and actively read/written by real handlers, but Snapshot() never serialized them and Restore() never restored them -- a Snapshot/Restore cycle (a service restart with persistence enabled) silently dropped access point scopes, Object Lambda AP policies/configs, Outposts bucket policy/tagging/lifecycle/versioning, MRAP routes, Access Grants instance resource policies, and job tags, even though the owning resource itself (e.g. the access point, the bucket) survived intact. Fixed: all 10 fields added to backendSnapshot, wired into Snapshot/Restore (including the version-mismatch discard-and-reset branch), s3controlSnapshotVersion bumped 1 -> 2. New test TestPersistence_Batch1Maps_SnapshotRestore locks in all 10."}

gaps:
  - The synchronous "DELETE /v20180820/mrap/instances/{Name}" route mapped to DeleteMultiRegionAccessPoint does not exist in the real API (only the async POST variant does). Dead code from a real client's perspective; low-risk cleanup deferred (unlike the fabricated PublicAccessBlock ops, DeleteMultiRegionAccessPoint IS a real op name -- only this one extra HTTP-verb/path combination for it is fake -- so this was judged lower-priority than deleting an entirely invented operation family).
  - s3control.ErrAlreadyExists (errors.go) wraps a generic "BucketAlreadyExists" code but is never actually returned by any backend method (verified via repo-wide grep) -- unused/dead sentinel, not a live bug, but worth removing or wiring up correctly if AlreadyExists semantics are ever needed for e.g. CreateAccessPoint on a duplicate name.
  - DeleteAccessGrantsInstance does not enforce the real API's documented precondition ("You must first delete the access grants and locations before S3 Access Grants can delete the instance") -- gopherstack allows deleting an instance that still has grants/locations attached, which a real AWS account would reject. Not fixed this pass: the correct AWS error code for this specific conflict is not present anywhere in aws-sdk-go-v2/service/s3control's typed exceptions (S3 Control largely returns untyped/generic errors), so guessing a code risked introducing an unverified wire-shape bug rather than fixing one. See items_still_open.
  - Only a modestly larger sample of response XML shapes were spot-checked against deserializers.go this pass (GetAccessPoint -- including the newly-added inline PublicAccessBlockConfiguration --, CreateAccessGrant, DescribeJob) on top of the prior pass's sample (GetAccessPoint, CreateJob, CreateMultiRegionAccessPoint, GetBucketPolicy/Tagging/Versioning). The remaining response types were not individually diffed field-by-field against the SDK deserializers -- see deferred.

deferred:
  - Full field-by-field wire-shape diff of every response XML struct against deserializers.go (this pass prioritized the leak, the two error-code bug classes, the ghost-map-row cascade-delete class, and the persistence-gap class, all of which had wide blast radius across many ops; response-body field audits remain sampled, not exhaustive).
  - AccessGrantsInstance / IdentityCenter association flows (state machine correctness beyond basic CRUD), including the un-enforced delete-grants-and-locations-first precondition noted under gaps.
  - Chaos fault-injection interaction with the fixed routes/leak (ChaosOperations() just echoes GetSupportedOperations(), unaffected by this pass).

leaks: {status: fixed, note: "LEAK FOUND AND FIXED THIS PASS. DeleteMultiRegionAccessPoint (multi_region_access_points.go) checked b.mraps.Has(key) and returned nil WITHOUT ever calling b.mraps.Delete(key) -- a disguised no-op. Both the synchronous DELETE /v20180820/mrap/instances/{Name} route and the async POST /v20180820/async-requests/mrap/delete route (the one a real aws-sdk-go-v2 client actually uses) call this same backend method, so every DeleteMultiRegionAccessPoint call, sync or async, silently failed to remove the resource: the MRAP stayed retrievable via GetMultiRegionAccessPoint/ListMultiRegionAccessPoints forever, and repeated create/delete cycles (e.g. any test or workload that creates+deletes MRAPs by generated/random names) accumulated an unbounded number of ghost rows in b.mraps with no way to reclaim them. No existing test caught this because the only assertion on delete was err == nil, never that the resource was actually gone -- classic 'green tests, real bug' (see the project's parity-principles.md point 3). Fixed: DeleteMultiRegionAccessPoint now actually deletes the row and cascade-cleans its route configuration (mrapRoutes); new tests TestBackend_DeleteMultiRegionAccessPoint_ActuallyRemoves and TestHandler_DeleteMultiRegionAccessPoint_AsyncRouteActuallyRemoves lock in both the backend- and HTTP-level behavior via Get-after-Delete and List-after-Delete assertions, not just the return value. While investigating this leak class, also found and fixed 6 more ghost-map-row leaks of the identical shape (delete removes the primary resource row but leaves secondary maps -- policy/scope/PAB/generic-tags -- behind forever) on DeleteAccessPoint, DeleteAccessPointForObjectLambda, DeleteBucket, DeleteAccessGrant, DeleteAccessGrantsLocation, DeleteAccessGrantsInstance, and DeleteStorageLensGroup -- see the tags/access-point-crud/bucket-outposts/access-grants/storage-lens family notes above. No goroutines/janitors/tickers exist in this service (verified: no `go func`/`time.NewTicker`/`time.AfterFunc`/`context.WithCancel` anywhere in services/s3control), so there is no goroutine-leak class here -- the leak this pass found and fixed was purely the disguised-no-op-delete / ghost-map-row class. Handler.Snapshot/Restore correctly delegate to InMemoryBackend.Snapshot/Restore (verified in persistence.go) so cli.go's setupPersistence registers it correctly -- no silent-unregistration bug found here."}
---

## Notes

**Protocol**: REST-XML (`/v20180820/` path-versioned), with `X-Amz-Account-Id` header
carrying the account ID (there is no path/query account parameter). Error bodies use
a bare `<Error><Code>/<Message>/<RequestId></Error>` envelope (not wrapped in an outer
`<ErrorResponse>` the way the Query protocol is) -- see `pkgs/awserr.ProtocolRestXML`.

**Route-matcher bug class (prior pass)**: `RouteMatcher()` itself just
checks `strings.HasPrefix(path, "/v20180820/")` -- real operation routing happens in
`ExtractOperation`/`Handler()`'s `extract*`/`dispatch*` helper functions, which do
literal path-prefix/suffix and HTTP-method matching. Nine ops were **unreachable by a
real aws-sdk-go-v2 client** despite having fully-implemented, real handler+backend
logic, because the literal path/method constants didn't match the real SDK's
`serializers.go` -- singular-vs-plural, hyphen-vs-underscore, casing, wrong verb, extra
path segment, wrong suffix. See git history for the full list; all fixed prior pass.

**Disguised-no-op-delete / ghost-map-row leak class (THIS pass)**: the leak this pass
was asked to find (`DeleteMultiRegionAccessPoint` checked existence and returned nil
without ever calling `.Delete()`) is one instance of a broader pattern found by reading
every `Delete*` backend method against every map it should have touched: a delete that
only removes the primary `store.Table` row, silently leaving secondary maps (per-resource
policy/scope/config, generic `resourceTags[arn]`) populated forever. Two distinct
symptoms: (1) the resource itself never actually disappears (MRAP case -- the primary
row survives too), and (2) the resource disappears but a delete/recreate cycle under the
same name/ARN silently resurrects the deleted resource's stale secondary state (the other
6 cases). Both are real memory-growth-over-time bugs in a long-running emulator process
and both are now fixed with matching regression tests (Get/List-after-Delete assertions,
or delete-then-recreate-then-assert-empty assertions) rather than trusting `err == nil`.

**Wrong-error-code class (THIS pass, second instance of the error-wire-shape bug
family)**: 15 call sites returned the generic `ErrNotFound` sentinel (AWS code
`NoSuchPublicAccessBlockConfiguration`) for AccessPoint/MRAP/Job-not-found instead of
the resource-specific sentinel. HTTP status was always correct (404), so this was
invisible to any test asserting status codes only -- exactly the failure mode
`parity-principles.md` warns about ("green tests, real bug"). All fixed; two new
sentinels added (`errAccessPointPolicyNotFound`) to correctly distinguish "resource
missing" from "resource exists but sub-field not set" where the prior code conflated
them under one AWS error code.

**Fabricated-ops deletion (THIS pass)**: `GetAccessPointPublicAccessBlock` /
`PutAccessPointPublicAccessBlock` / `DeleteAccessPointPublicAccessBlock` were gopherstack-
invented standalone REST operations with no counterpart in
`aws-sdk-go-v2/service/s3control` (confirmed: no `api_op_*AccessPointPublicAccessBlock*.go`
files exist in the SDK module). Deleted per parity policy. Deleting them surfaced a real,
previously-hidden gap underneath: the actual AWS feature (`PublicAccessBlockConfiguration`
travels inline on `CreateAccessPointInput`/`GetAccessPointOutput`, confirmed via the SDK's
own generated types) was half-implemented -- `CreateAccessPoint` stored it but
`GetAccessPoint` never echoed it back. Both are fixed now: the fabricated ops are gone,
the real inline field works.

**Persistence-gap class (THIS pass, new)**: found by systematically checking every raw
(non-`store.Table`) map field declared on `InMemoryBackend` (`store.go`) against
`backendSnapshot`'s field list (`persistence.go`). 10 of them were write-through live
state with no persistence wiring at all -- a silent data-loss bug on any
Snapshot/Restore cycle (service restart with persistence enabled). All 10 fixed; see
the `persistence-gap` family note above for the full list and
`TestPersistence_Batch1Maps_SnapshotRestore` for the regression test.

**Error XML**: `pkgs/awserr.Write(c, awserr.ProtocolRestXML, awserr.APIError{...})`
existed in the shared pkgs/ layer but was unused by ANY service in the codebase before
the prior pass (verified via repo-wide grep). s3control's backend errors are already
created via `awserr.New(code, sentinel)` (e.g. `errAccessPointNotFound =
awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)`), so `err.Error()` IS the AWS
error code string -- `handleBackendError` does `code := err.Error()` and passes it
straight through to `awserr.Write`. This is exactly the mechanism that made the
wrong-error-code bug class (this pass) possible: using the wrong *sentinel* (right
HTTP status, wrong `err.Error()` string) silently produces a wrong-but-well-formed XML
`<Code>`, which only a code-string assertion (not a status-code assertion) will catch.
If re-auditing other REST-XML services, check both status code AND `<Code>` string.

## items_still_open (see gaps/deferred above for full detail)

- Full field-by-field response-XML diff against deserializers.go for the ~55 remaining
  response types not spot-checked this pass or the prior pass. Reason not finished:
  each diff requires reading the generated SDK deserializer source per operation:
  this pass prioritized the leak (explicitly flagged), the two service-wide
  wrong-error-code bug classes, the 7-family ghost-map-row cascade-delete class, and
  the 10-field persistence-gap class, all of which had broader blast radius than any
  single response-shape field diff. Un-diffed, not reclassified to "ok".
- DeleteAccessGrantsInstance does not enforce "grants/locations must be deleted
  first" (real AWS behavior per the op's own doc comment). Reason not fixed: the
  correct AWS error code for this conflict is not present in
  aws-sdk-go-v2/service/s3control's typed exceptions (S3 Control returns mostly
  untyped/generic errors for validation failures), so implementing the check without
  a confirmed wire-accurate error code risked trading a missing-validation gap for a
  wrong-error-code bug of the exact class this pass spent most of its effort fixing
  elsewhere.
- ErrAlreadyExists (errors.go) remains an unused/dead sentinel. Reason not fixed: no
  backend method needs AlreadyExists semantics currently (e.g. CreateAccessPoint does
  not reject duplicate names), and confirming whether real AWS actually returns
  AlreadyExists for any s3control Create* op -- versus silently overwriting, versus a
  different validation error -- was out of scope for this pass's leak/error-code/
  persistence focus.
- The synchronous DELETE /v20180820/mrap/instances/{Name} route (mapped to the real
  op name DeleteMultiRegionAccessPoint, but via an HTTP verb/path combination the real
  SDK never sends) remains routable. Reason not removed: unlike the 3 fabricated
  PublicAccessBlock ops (which had no real op-name counterpart at all), this route
  reuses a genuine op name and was already fixed by this pass's leak fix (it no longer
  behaves like a no-op); removing the route itself is pure dead-surface cleanup with
  no remaining functional bug, judged lower priority than the items above.

## Wire-shape field-diff audit (gopherstack-tir4, THIS pass)

Full field-by-field diff of response/request XML shapes against the installed
`aws-sdk-go-v2/service/s3control@v1.73.0`'s `deserializers.go`/`serializers.go`/`types/types.go`
(the SDK module was updated from v1.68.2 -> v1.73.0 as part of confirming shapes against the
currently-vendored version). Grouped by handler file. `types_not_reached` at the end lists
what a follow-up pass should still cover.

### Access Grants (`handler_access_grants.go`) -- all ops diffed

- `CreateAccessGrantsInstanceResult`: FIXED -- missing `CreatedAt` (backing data existed, never
  wired).
- `GetAccessGrantsInstanceResult`: CLEAN.
- `ListAccessGrantsInstancesResult`: FIXED -- per-item `CreatedAt` field existed in the struct
  but was never populated in the loop; `IdentityCenterInstanceArn`/`IdentityCenterApplicationArn`
  were missing entirely despite backing data.
- `AssociateAccessGrantsIdentityCenterResult` / `DissociateAccessGrantsIdentityCenter`:
  CLEAN (real ops have no output body; verified no `awsRestxml_deserializeOpDocument...Output`
  function exists for the first).
- `GetAccessGrantsInstanceForPrefixResult`: FIXED -- missing `AccessGrantsInstanceId` (real
  field, backing data existed).
- `GetAccessGrantsInstanceResourcePolicyResult` / `PutAccessGrantsInstanceResourcePolicyResult`:
  GAP-DOCUMENTED -- real type also has `CreatedAt`/`Organization`; this backend's resource-policy
  store is a bare string map with no such data.
- `GetAccessGrantResult`: FIXED -- missing `AccessGrantsLocationId`, `GrantScope`,
  `ApplicationArn`, `CreatedAt` (all backed by data on every stored grant, none were wired).
- `ListAccessGrantsResult`: FIXED -- item type only carried `AccessGrantId`/`Permission`/
  `GrantScope`; added `AccessGrantArn`/`AccessGrantsLocationId`/`ApplicationArn`/`CreatedAt`/
  `Grantee` (all backed).
- `ListCallerAccessGrantsResult`: **ENVELOPE BUG, FIXED** -- wrapped the list under
  `AccessGrantsList` (the key `ListAccessGrants`, a *different* operation, uses); the real
  deserializer (`awsRestxml_deserializeOpDocumentListCallerAccessGrantsOutput`) only recognizes
  `CallerAccessGrantsList`. A real client would see an empty list on every call. Also: the item
  type was reused from `ListAccessGrants` and fabricated an `AccessGrantId` element --
  `ListCallerAccessGrantsEntry` has no such field in the real SDK at all (verified against
  `types.ListCallerAccessGrantsEntry`); it also lacked `ApplicationArn`/`GrantScope`, which
  are real and backed. New dedicated `listCallerAccessGrantItemXML` type added.
- `GetAccessGrantsLocationResult` / `CreateAccessGrantsLocationResult` /
  `UpdateAccessGrantsLocationResult`: CLEAN.
- `ListAccessGrantsLocationsResult`: FIXED -- item type only carried `AccessGrantsLocationId`/
  `LocationScope`; added `AccessGrantsLocationArn`/`CreatedAt`/`IAMRoleArn` (all backed).
- `GetDataAccessResult`: GAP-DOCUMENTED -- real type also has `Credentials.SessionToken`,
  `Credentials.Expiration`, and a top-level `Grantee`; this backend does not issue real
  STS-style credentials or resolve which grant matched, so these are left unpopulated rather
  than invented.
- `DeleteAccessGrantsInstance` precondition: **FIXED** -- see dedicated section below.

### Jobs (`handler_jobs.go`) -- all ops diffed

- `CreateJobResult`, `DescribeJobResult`: CLEAN (both already spot-checked in the prior pass;
  re-verified `JobDescriptor`'s full field list this pass -- `GeneratedManifestDescriptor`/
  `ManifestGenerator`/`FailureReasons`/`SuspendedCause`/`SuspendedDate` have no backing data,
  GAP not fabricated, not individually annotated in code this pass -- see types_not_reached).
- `ListJobsResult`: FIXED -- item type (`JobListDescriptor`) only carried `JobId`/`Status`/
  `Priority`; added `Description`/`CreationTime`/`TerminationDate` (backed directly) and
  `Operation` (derived from the raw `<Operation>` blob's root element name via a new
  `jobOperationName` helper, since `JobListDescriptor.Operation` is a plain `OperationName`
  enum string like `"LambdaInvoke"`, not the full nested config `JobDescriptor.Operation`
  carries -- confirmed via `types.JobListDescriptor`). `ProgressSummary`: GAP-DOCUMENTED (no
  task-count tracking in this backend).
- `UpdateJobPriorityResult`: CLEAN.
- `UpdateJobStatusResult`: FIXED -- missing `StatusUpdateReason` (real field, backed).
- `GetJobTaggingResult` / `PutJobTaggingRequest`: **ENVELOPE BUG, FIXED** -- `jobTagSetXML`
  used member name `<Tag>`; the real `S3TagSet` type (shared with bucket tagging) serializes
  entries as `<member>` (confirmed via `awsRestxml_serializeDocumentS3TagSet`). This broke
  both directions: `GetJobTagging` would show an empty tag set to a real client, and
  `PutJobTagging` would silently drop every tag a real client sends (its request always uses
  `<member>`, never `<Tag>`).

### Object Lambda (`handler_object_lambda.go`) -- all ops diffed

- `CreateAccessPointForObjectLambdaResult`: GAP-DOCUMENTED -- real type also has `Alias`; no
  backing data (`ObjectLambdaAccessPoint` in models.go tracks no alias).
- `GetAccessPointForObjectLambdaResult`: **FABRICATION, FIXED** -- emitted an
  `ObjectLambdaAccessPointArn` element; the real `GetAccessPointForObjectLambdaOutput` has NO
  such field at all (confirmed against the SDK type and its deserializer -- only
  `Alias`/`CreationDate`/`Name`/`PublicAccessBlockConfiguration` exist). Deleted; the three real
  fields have no backing data in this backend (GAP-DOCUMENTED).
- `ListAccessPointsForObjectLambdaResult`: CLEAN (`Alias` gap noted, no backing data).
- `GetAccessPointPolicyForObjectLambdaResult` / `Put...` / `Delete...`: CLEAN.
- `GetAccessPointPolicyStatusForObjectLambdaResult`: CLEAN (same `PolicyStatus>IsPublic`
  pattern as the AccessPoint/MRAP variants, verified independently).
- `GetAccessPointConfigurationForObjectLambdaResult` / `PutAccessPointConfigurationForObjectLambdaRequest`:
  **ENVELOPE BUG, FIXED** -- wrapped the payload under `<ObjectLambdaConfiguration>`; the real
  wrapper element is `<Configuration>` (confirmed via
  `awsRestxml_deserializeOpDocumentGetAccessPointConfigurationForObjectLambdaOutput` and
  `awsRestxml_serializeOpDocumentPutAccessPointConfigurationForObjectLambdaInput`). Also fixed a
  second bug in the same field: it was captured/emitted as a flat `string`, which would
  concatenate/lose a real client's nested `TransformationConfigurations`/`AllowedFeatures`
  structure; now captured as raw inner XML (same pattern as `CreateJob`'s Manifest/Operation/Report).

### Storage Lens (`handler_storage_lens.go`) -- all ops diffed

- `CreateStorageLensGroupResult` / `UpdateStorageLensGroupResult`: CLEAN (verified these real
  ops have NO output body at all -- no `awsRestxml_deserializeOpDocument...Output` function
  exists for either).
- `GetStorageLensConfigurationResult` / `PutStorageLensConfigurationRequest`:
  **ENVELOPE + REQUEST-BREAKING BUG, FIXED** -- expected/emitted a `<Config>` child element that
  does not exist anywhere on the real `StorageLensConfiguration` type. On the request side this
  was a hard break: a real client's PUT body nests the whole configuration directly under
  `<StorageLensConfiguration>` (a "payload"-bound field, confirmed via
  `awsRestxml_serializeOpDocumentPutStorageLensConfigurationInput`), so the `<Config>` field
  never matched anything a real client sends -- every real `PutStorageLensConfiguration` call
  silently stored an empty configuration. Fixed: the real per-field structure
  (`AccountLevel`/`IsEnabled`/etc.) is now captured/replayed as raw inner XML directly under
  `<StorageLensConfiguration>`, alongside the real, always-known `Id`.
- `GetStorageLensConfigurationTaggingResult` / `PutStorageLensConfigurationTaggingRequest`:
  CLEAN -- `StorageLensTags`' member name really is `<Tag>` (a *different* type from the
  `S3TagSet`/`<member>` type job and bucket tagging use), verified independently; no bug here.
- `ListStorageLensConfigurationsResult`: **ENVELOPE BUG, FIXED** -- wrapped the list under
  `<StorageLensConfigurationList>`; the real list is FLATTENED (repeated
  `<StorageLensConfiguration>` elements directly under the result, confirmed via
  `awsRestxml_deserializeDocumentStorageLensConfigurationListUnwrapped` -- the "Unwrapped"
  suffix is smithy-go's flattened-list marker). A real client would see an empty list on every
  call.
- `GetStorageLensGroupResult`: **FABRICATION, FIXED** -- emitted a `CreatedAt` element; the
  real `StorageLensGroup` type has NO such field at all (confirmed via
  `awsRestxml_deserializeDocumentStorageLensGroup` -- only `Filter`/`Name`/`StorageLensGroupArn`
  exist). Deleted (the internal `StorageLensGroup.CreatedAt` field remains tracked, just not
  serialized).
- `ListStorageLensGroupsResult`: **ENVELOPE BUG + FABRICATION, FIXED** -- same flattened-list
  bug as `ListStorageLensConfigurations` (wrapped under `<StorageLensGroupList>` instead of
  flattening), plus the same fabricated `CreatedAt`, plus items reused the `Get` shape's
  `Filter` field, which the real, narrower `ListStorageLensGroupEntry` type does not have
  (it has `HomeRegion` instead -- GAP-DOCUMENTED, no backing data). New dedicated
  `listStorageLensGroupItemXML` type added.

### Tags (`handler_tags.go`) -- all ops diffed

- `ListTagsForResourceResult` / `TagResourceRequest`: CLEAN -- the generic resource `TagList`
  type's member name really is `<Tag>` (confirmed via `awsRestxml_serializeDocumentTagList`'s
  `ArrayWithCustomName(... Local: "Tag")`), a different type from `S3TagSet`; no bug.
- `UntagResourceRequest`: **TRANSPORT BUG, FIXED** -- expected `TagKeys` in an XML request body
  (`<UntagResourceRequest><TagKeys><TagKey>...`). The real `UntagResourceInput` has NO XML body
  at all for this op -- `TagKeys` travels as repeated `tagKeys` query-string parameters
  (confirmed via `awsRestxml_serializeOpHttpBindingsUntagResourceInput`, which calls
  `encoder.AddQuery("tagKeys", ...)` and has no corresponding body serializer). Since
  `decodeXML` treats an empty body (`io.EOF`) as success, every real `UntagResource` call from
  an actual aws-sdk-go-v2 client silently deleted **zero** tags while still returning 204 --
  a disguised no-op of the same severity class as the MRAP delete leak from the prior pass.
  Fixed: `tagKeys` now read from the query string.

### Access Points (`handler_access_points.go`) -- all ops diffed

- `CreateAccessPointResult`, `GetAccessPointPolicyResult`/`Put`/`Delete`,
  `GetAccessPointPolicyStatusResult`, `PutAccessPointScopeResult`: CLEAN.
- `GetAccessPointResult`: GAP-DOCUMENTED (not fixed, no backing data) -- real type also has
  `DataSourceId`/`DataSourceType`/`Endpoints`; doc comment added.
- `ListAccessPointsResult`: CLEAN (verified against `types.AccessPoint`'s full field list;
  same `DataSourceId`/`DataSourceType` gap, no backing data).
- `GetAccessPointScopeResult` / `PutAccessPointScopeRequest`: **WRONG-SHAPE BUG, FIXED** --
  `Scope` was treated as a flat string; the real `GetAccessPointScopeOutput.Scope` /
  `PutAccessPointScopeInput.Scope` is a structured type (`Permissions []ScopePermission`,
  `Prefixes []string`, confirmed via `awsRestxml_deserializeDocumentScope`). A real client's
  nested `<Scope><Permissions>...</Permissions><Prefixes>...</Prefixes></Scope>` body would
  have been collapsed to (mostly empty) character data on decode. Fixed: captured/replayed as
  raw inner XML nested under `<Scope>`, same pattern as the ObjectLambda Configuration fix.
- `ListAccessPointsForDirectoryBucketsResult`: FIXED -- confirmed this op shares the exact same
  entry type and envelope as `ListAccessPoints` (`awsRestxml_deserializeOpDocumentListAccessPointsForDirectoryBucketsOutput`
  delegates to the identical `AccessPointList` deserializer); the handler previously used a
  narrower ad hoc type (`Name`/`AccessPointArn`/`Bucket` only), omitting
  `BucketAccountId`/`NetworkOrigin`/`Alias`/`VpcConfiguration` despite this backend tracking all
  of them. Now reuses `listAccessPointItemXML`.

### Bucket (Outposts) (`handler_bucket.go`) -- all ops diffed

- `CreateBucketResult`, `GetBucketPolicyResult`/`Put`/`Delete`,
  `GetBucketReplicationResult`/`Put`/`Delete` (already used correct innerxml capture under the
  correct `ReplicationConfiguration` root, verified against the real payload-bound field):
  CLEAN.
- `GetBucketResult`: **FABRICATION, FIXED** -- emitted `BucketArn` (does not exist on the real
  `GetBucketOutput` at all -- only `Bucket`/`CreationDate`/`PublicAccessBlockEnabled` do,
  confirmed against the SDK type) and mislabeled the internal HTTP `Location`-header path
  fragment as `OutpostId` (also not a real field on this output). Both deleted;
  `CreationDate`/`PublicAccessBlockEnabled` GAP-DOCUMENTED (no backing data on
  `OutpostsBucket`).
- `GetBucketTaggingResult` / `PutBucketTaggingRequest`: **ENVELOPE + REQUEST-BREAKING BUG,
  FIXED** -- same `S3TagSet` member-name bug as job tagging (`<Tag>` instead of `<member>`),
  compounded by a payload-root bug: `Tagging` is a "payload"-bound field in the real SDK, so
  the ENTIRE request body root is `<Tagging>` with no `<PutBucketTaggingRequest>` wrapper at
  all (confirmed via the serializer, which sets the XML root element to `"Tagging"` directly).
  The previous shape expected the payload nested one level deeper, which a real
  aws-sdk-go-v2 client's request would never match (root-element mismatch) -- **every real
  PutBucketTagging call would have been rejected outright**, not merely mis-parsed.
- `GetBucketVersioningResult`: GAP-DOCUMENTED (`MfaDelete`, no backing data).
- `PutBucketVersioningRequest`: **REQUEST-BREAKING BUG, FIXED** -- same payload-root class of
  bug as `PutBucketTagging`: `VersioningConfiguration` is payload-bound, so the real root is
  `<VersioningConfiguration>` with `Status` as a direct child, not
  `<PutBucketVersioningRequest><VersioningConfiguration><Status>`. Every real
  `PutBucketVersioning` call would have been rejected outright.
- `ListRegionalBucketsResult`: GAP-DOCUMENTED -- item type (`RegionalBucket`) also has
  `CreationDate`/`OutpostId`/`PublicAccessBlockEnabled`; no backing data for any of the three.

### Multi-Region Access Points (`handler_multi_region_access_points.go`) -- all ops diffed

- `CreateMultiRegionAccessPointResult`, `DeleteMultiRegionAccessPointResult` (async),
  `PutMultiRegionAccessPointPolicyResult`: CLEAN (all just `RequestTokenARN`, verified against
  their deserializers).
- `GetMultiRegionAccessPointResult`: GAP-DOCUMENTED -- shares the `MultiRegionAccessPointReport`
  type with the List response (see below); `PublicAccessBlock` has no backing data at the MRAP
  level, and per-region `Region`/`BucketAccountId` (real `RegionReport` fields) have no backing
  data either (this backend tracks only bucket names per region).
- `ListMultiRegionAccessPointsResult`: **ENVELOPE BUG, FIXED** -- list member name was `item`;
  the real member name is `AccessPoint` (confirmed via
  `awsRestxml_deserializeDocumentMultiRegionAccessPointReportList`) -- a real client would see
  an empty list on every call. Also: items were a narrower ad hoc type
  (`Name`/`Alias`/`Status` only) when the real entry type is the SAME
  `MultiRegionAccessPointReport` `GetMultiRegionAccessPoint` returns; added `CreatedAt`/
  `Regions` (both backed).
- `DescribeMultiRegionAccessPointOperationResult`: GAP-DOCUMENTED -- real `AsyncOperation` also
  carries `CreationTime`/`Operation`/`RequestParameters`/`ResponseDetails`; no audit-trail
  tracking in this backend. `RequestStatus` is hardcoded `"SUCCEEDED"`, which is not fabricated
  per se since every MRAP mutation here completes synchronously.
- `GetMultiRegionAccessPointPolicyResult`: CLEAN (`Policy>Established>Policy` nesting verified;
  `Proposed` GAP-DOCUMENTED, no backing data).
- `GetMultiRegionAccessPointPolicyStatusResult`: CLEAN.
- `GetMultiRegionAccessPointRoutesResult` / `SubmitMultiRegionAccessPointRoutesRequest`:
  **WRONG-SHAPE + REQUEST-BREAKING BUG, FIXED** -- `Routes` was a flat string on both sides,
  and the request field was literally misnamed: the real `SubmitMultiRegionAccessPointRoutesInput`
  field is `RouteUpdates`, not `Routes` (confirmed via
  `awsRestxml_serializeOpDocumentSubmitMultiRegionAccessPointRoutesInput`), wrapping a list of
  `<Route>` entries (`Bucket`/`Region`/`TrafficDialPercentage`). A real client's request would
  never populate the old `Routes` field at all -- every real `SubmitMultiRegionAccessPointRoutes`
  call silently stored an empty routing update. Fixed: renamed to `RouteUpdates`, captured as
  raw inner XML on both request and response sides to preserve the real per-route structure.

### `DeleteAccessGrantsInstance` precondition -- FIXED

Real API doc comment: "You must first delete the access grants and locations before S3 Access
Grants can delete the instance." Previously unenforced (gopherstack allowed deleting an
instance with grants/locations still attached). Fixed: `DeleteAccessGrantsInstance` now checks
for any `AccessGrant`/`AccessGrantsLocation` belonging to the account and rejects with
`errAccessGrantsInstanceNotEmpty` (aliased to the existing `ErrValidation` / `BadRequestException`
sentinel) if either exists. No S3 Control typed exception is specific to this conflict
(verified against the SDK's full `types/errors.go` exception list: `BadRequestException`,
`BucketAlreadyExists`, `BucketAlreadyOwnedByYou`, `IdempotencyException`,
`InternalServiceException`, `InvalidNextTokenException`, `InvalidRequestException`,
`JobStatusException`, `NoSuchPublicAccessBlockConfiguration`, `NotFoundException`,
`TooManyRequestsException`, `TooManyTagsException` -- none named for this case), so this reuses
the generic `BadRequestException` sentinel this codebase already uses for other S3 Access
Grants validation failures, rather than inventing an unverified specific code.

### types_not_reached (honest remainder)

Not diffed field-by-field this pass (no changes made, status unknown -- do not assume clean):

- `DescribeJob`'s nested sub-structures beyond the top-level fields already verified
  (`GeneratedManifestDescriptor`, `ManifestGenerator`, `FailureReasons`, `SuspendedCause`,
  `SuspendedDate` -- likely GAP-not-fabricated given this backend's job model, but not
  individually confirmed against the deserializer this pass).
- `CreateAccessPoint`'s request-side `VpcConfiguration`/`PublicAccessBlockConfiguration`
  nesting (spot-checked in a prior pass, not re-verified this pass).
- Account-level `GetPublicAccessBlock`/`PutPublicAccessBlock`/`DeletePublicAccessBlock` --
  wrapper element name (`PublicAccessBlockConfiguration`) confirmed correct this pass, but
  field-by-field (`BlockPublicAcls`/etc.) not re-verified since a prior pass already covered
  this shape.
- Bucket lifecycle (`GetBucketLifecycleConfiguration`/`Put`/`Delete`) and bucket policy
  (`GetBucketPolicy`/`Put`/`Delete`) beyond the route-level fix already recorded in the prior
  pass -- these use raw-body passthrough (no XML struct decoding), so they carry no
  root-element-mismatch risk, but the passthrough behavior itself (no validation, no partial
  merge semantics) was not otherwise audited this pass.
- Chaos fault-injection interaction with any of the fixes above (`ChaosOperations()` just
  echoes `GetSupportedOperations()`, unaffected).

Given the density of severe bugs found in the ~35 types that WERE diffed this pass (13 distinct
envelope/transport/fabrication bugs across 9 handler files, several of which would reject or
silently no-op every real SDK client call), the ~15-20 types listed above should be treated as
unverified, not presumptively clean, in any future audit of this service.
