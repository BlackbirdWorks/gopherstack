---
service: ecr
sdk_module: aws-sdk-go-v2/service/ecr@v1.59.0
last_audit_commit: fba3c784+uncommitted  # this pass's changes are uncommitted working-tree edits; see Notes
last_audit_date: 2026-07-23
overall: B+  # independently re-field-diffed every op against the real SDK deserializers this pass; found and fixed 4 genuine wire-shape bugs the prior "ok" audit missed (see "Genuine fixes made this pass, round 2" below)
ops:
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "force-with-images enforced in handler via DescribeImages pre-check"}
  PutImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — the 'image' response object was the raw internal Image domain struct, leaking 5 gopherstack-only fields (imageDigest, imagePushedAt, imageStatus, storageClass, imageSizeInBytes) not present on the real ecr.types.Image shape (imageId/imageManifest/imageManifestMediaType/registryId/repositoryName only, per awsAwsjson11_deserializeDocumentImage); imagePushedAt was also a bare time.Time (RFC3339 string) though moot since the field itself was invented. Fixed via a new imageView wire type; the digest remains available via the correct nested imageId.imageDigest. Also carries the round-1 ImageDigestDoesNotMatchException fix."}
  BatchGetImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — same invented-field leak as PutImage (Images []Image → []imageView); see PutImage note"}
  BatchDeleteImage: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImages: {wire: partial, errors: ok, state: ok, persist: ok, note: "core fields (imageDigest, imageTags, imagePushedAt as epoch, imageSizeInBytes, imageManifestMediaType, imageStatus, registryId, repositoryName) verified correct via imageDetailView. GAP (not fixed this pass, see gaps below): real ImageDetail additionally has artifactMediaType, imageScanFindingsSummary, imageScanStatus, lastActivatedAt, lastArchivedAt, lastRecordedPullTime, subjectManifestDigest — none implemented."}
  ListImages: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImageReferrers: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchCheckLayerAvailability: {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateLayerUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIFO TTL pruning bounds layerUploads/layerUploadQueue"}
  UploadLayerPart: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — added part-sequencing validation (InvalidLayerPartException) for non-consecutive partFirstByte"}
  CompleteLayerUpload: {wire: ok, errors: partial, state: ok, persist: ok, note: "FIXED — was missing RepositoryNotFoundException FK check (unlike every other op) and never rejected re-completing an already-registered layer digest (LayerAlreadyExistsException). RE-VERIFIED round 2, still gap (see gaps below): UploadNotFoundException (real error, confirmed present in types/errors.go) is never returned — a Complete call whose uploadId matches no live session (or an empty/zero-byte session) is silently accepted via a 'direct digest' fallback path. This is a deliberate, long-standing test-seeding convenience used by ~9 distinct call sites across the suite (incl. a test literally named 'empty_upload_no_error' asserting the current behavior), so flipping it was judged out of scope for this pass — same test-suite-wide-blast-radius reasoning as the EmptyUploadException gap below, which is the same code path."}
  GetDownloadUrlForLayer: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthorizationToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "base64(AWS:dummy-password), 12h TTL, proxyEndpoint derived from first request Host"}
  CreatePullThroughCacheRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePullThroughCacheRules: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePullThroughCacheRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePullThroughCacheRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ValidatePullThroughCacheRule: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateRepositoryCreationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRepositoryCreationTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRepositoryCreationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepositoryCreationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  PutLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "applied immediately on Put, matching AWS's immediate evaluation. FIXED (round 2) — lastEvaluatedAt was a bare time.Time returned directly on the domain struct (RFC3339 string on the wire); real GetLifecyclePolicyOutput.lastEvaluatedAt deserializes via smithytime.ParseEpochSeconds(json.Number). Fixed via lifecyclePolicyResultView (epoch float64), same convention as repositoryView.createdAt."}
  GetLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — see PutLifecyclePolicy note"}
  DeleteLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — see PutLifecyclePolicy note"}
  StartLifecyclePolicyPreview: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — previewResults was []ImageIdentifier (imageDigest/imageTag only); real GetLifecyclePolicyPreviewOutput.previewResults is []types.LifecyclePolicyPreviewResult carrying action{type}, appliedRulePriority, imageDigest, imagePushedAt (epoch), imageTags, storageClass — entirely missing action/priority/pushedAt/storageClass, and the top-level summary.expiringImageTotalCount field was absent too. Fixed: evaluateLifecyclePolicy now returns []LifecyclePolicyPreviewEntry carrying the full AWS-shaped detail, surfaced via lifecyclePolicyPreviewView. GAP still open: real GetLifecyclePolicyPreviewInput also accepts Filter/ImageIds/MaxResults/NextToken (result filtering + pagination); gopherstack always returns the full unfiltered, unpaginated result set — not implemented this pass (see gaps below)."}
  GetLifecyclePolicyPreview: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — see StartLifecyclePolicyPreview note; same Filter/ImageIds/MaxResults/NextToken gap"}
  GetRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  SetRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRegistryPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — RegistryPolicyResult carried a gopherstack-invented 'status' field (\"ACTIVE\"); the real GetRegistryPolicyOutput/PutRegistryPolicyOutput/DeleteRegistryPolicyOutput shapes have only policyText+registryId. Field deleted."}
  PutRegistryPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — same invented 'status' field (\"SetComplete\") deleted; see GetRegistryPolicy note"}
  DeleteRegistryPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (round 2) — same invented 'status' field (\"DELETED\") deleted; see GetRegistryPolicy note"}
  DescribeRegistry: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRegistryScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRegistryScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetRepositoryScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutImageScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutImageTagMutability: {wire: ok, errors: ok, state: ok, persist: ok, note: "exclusion filters (WILDCARD + literal) enforced correctly"}
  StartImageScan: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImageScanFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "BASIC vs ENHANCED finding shapes genuinely differ; paginated via index-based nextToken; ScanNotFoundException for never-scanned images. FIXED (round 2) — ImageScanFindingsResult.completedAt was a bare time.Time under the WRONG key entirely: real ecr.types.ImageScanFindings has no 'completedAt' field at all; the real key is 'imageScanCompletedAt' (epoch seconds, per awsAwsjson11_deserializeDocumentImageScanFindings), plus a second field 'vulnerabilitySourceUpdatedAt' that gopherstack didn't emit at all. A real SDK client parsing gopherstack's old response would silently get a nil/zero ImageScanCompletedAt (unknown JSON keys are ignored, so no hard failure, but the field was simply never populated client-side). Fixed: renamed to ImageScanCompletedAt/VulnerabilitySourceUpdatedAt (float64, epoch seconds); VulnerabilitySourceUpdatedAt is only populated for ENHANCED scans (BASIC omits it, matching AWS's Inspector-only semantics for that field)."}
  PutReplicationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImageReplicationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSigningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutSigningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSigningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImageSigningStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateImageStorageClass: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccountSetting: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccountSetting: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterPullTimeUpdateExclusion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterPullTimeUpdateExclusion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPullTimeUpdateExclusions: {wire: partial, errors: ok, state: ok, persist: ok, note: "RE-VERIFIED round 2: pullTimeUpdateExclusions: []string wire shape itself is correct (confirmed against real ListPullTimeUpdateExclusionsOutput — no per-item createdAt on this op, only a flat ARN list). GAP (not fixed): real input also accepts MaxResults/NextToken; gopherstack always returns the full unpaginated list — low risk given the realistic exclusion-list size, but not implemented (see gaps below)."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  registry-v2-proxy: {status: ok, note: "docker distribution/v3 in-memory storage driver embedded for /v2/ blob+manifest paths; ExtractResource avoids buffering upload bodies"}
  lifecycle-evaluation: {status: ok, note: "priority-ordered rules, imageCountMoreThan + sinceImagePushed count types, tagStatus any/tagged/untagged with prefix+wildcard pattern matching, janitor sweeps on a timer independent of API calls"}
  mock-scanning: {status: ok, note: "deterministic per-digest CVE selection (sha256-seeded bitmask) so repeated scans of the same image are stable; BASIC and ENHANCED shapes are genuinely different data, not the same list reshaped"}
gaps:
  - "EmptyUploadException (CompleteLayerUpload with no UploadLayerPart calls) intentionally NOT enforced. RE-VERIFIED round 2 against the real SDK: types.EmptyUploadException genuinely exists (types/errors.go:40, 'The specified layer upload does not contain any layer parts.'). Independently re-confirmed the blocking test is TestLayerUploadFlow/empty_upload_no_error (layers_test.go) — its own subtest name states the asserted behavior is deliberate, not accidental (the PARITY.md test name cited in the prior audit, TestBatch1_CompleteLayerUpload_Makes_Layer_Available, was stale/renamed since; corrected here). This is the same code path as the newly-documented CompleteLayerUpload 'direct digest'/UploadNotFoundException gap above — ~9 call sites across the suite deliberately Complete an upload with zero or no UploadLayerPart calls as a seeding shortcut. Confirmed still out of scope: fixing requires flipping that shared path, which is a test-suite-wide-blast-radius change. (bd: gopherstack-x6i)"
  - "ImageAlreadyExistsException (PutImage with an unchanged manifest+tag) intentionally NOT enforced. RE-VERIFIED round 2: types.ImageAlreadyExistsException genuinely exists (types/errors.go:119). Independently re-confirmed the blocking test is TestImmutableRepo_SameManifestSameTag_Idempotent (handler_images_test.go; PARITY.md's prior name TestBatch2_ImmutableRepo_SameManifestSameTag_Idempotent was stale, corrected here). Real AWS's exact trigger condition (is it enforced on every repo, or only IMMUTABLE ones?) could not be confirmed without a live-AWS test and the task's explicit required-error-code list does not include it; still deferred to avoid a moderate-confidence behavior flip with test-suite-wide blast radius. (bd: gopherstack-x6i)"
  - "UploadLayerPart minimum-part-size (LayerPartTooSmallException, 'parts must be at least 5MiB except the last') not enforced: the backend cannot know which part is 'last' until CompleteLayerUpload, and all current tests upload tiny (<10 byte) parts, so implementing this needs a size-deferred-validation design (validate at Complete time against accumulated part boundaries) that is out of scope for this pass. RE-VERIFIED round 2: types.LayerPartTooSmallException genuinely exists (types/errors.go:474). (bd: gopherstack-x6i)"
  - "NEW round 2: CompleteLayerUpload never returns UploadNotFoundException (types/errors.go:1244, genuinely exists) for an uploadId that matches no live InitiateLayerUpload session — the 'direct digest' fallback path (layers.go resolveCompletedLayerLocked, third case) accepts any uploadId+digest pair unconditionally. This was not previously documented as a gap (the prior audit's CompleteLayerUpload note only covered the two fixes it made). Deliberately used as a layer-seeding shortcut by ~9 distinct test call sites (layers_test.go, handler_test.go, interfaces_test.go's persistence round-trip test all Complete against uploadIds that were never Initiated), including a test case named 'empty digests still completes' that explicitly asserts success. Same test-suite-wide-blast-radius reasoning as the EmptyUploadException gap above (same code path) — deferred, not fixed."
  - "NEW round 2: GetLifecyclePolicyPreview/StartLifecyclePolicyPreview ignore the real API's Filter, ImageIds, MaxResults, and NextToken request parameters — gopherstack always evaluates and returns the full, unfiltered, unpaginated preview result set for the repository. The per-image response shape itself was fixed this pass (see StartLifecyclePolicyPreview above); request-side filtering/pagination was not, and is a moderate scope addition (new filter-matching + cursor logic) deferred for a future pass."
  - "NEW round 2: DescribeImages's ImageDetail wire shape is missing artifactMediaType, imageScanFindingsSummary, imageScanStatus, lastActivatedAt, lastArchivedAt, lastRecordedPullTime, and subjectManifestDigest — all present on the real types.ImageDetail. imageScanFindingsSummary/imageScanStatus are feasible (the imageScanFindings store already holds the data; would need a per-image lookup inside DescribeImages, handled carefully so an image with no scan yet doesn't error) but were judged a real feature addition beyond this pass's wire-shape-correctness focus, not attempted to avoid a rushed/undertested implementation. lastActivatedAt/lastArchivedAt/lastRecordedPullTime need genuinely new backend state (archive-event and pull-time tracking) that doesn't exist at all today."
  - "NEW round 2: ListPullTimeUpdateExclusions ignores MaxResults/NextToken (always returns the full list, no pagination) — real API supports both. Low risk (this is a niche newer API; realistic exclusion lists are small) but not implemented."
deferred:
  - "docker registry v2 proxy internals (pkgs distribution/v3 wiring) — treated as a vendored subsystem, not re-audited this pass"
  - "chaos/fault-injection interaction with ECR ops — not exercised this pass"
leaks: {status: clean, note: "RE-VERIFIED round 2: layerUploads bounded by FIFO TTL queue pruned on InitiateLayerUpload; janitor (janitor.go) uses pkgs/worker.Group with ctx.Done() shutdown + g.Stop() drain; no unbounded maps found; no new goroutines/locks introduced by this pass's changes (lifecycle.go/lifecycle_policy.go/scan.go/handler_images.go edits were pure data-shape fixes, no new lock paths)"}
---

## Notes

Protocol: JSON RPC 1.1 (`application/x-amz-json-1.1`, `X-Amz-Target:
AmazonEC2ContainerRegistry_V20150921.<Op>`). Timestamps are epoch-seconds JSON
numbers (verified against `awsAwsjson11_deserializeDocumentRepository` etc. in
the vendored SDK deserializer). CORRECTION (round 2): the prior audit's claim
that "this codebase already uses that convention throughout" was **not fully
true** — `LifecyclePolicyResult.LastEvaluatedAt` and
`ImageScanFindingsResult.CompletedAt` were bare `time.Time` fields returned
directly to the wire (RFC3339 strings), and `PutImage`/`BatchGetImage`'s
`Image` response leaked 5 gopherstack-only fields not in the real
`ecr.types.Image` shape. All fixed this pass — see the `PutImage`,
`GetLifecyclePolicy`, and `DescribeImageScanFindings` op notes above for
specifics. The general pattern (dedicated `*View` wire structs built by
`to*View()` conversion functions, e.g. `repositoryView`/`imageDetailView`) is
sound and is now applied consistently to every JSON-tagged `time.Time` field
in the package — reverify with:
`grep -rn 'time\.Time' services/ecr/*.go | grep -v _test.go` and confirm every
hit is either (a) consumed only by a `to*View` converter before reaching the
wire, or (b) purely internal (`layerUploadState`, `lifecycleLastEvaluated`
map) and never JSON-tagged.

This service had already been through multiple prior parity sweeps (test files
named `handler_accuracy_batch1/2`, `handler_refinement1/2`,
`handler_parity_ecr/ecr2`, `audit_ecr_test.go`, `replication_status_test.go`,
`scan_enhanced_test.go`, `lifecycle_expiry_test.go`, `leak_test.go` — ~17.9k LOC
total before this pass). Op-by-op review of `backend.go` confirmed FK
validation (repository/image/rule existence checks) present and correct on
every op **except** `CompleteLayerUpload`, which is the main finding this pass.

### Genuine fixes made this pass

1. **`CompleteLayerUpload` missing repository-existence check.** Every other
   backend method validates `b.repos[repositoryName]` before mutating state;
   `CompleteLayerUpload` did not, so completing an upload against a
   nonexistent repository silently "succeeded" instead of returning
   `RepositoryNotFoundException`. Also exposed a **pre-existing test bug**:
   `TestECR_CompleteLayerUpload` called `CompleteLayerUpload` against
   `"my-repo"`, which the test never created — it was accidentally relying on
   the missing FK check to pass. Fixed by creating the repo in the test setup
   (test now encodes the corrected/real behavior).

2. **`CompleteLayerUpload` missing `LayerAlreadyExistsException`.** AWS
   rejects re-completing a layer digest that is already registered as
   available in the repository (SDK doc: "The image layer already exists in
   the associated repository."). The backend silently overwrote
   `uploadedLayers[repo][digest]` on every call. Fixed by checking for an
   existing entry before writing (guarded against `digest == ""` so the
   pre-existing "empty session, no digest" completion path — exercised
   deliberately by an existing test — cannot self-collide).

3. **`UploadLayerPart` missing part-sequencing validation
   (`InvalidLayerPartException`).** AWS requires each part's `partFirstByte`
   to be consecutive to the number of bytes already received in the session
   ("the first byte specified is not consecutive to the last byte of a
   previous layer part upload"). The parameter was previously discarded
   (`_, lastByte int64`) entirely. Fixed by comparing `firstByte` against
   `upload.Size` and returning the new sentinel error on mismatch. No existing
   test exercises multi-part sequences with gaps, so this is purely additive.

4. **`PutImage` missing `ImageDigestDoesNotMatchException`.** AWS validates a
   caller-supplied `imageDigest` against the digest it computes from the
   manifest and rejects a mismatch. This codebase never checked it — any
   client-supplied digest was trusted verbatim. Implemented **at the JSON
   handler boundary** (`handlePutImage`), not inside
   `InMemoryBackend.PutImage`: dozens of existing unit tests call
   `Backend.PutImage` directly with synthetic, non-cryptographic `ImageDigest`
   values (e.g. `"sha256:abc111"`) that intentionally don't hash-match their
   paired `ImageManifest` text, using the digest purely as a unique key. Wire
   fixtures (`mustPutImage`, SDK-client tests) never set an explicit
   `imageDigest` on the request, so none of them exercise this new check; it
   only fires for a real client-supplied mismatch, matching where AWS itself
   performs the validation (request processing, not backend storage).
   Uncovered a **pre-existing test bug**: `TestECR_NewOps_PersistenceRoundTrip`
   sent `imageDigest: "sha256:persist123"` with `imageManifest: "{}"` — not a
   real sha256 of `"{}"`. Fixed by replacing the literal with the real
   `sha256("{}")` hex digest everywhere it's threaded through that test
   (5 occurrences: layer digest, image digest ×2, image-id lookups ×2).

### Deliberately NOT fixed (see `gaps` above for full rationale)

- `EmptyUploadException` — contradicted by an existing, clearly-intentional
  test.
- `ImageAlreadyExistsException` (idempotent-repush guard) — contradicted by an
  existing, clearly-intentional test; not in the task's explicit
  required-error-code list; moderate-confidence AWS-doc interpretation with
  test-suite-wide blast radius if wrong.
- `LayerPartTooSmallException` — needs a design for deferred (at-Complete-time)
  validation since "last part" isn't knowable at upload time; no test coverage
  either direction.

None of these were silently stubbed — they are explicitly called out as
deferred with the reasoning, per the no-stub principle's guidance to prefer an
explicit terminal note over a guessed half-fix.

### Genuine fixes made this pass, round 2 (2026-07-23)

The round-1 audit (2026-07-05) field-diffed error codes and the
already-fixed ops thoroughly but did not exhaustively re-diff every
JSON-tagged struct field name/type against the real SDK deserializers for
ops it marked `ok`. Independently re-diffing every op this pass (reading
`aws-sdk-go-v2/service/ecr@v1.59.0`'s `types/types.go` and `deserializers.go`
directly, not trusting the prior "ok" marks) surfaced four real wire-shape
bugs:

1. **`LifecyclePolicyResult.LastEvaluatedAt`** (used by
   `GetLifecyclePolicy`/`PutLifecyclePolicy`/`DeleteLifecyclePolicy`) was a
   bare `time.Time` returned directly to the JSON wire — `encoding/json`
   renders that as an RFC3339 string, but the real
   `GetLifecyclePolicyOutput.lastEvaluatedAt` deserializes via
   `smithytime.ParseEpochSeconds(json.Number)` and rejects strings. Fixed by
   adding `lifecyclePolicyResultView` (epoch `float64`), matching the
   `repositoryView.createdAt` convention already used elsewhere in the
   package. `LifecyclePolicyResult` itself is now purely an internal domain
   type (json tags removed from it).

2. **`ImageScanFindingsResult`** (`DescribeImageScanFindings`) had a
   `CompletedAt time.Time \`json:"completedAt"\`` field. The real
   `ecr.types.ImageScanFindings` has **no `completedAt` field at all** — the
   real key is `imageScanCompletedAt` (confirmed against
   `awsAwsjson11_deserializeDocumentImageScanFindings`), and a second field,
   `vulnerabilitySourceUpdatedAt`, was missing entirely. Both are epoch
   seconds. Fixed by renaming to `ImageScanCompletedAt`/
   `VulnerabilitySourceUpdatedAt` (`float64`); the latter is only populated
   for `ENHANCED` scans (BASIC omits it), matching that field's real
   Inspector-vulnerability-source semantics.

3. **`GetLifecyclePolicyPreview`/`StartLifecyclePolicyPreview`**'s
   `previewResults` was `[]ImageIdentifier` (`imageDigest`/`imageTag` only).
   The real `GetLifecyclePolicyPreviewOutput.previewResults` is
   `[]types.LifecyclePolicyPreviewResult`, a genuinely richer per-image shape
   carrying `action{type}`, `appliedRulePriority`, `imageDigest`,
   `imagePushedAt` (epoch), `imageTags`, and `storageClass` — plus a
   top-level `summary.expiringImageTotalCount` that gopherstack didn't emit
   at all. This is the largest fix this pass: `evaluateLifecyclePolicy`
   (lifecycle.go) now returns `[]LifecyclePolicyPreviewEntry` carrying the
   full detail instead of bare identifiers, and a new
   `lifecyclePolicyPreviewView`/`toLifecyclePolicyPreviewView` builds the
   AWS-shaped wire response including the summary count. Existing tests that
   only read `.ImageDigest` off preview entries kept compiling/passing
   unchanged since the new entry type preserves that field name.

4. **`PutImage`/`BatchGetImage`**'s `image`/`images` response fields
   serialized the raw internal `Image` domain struct, which carries 5
   gopherstack-only bookkeeping fields — `imageDigest`, `imagePushedAt`,
   `imageStatus`, `storageClass`, `imageSizeInBytes` — used internally by the
   *separate* `DescribeImages` `ImageDetail` shape. The real
   `ecr.types.Image` (confirmed via
   `awsAwsjson11_deserializeDocumentImage`) has exactly five fields:
   `imageId`, `imageManifest`, `imageManifestMediaType`, `registryId`,
   `repositoryName` — no digest/pushedAt/status/storageClass/size at the top
   level (the digest is available via the correctly-nested
   `imageId.imageDigest`, which was already present and correct). Real SDK
   clients silently ignore unknown JSON keys, so this wasn't a hard
   client-breaking bug, but it violates the "no gopherstack-invented fields"
   rule and the `imagePushedAt` sub-bug (bare `time.Time`) compounds it.
   Fixed via a new `imageView`/`toImageView`; the widely-used
   `mustPutImage` test helper (`handler_test.go`) was updated to read the
   digest from `image.imageId.imageDigest` instead of the deleted top-level
   `image.imageDigest`.

5. **`RegistryPolicyResult`** (`GetRegistryPolicy`/`PutRegistryPolicy`/
   `DeleteRegistryPolicy`) carried a `Status string \`json:"status"\`` field
   populated with fabricated values (`"ACTIVE"`, `"SetComplete"`,
   `"DELETED"`) that have no counterpart in the real
   `GetRegistryPolicyOutput`/`PutRegistryPolicyOutput`/
   `DeleteRegistryPolicyOutput` shapes (confirmed: those carry only
   `policyText`+`registryId`). Deleted per the "delete gopherstack-invented
   fields" directive; no test asserted on it.

Also independently re-verified (not fixed, see `gaps`): the three
round-1-deferred error codes are real SDK error types (confirmed by reading
`types/errors.go` directly — `EmptyUploadException`,
`ImageAlreadyExistsException`, `LayerPartTooSmallException` all genuinely
exist), and the test names cited as blockers in the round-1 PARITY.md were
stale (renamed since); corrected references are in `gaps` above. Also newly
identified (not previously documented) but judged the same deferred-blast-radius
class: `CompleteLayerUpload`'s "direct digest" fallback path never returns
`UploadNotFoundException` for a nonexistent/never-initiated upload session,
and `DescribeImages`'s `ImageDetail` is missing several real fields
(`artifactMediaType`, `imageScanFindingsSummary`, `imageScanStatus`,
`lastActivatedAt`, `lastArchivedAt`, `lastRecordedPullTime`,
`subjectManifestDigest`) — see `gaps` for full detail on both.

All 58 real ECR operations were diffed by name against
`aws-sdk-go-v2/service/ecr@v1.59.0`'s `api_op_*.go` file list this pass
(`diff` of the two sorted op-name lists is empty) — every real op is routed
and no gopherstack-invented op exists.

New tests locking every fix above: `TestLifecyclePolicyResult_LastEvaluatedAt_IsEpochNumber`,
`TestLifecyclePolicyPreview_EntryShape` (lifecycle_policy_test.go);
`TestScan_ImageScanCompletedAt_Populated` (scan_test.go);
`TestDescribeImageScanFindings_ImageScanCompletedAt_WireShape`
(image_scanning_test.go); `TestPutImage_ImageView_OmitsInventedFields`,
`TestBatchGetImage_ImageView_OmitsInventedFields` (handler_images_test.go);
`TestRegistryPolicy_OmitsInventedStatusField` (registry_policy_test.go).
