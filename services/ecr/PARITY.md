---
service: ecr
sdk_module: aws-sdk-go-v2/service/ecr@v1.58.6
last_audit_commit: fba3c784
last_audit_date: 2026-07-05
overall: B  # already-accurate op-by-op, with a handful of genuine fixes (~190 LOC prod + ~260 LOC new tests)
ops:
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "force-with-images enforced in handler via DescribeImages pre-check"}
  PutImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — added ImageDigestDoesNotMatchException validation at the wire boundary (handler); tag-mutability + retag semantics were already correct"}
  BatchGetImage: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeleteImage: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImages: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImages: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImageReferrers: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchCheckLayerAvailability: {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateLayerUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIFO TTL pruning bounds layerUploads/layerUploadQueue"}
  UploadLayerPart: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — added part-sequencing validation (InvalidLayerPartException) for non-consecutive partFirstByte"}
  CompleteLayerUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — was missing RepositoryNotFoundException FK check (unlike every other op) and never rejected re-completing an already-registered layer digest (LayerAlreadyExistsException)"}
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
  PutLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "applied immediately on Put, matching AWS's immediate evaluation"}
  GetLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  StartLifecyclePolicyPreview: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLifecyclePolicyPreview: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  SetRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRegistryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRegistryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRegistryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRegistry: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRegistryScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRegistryScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetRepositoryScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutImageScanningConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutImageTagMutability: {wire: ok, errors: ok, state: ok, persist: ok, note: "exclusion filters (WILDCARD + literal) enforced correctly"}
  StartImageScan: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImageScanFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "BASIC vs ENHANCED finding shapes genuinely differ; paginated via index-based nextToken; ScanNotFoundException for never-scanned images"}
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
  ListPullTimeUpdateExclusions: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  registry-v2-proxy: {status: ok, note: "docker distribution/v3 in-memory storage driver embedded for /v2/ blob+manifest paths; ExtractResource avoids buffering upload bodies"}
  lifecycle-evaluation: {status: ok, note: "priority-ordered rules, imageCountMoreThan + sinceImagePushed count types, tagStatus any/tagged/untagged with prefix+wildcard pattern matching, janitor sweeps on a timer independent of API calls"}
  mock-scanning: {status: ok, note: "deterministic per-digest CVE selection (sha256-seeded bitmask) so repeated scans of the same image are stable; BASIC and ENHANCED shapes are genuinely different data, not the same list reshaped"}
gaps:
  - "EmptyUploadException (CompleteLayerUpload with no UploadLayerPart calls) intentionally NOT enforced: an existing test (TestBatch1_CompleteLayerUpload_Makes_Layer_Available) explicitly exercises Initiate→Complete with no UploadLayerPart call and asserts success. Flipping this would contradict established, deliberate test behavior without a design doc confirming which is correct for this emulator; deferred rather than guessed. (bd: gopherstack-x6i)"
  - "ImageAlreadyExistsException (PutImage with an unchanged manifest+tag) intentionally NOT enforced: the SDK doc text (\"no changes to the manifest or image tag after the last push\") supports it, but an existing test (TestBatch2_ImmutableRepo_SameManifestSameTag_Idempotent) explicitly asserts idempotent re-push succeeds with 200, and the task's explicit required-error-code list does not include it. Deferred to avoid a moderate-confidence behavior flip with test-suite-wide blast radius. (bd: gopherstack-x6i)"
  - "UploadLayerPart minimum-part-size (LayerPartTooSmallException, 'parts must be at least 5MiB except the last') not enforced: the backend cannot know which part is 'last' until CompleteLayerUpload, and all current tests upload tiny (<10 byte) parts, so implementing this needs a size-deferred-validation design (validate at Complete time against accumulated part boundaries) that is out of scope for this pass. (bd: gopherstack-x6i)"
deferred:
  - "docker registry v2 proxy internals (pkgs distribution/v3 wiring) — treated as a vendored subsystem, not re-audited this pass"
  - "chaos/fault-injection interaction with ECR ops — not exercised this pass"
leaks: {status: clean, note: "layerUploads bounded by FIFO TTL queue pruned on InitiateLayerUpload; janitor uses worker.Group with ctx.Done() shutdown; no unbounded maps found in this pass"}
---

## Notes

Protocol: JSON RPC 1.1 (`application/x-amz-json-1.1`, `X-Amz-Target:
AmazonEC2ContainerRegistry_V20150921.<Op>`). Timestamps are epoch-seconds JSON
numbers (verified against `awsAwsjson11_deserializeDocumentRepository` etc. in
the vendored SDK deserializer) — this codebase already uses that convention
throughout (`createdAt float64`, etc.), matching `smithytime.ParseEpochSeconds`.

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
