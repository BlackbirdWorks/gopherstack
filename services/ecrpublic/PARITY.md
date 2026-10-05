---
service: ecrpublic
sdk_module: aws-sdk-go-v2/service/ecrpublic@v1.47.1
last_audit_commit: 5bb0d02ee
last_audit_date: 2026-10-05
overall: A            # A: SDK-driven test/integration suite TestIntegration_ECRPublic_RepositoryImageLifecycle (test/integration/ecrpublic_test.go) + every buildable gap closed; remaining divergences are structural_gaps.
ops:
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRepositories: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "force required to delete a non-empty repository"}
  GetRepositoryCatalogData: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRepositoryCatalogData: {wire: ok, errors: ok, state: ok, persist: ok}
  SetRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepositoryPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRegistries: {wire: ok, errors: ok, state: ok, persist: n/a, note: "one public registry per account, matching AWS; always the caller's own"}
  GetRegistryCatalogData: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRegistryCatalogData: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthorizationToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "stable dummy AWS:password credential, matches services/ecr; see structural_gaps"}
  DescribeImages: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImageTags: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchCheckLayerAvailability: {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateLayerUpload: {wire: ok, errors: ok, state: ok, persist: n/a, note: "in-flight sessions never persisted, matching AWS; abandoned sessions pruned lazily after layerUploadTTL (24h)"}
  UploadLayerPart: {wire: ok, errors: ok, state: ok, persist: n/a}
  CompleteLayerUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "SHA256 computed from accumulated bytes; verified against a caller-supplied full digest"}
  PutImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-10-05: rejects a manifest referencing layer/config digests never uploaded (LayersNotFoundException) and an OCI index / manifest list whose child manifests were never pushed (ReferencedImagesNotFoundException); imageManifestMediaType is taken from the manifest mediaType when the request omits it, InvalidParameterException when neither carries one; artifactMediaType recorded from artifactType or config.mediaType"}
  BatchDeleteImage: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Repository: {status: ok, note: "Create/Describe/Delete verified end-to-end against the real aws-sdk-go-v2 client over an httptest server -- ARN (arn:aws:ecr-public::<account>:repository/<name>, no region segment), repositoryUri (public.ecr.aws/<alias>/<name>), and epoch createdAt all round-trip cleanly."}
  CatalogData: {status: ok, note: "Repository- and registry-level catalog data round-trip the full shape (aboutText/description/usageText/architectures/operatingSystems/logoImageBlob). logoUrl is a synthetic placeholder host, not a real asset store."}
  RepositoryPolicy: {status: ok, note: "Set/Get/Delete round-trip opaque policy text; RepositoryPolicyNotFoundException on a repository with no policy."}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource key off the repository ARN, the only taggable resource in this API."}
  Registry: {status: ok, note: "DescribeRegistries/Get+PutRegistryCatalogData/GetAuthorizationToken -- see items_still_open for the single-tenant and dummy-credential simplifications."}
  ImagesAndLayers: {status: ok, note: "BatchCheckLayerAvailability/InitiateLayerUpload/UploadLayerPart/CompleteLayerUpload/PutImage/BatchDeleteImage/DescribeImages/DescribeImageTags all mutate real in-memory state: layer bytes are buffered and SHA256-verified, PutImage verifies every layer/config digest a manifest references was actually uploaded first (and every child manifest of an OCI index was pushed), and BatchDeleteImage's by-tag vs by-digest semantics match AWS (by-tag removes only the binding; the image survives untagged)."}
gaps: []
items_still_open: []
structural_gaps:
  - "No Docker Registry v2 data plane: a docker push/pull addresses the repositoryUri host (public.ecr.aws/<alias>/<name>), which needs public DNS and TLS the emulator cannot own, and /v2/ on the emulator port is already claimed by services/ecr, whose embedded distribution registry is itself not coupled to its control-plane state. The layer/image control-plane ops are real (bytes buffered and SHA256-verified, manifests validated, tags tracked) but nothing serves blobs over /v2."
  - "GetAuthorizationToken returns a stable AWS:password credential: with no registry to authenticate against there is nothing for a docker login to be checked by (same convention as services/ecr)."
  - "verified / marketplaceCertified badges are always false: they are set by the AWS Marketplace vendor-verification workflow, a human process no emulator can drive."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; the backend owns no goroutines or timers (upload sessions are pruned lazily)."}
---

## Notes

Initial implementation (2026-09-25): JSON-RPC (awsjson1.1, X-Amz-Target prefix
`SpencerFrontendService.`) control-plane API modeled after services/kinesisvideo's
pkgs/store + pkgs/lockmetrics + pkgs/persistence layout, and after services/ecr's
X-Amz-Target dispatch via pkgs/service.HandleTarget/WrapOp for the shared protocol.
Every operation mutates/reads real in-memory state, with JSON snapshot/restore wired
into pkgs/persistence.

Wire shapes and errors were verified directly against the pinned
aws-sdk-go-v2/service/ecrpublic v1.47.1 request_snapshot/ and response_snapshot/
fixtures -- that SDK version generates via smithy schemas rather than the older
serializers.go/deserializers.go, so those exact-wire-JSON snapshot fixtures (one
per operation, request and response, plus one per reachable exception) are the
authoritative source, not a serializer function body. Confirmed from AWS docs and
the terraform-provider-aws `aws_ecrpublic_repository` resource: unlike private ECR,
the repository ARN omits the region segment
(`arn:aws:ecr-public::<account>:repository/<name>`), and repositoryUri follows
`public.ecr.aws/<registryAlias>/<name>` with a registry alias derived
deterministically per account (a real alias is an opaque, AWS-assigned string).

### 2026-09-26: InitiateLayerUpload session leak fixed

Unfinished `InitiateLayerUpload` sessions (never reaching
`CompleteLayerUpload`) were retained in `layerUploads` forever, leaking
memory in a long-running server. AWS does not document an explicit expiry
window for unfinished layer uploads, so this follows services/ecr's own
`layerUploadTTL` precedent: sessions older than 24h are now pruned lazily on
the next `InitiateLayerUpload` call (`pruneExpiredLayerUploadsLocked`, in
layers.go). Covered by `layer_upload_ttl_test.go`
(`testing/synctest`-driven: kept within the window, evicted just past it).

## 2026-10-04 (gopherstack-jrfzw multi-region)

ecrpublic stays global by design: AWS serves ECR Public only from us-east-1, so a repository created through any region lands in the one store. Proof: `TestRegionIsolation/ecrpublic` (global case).

## 2026-10-04 (gopherstack-uox6 value-semantics)

DescribeRepositories/DescribeImages/DescribeImageTags now paginate: maxResults 1-1000 (default 100), nextToken honoured; maxResults/nextToken combined with repositoryNames/imageIds is rejected ("you can't use this option"). DescribeImageTags orders by tag.

### 2026-10-05: A grade

Grade moves B to A on `TestIntegration_ECRPublic_RepositoryImageLifecycle` (test/integration/ecrpublic_test.go: repository, layer upload, image push, OCI index push, referenced-image error, policy, force delete through the real SDK client against the Docker image) plus the one buildable gap the B grade named.

- PutImage now parses OCI image indexes / Docker manifest lists: every child manifest digest must already exist in the repository or the call fails with ReferencedImagesNotFoundException (documented on the SDK's PutImage error set). Covered by `TestPutImageManifestIndex` (manifest_index_test.go).
- imageManifestMediaType falls back to the manifest's own mediaType (the SDK field doc says it is required only when the manifest has none); with neither, InvalidParameterException. DescribeImages/DescribeImageTags artifactMediaType is now recorded (artifactType, else config.mediaType) instead of always empty. Covered by `TestPutImageMediaTypeRequirement` and `TestDescribeImagesArtifactMediaType`.
- The single-tenant DescribeRegistries entry is no longer an open item: an account has exactly one public registry, so returning the caller's own is the AWS behaviour. The remaining three divergences are recorded under structural_gaps (they need public DNS/TLS, a docker-login registry, or the Marketplace verification process).

## 2026-10-05 (gopherstack-uox6 pass 12, value semantics)

Clean. PutRepositoryCatalogData replaces the catalog data and PutRegistryCatalogData replaces the display name; the SDK says only 'creates or updates', so full replacement is an interpretation. Repository creation stores CatalogData that GetRepositoryCatalogData returns.
