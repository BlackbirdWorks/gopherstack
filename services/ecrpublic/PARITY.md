---
service: ecrpublic
sdk_module: aws-sdk-go-v2/service/ecrpublic@v1.47.1
last_audit_commit: 709187947
last_audit_date: 2026-09-25
overall: B            # new service, control plane + honest layer/image metadata tracking, unit-tested against the real SDK client
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
  DescribeRegistries: {wire: ok, errors: ok, state: ok, persist: n/a, note: "single-tenant emulator: always returns exactly the caller's own registry -- see items_still_open"}
  GetRegistryCatalogData: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRegistryCatalogData: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthorizationToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "stable dummy AWS:password credential, matches services/ecr's convention -- see items_still_open"}
  DescribeImages: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImageTags: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchCheckLayerAvailability: {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateLayerUpload: {wire: ok, errors: ok, state: ok, persist: n/a, note: "in-flight sessions never persisted, matching AWS; abandoned sessions pruned lazily after layerUploadTTL (24h)"}
  UploadLayerPart: {wire: ok, errors: ok, state: ok, persist: n/a}
  CompleteLayerUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "SHA256 computed from accumulated bytes; verified against a caller-supplied full digest"}
  PutImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects a manifest referencing layer/config digests never uploaded (LayersNotFoundException)"}
  BatchDeleteImage: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Repository: {status: ok, note: "Create/Describe/Delete verified end-to-end against the real aws-sdk-go-v2 client over an httptest server -- ARN (arn:aws:ecr-public::<account>:repository/<name>, no region segment), repositoryUri (public.ecr.aws/<alias>/<name>), and epoch createdAt all round-trip cleanly."}
  CatalogData: {status: ok, note: "Repository- and registry-level catalog data round-trip the full shape (aboutText/description/usageText/architectures/operatingSystems/logoImageBlob). logoUrl is a synthetic placeholder host, not a real asset store."}
  RepositoryPolicy: {status: ok, note: "Set/Get/Delete round-trip opaque policy text; RepositoryPolicyNotFoundException on a repository with no policy."}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource key off the repository ARN, the only taggable resource in this API."}
  Registry: {status: ok, note: "DescribeRegistries/Get+PutRegistryCatalogData/GetAuthorizationToken -- see items_still_open for the single-tenant and dummy-credential simplifications."}
  ImagesAndLayers: {status: ok, note: "BatchCheckLayerAvailability/InitiateLayerUpload/UploadLayerPart/CompleteLayerUpload/PutImage/BatchDeleteImage/DescribeImages/DescribeImageTags all mutate real in-memory state: layer bytes are buffered and SHA256-verified, PutImage verifies every layer/config digest a manifest references was actually uploaded first, and BatchDeleteImage's by-tag vs by-digest semantics match AWS (by-tag removes only the binding; the image survives untagged)."}
gaps: []
items_still_open:
  - "There is no embedded Docker Registry v2 HTTP API (unlike services/ecr's optional
    GOPHERSTACK_ENABLE_LOCAL_REGISTRY local registry): the control-plane layer/image
    operations (BatchCheckLayerAvailability, InitiateLayerUpload, UploadLayerPart,
    CompleteLayerUpload, PutImage, BatchDeleteImage) track real layer/image metadata, but
    nothing serves the resulting blobs over /v2/... for an actual `docker pull` against a
    public.ecr.aws-style host. Structural: out of scope for this pass."
  - "GetAuthorizationToken returns a stable dummy AWS:password credential and does not
    enforce docker-login authentication against it, matching services/ecr's existing
    convention for the same operation."
  - "DescribeRegistries is single-tenant: it always returns exactly the caller's own
    registry, never other accounts' registries. There is no cross-account Amazon ECR
    Public Gallery directory modeled (that surface is the public gallery.ecr.aws website,
    not this control-plane API, but even the multi-account admin view this operation can
    return for a verified account is not modeled)."
  - "Registry/repository 'verified' and marketplaceCertified badges are always false --
    the Amazon Web Services Marketplace vendor verification workflow is not modeled."
  - "PutImage's manifest-layer verification only understands a plain OCI/Docker image
    manifest ({config.digest, layers[].digest}); a manifest list / OCI index (multi-arch)
    is not parsed for referenced digests and is pushed without that check. Real docker
    clients pushing multi-arch images would not get LayersNotFoundException protection
    for the top-level manifest list, only for each per-platform manifest they also push."
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
