package ecr

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (and, for a bare scalar value, map[string]T) resource field on
// InMemoryBackend whose value has a pure identity of its own is registered
// exactly once here as a *store.Table[T] on b.registry. See pkgs/store's
// package doc and the services/ec2 (commit 12e611a4, data-driven registration
// slice) and services/sqs (commit 0f09d77c, DTO-registry) conversions this
// follows, plus services/cloudwatchlogs's kmsKeyEntry-style DTO wrapping for
// the bare-scalar-value case.
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the comment above registerAllTables for the full list and why.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// --- store.Table / store.Index key functions ---

func repoKeyFn(r *Repository) string { return r.RepositoryName }

// imageTableKey returns the store.Table primary key for an image: repository
// name and digest joined by "@", matching the OCI/Docker image-reference
// convention (name@digest). "@" is not a valid character in an ECR repository
// name and never appears in a "sha256:..." digest, so this cannot collide.
func imageTableKey(repositoryName, digest string) string { return repositoryName + "@" + digest }

func imageKeyFn(img *Image) string { return imageTableKey(img.RepositoryName, img.ImageDigest) }

// imageRepoIndexKeyFn groups images by owning repository, replacing the old
// repositoryName->digest->*Image map nesting for repo-scoped access
// (DescribeImages/ListImages/DeleteRepository cascade/lifecycle evaluation).
func imageRepoIndexKeyFn(img *Image) string { return img.RepositoryName }

// findingsTableKey returns the store.Table primary key for an image scan
// findings record; it mirrors imageTableKey exactly since a findings record's
// identity is the same (repository, digest) pair as the image it describes.
func findingsTableKey(repositoryName, digest string) string {
	return imageTableKey(repositoryName, digest)
}

func findingsKeyFn(f *ImageScanFindingsResult) string {
	return findingsTableKey(f.RepositoryName, f.ImageID.ImageDigest)
}

// findingsRepoIndexKeyFn groups scan findings by owning repository, replacing
// the old repositoryName->digest->*ImageScanFindingsResult map nesting
// (DeleteRepository cascade cleanup).
func findingsRepoIndexKeyFn(f *ImageScanFindingsResult) string { return f.RepositoryName }

func pullThroughCacheRuleKeyFn(r *PullThroughCacheRule) string { return r.EcrRepositoryPrefix }

func repositoryCreationTemplateKeyFn(t *RepositoryCreationTemplate) string { return t.Prefix }

func lifecyclePolicyPreviewKeyFn(p *LifecyclePolicyPreviewResult) string { return p.RepositoryName }

func pullTimeUpdateExclusionKeyFn(e *PullTimeUpdateExclusion) string { return e.PrincipalArn }

// lifecyclePolicyEntry wraps the bare lifecycle-policy-text value (previously
// map[string]string) in a minimal identified struct, since store.Table
// requires values to carry their own primary key (matches
// services/cloudwatchlogs's kmsKeyEntry-style wrapping of a bare
// map[string]string). lifecycleLastEvaluated is intentionally NOT folded into
// this entry -- it was never part of backendSnapshot before this conversion
// (GetLifecyclePolicy's LastEvaluatedAt does not survive a restore today), and
// folding it in would silently start persisting data that wasn't persisted
// before. It remains its own plain map; see registerAllTables below.
type lifecyclePolicyEntry struct {
	RepositoryName string `json:"repositoryName"`
	PolicyText     string `json:"policyText"`
}

func lifecyclePolicyEntryKeyFn(e *lifecyclePolicyEntry) string { return e.RepositoryName }

// repositoryPolicyEntry wraps the bare repository-policy-text value
// (previously map[string]string); see lifecyclePolicyEntry's doc.
type repositoryPolicyEntry struct {
	RepositoryName string `json:"repositoryName"`
	PolicyText     string `json:"policyText"`
}

func repositoryPolicyEntryKeyFn(e *repositoryPolicyEntry) string { return e.RepositoryName }

// accountSettingEntry wraps a bare account-setting value (previously
// map[string]string); see lifecyclePolicyEntry's doc.
type accountSettingEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func accountSettingEntryKeyFn(e *accountSettingEntry) string { return e.Name }

// registerAllTables registers every converted resource table on b.registry
// exactly once. It must be called during construction only (immediately
// after b.registry is created), never on every Reset() -- store.Register
// panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately left as plain maps (NOT
// registered here):
//
//   - repoTags: map[string]map[string]string keyed by resource ARN; the value
//     (a bare tag map) carries no identity field of its own to serve as a
//     store.Table key (matches services/s3's "tags" field exception). Still
//     persisted directly in backendSnapshot, exactly as before this
//     conversion.
//   - tagIndex: map[string]map[string]string, repo -> tag -> digest; the
//     value (a digest string) carries no identity field of its own, and the
//     nesting is itself a secondary index derived from images, not a resource
//     in its own right. Was never persisted before this conversion (a
//     restored backend rebuilds nothing into it -- a pre-existing gap, not a
//     regression introduced here) and still isn't.
//   - digestTagsIndex: map[string]map[string][]string, repo -> digest ->
//     []tag; slice-valued, no identity field of its own. Same never-persisted
//     status as tagIndex, preserved as-is.
//   - uploadedLayers: map[string]map[string]int64, repo -> digest -> size; the
//     value (int64) carries no identity field of its own (matches
//     services/apigateway's usagePlanKeys exception for a bare int64). Still
//     persisted directly in backendSnapshot, exactly as before.
//   - layerUploads: map[string]*layerUploadState; layerUploadState carries no
//     upload-ID field of its own (the ID is the map key only), the same "no
//     identity field" exception documented for EC2's instanceIMDSOptions
//     (commit 12e611a4). Left unconverted so the parity-sweep layer
//     part-sequencing / digest-verification logic in CompleteLayerUpload and
//     UploadLayerPart is untouched. Never persisted before this conversion
//     (in-progress uploads do not survive a restart, matching AWS) and still
//     isn't.
//   - repoUploadIndex: map[string]map[string]struct{}, a secondary index over
//     layerUploads (itself unconverted above); non-*T value, no identity
//     field. Never persisted, still isn't.
//   - lifecycleLastEvaluated: map[string]time.Time; a bare time.Time carries
//     no identity field of its own. Never persisted before this conversion
//     (GetLifecyclePolicy's LastEvaluatedAt resets to zero across a restore
//     today), still isn't -- see lifecyclePolicyEntry's doc for why it was
//     not folded into that table instead.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.repos = store.Register(b.registry, "repos", store.New(repoKeyFn))
	},
	func(b *InMemoryBackend) {
		imagesT := store.Register(b.registry, "images", store.New(imageKeyFn))
		b.images = imagesT
		b.imagesByRepo = imagesT.AddIndex("repo", imageRepoIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		findingsT := store.Register(b.registry, "imageScanFindings", store.New(findingsKeyFn))
		b.imageScanFindings = findingsT
		b.imageScanFindingsByRepo = findingsT.AddIndex("repo", findingsRepoIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.pullThroughCacheRules = store.Register(
			b.registry, "pullThroughCacheRules", store.New(pullThroughCacheRuleKeyFn))
	},
	func(b *InMemoryBackend) {
		b.repositoryCreationTemplates = store.Register(
			b.registry, "repositoryCreationTemplates", store.New(repositoryCreationTemplateKeyFn))
	},
	func(b *InMemoryBackend) {
		b.lifecyclePolicies = store.Register(
			b.registry, "lifecyclePolicies", store.New(lifecyclePolicyEntryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.lifecyclePolicyPreviews = store.Register(
			b.registry, "lifecyclePolicyPreviews", store.New(lifecyclePolicyPreviewKeyFn))
	},
	func(b *InMemoryBackend) {
		b.repositoryPolicies = store.Register(
			b.registry, "repositoryPolicies", store.New(repositoryPolicyEntryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.accountSettings = store.Register(
			b.registry, "accountSettings", store.New(accountSettingEntryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.pullTimeUpdateExclusions = store.Register(
			b.registry, "pullTimeUpdateExclusions", store.New(pullTimeUpdateExclusionKeyFn))
	},
}
