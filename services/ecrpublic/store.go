package ecrpublic

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	ecrPublicService = "ecr-public"

	// repositoryURIHost is the fixed public pull host; see
	// https://docs.aws.amazon.com/AmazonECR/latest/public/public-getting-started.html --
	// "public.ecr.aws/registry_alias/repository_name".
	repositoryURIHost = "public.ecr.aws"
)

// InMemoryBackend is the in-memory implementation of Backend. Amazon ECR
// Public is a single-region (us-east-1), single-registry-per-account service:
// there is no per-region partitioning of repositories, matching the real API.
type InMemoryBackend struct {
	registry     *store.Registry
	repos        *store.Table[Repository]
	images       *store.Table[Image]
	imagesByRepo *store.Index[Image]

	// tagIndex, uploadedLayers, and layerUploads carry no identity field of
	// their own (see services/ecr/store_setup.go's registerAllTables doc for
	// the same exemption pattern) and are left as plain maps rather than
	// store.Table entries.
	tagIndex       map[string]map[string]tagBinding
	uploadedLayers map[string]map[string]int64
	layerUploads   map[string]*layerUploadState

	registryCatalogData RegistryCatalogData

	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
	registryAlias  string
	layerUploadSeq uint64
}

// NewInMemoryBackend creates a new Amazon ECR Public backend for accountID.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:       store.NewRegistry(),
		tagIndex:       make(map[string]map[string]tagBinding),
		uploadedLayers: make(map[string]map[string]int64),
		layerUploads:   make(map[string]*layerUploadState),
		mu:             lockmetrics.New("ecrpublic"),
		accountID:      accountID,
		region:         region,
		registryAlias:  registryAliasForAccount(accountID),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tagIndex = make(map[string]map[string]tagBinding)
	b.uploadedLayers = make(map[string]map[string]int64)
	b.layerUploads = make(map[string]*layerUploadState)
	b.layerUploadSeq = 0
	b.registryCatalogData = RegistryCatalogData{}
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// shortHash returns a short, deterministic hex digest of s, used to derive a
// stable-looking pseudo-random registry alias from an account ID.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])[:10]
}

// registryAliasForAccount derives a stable default registry alias for an
// account. Real aliases are opaque, account-scoped strings assigned by AWS
// (e.g. "a1b2c3d4e5"); this emulator derives one deterministically so it is
// stable across restarts (and across Restore) without persisting it.
func registryAliasForAccount(accountID string) string {
	return shortHash(accountID)
}

// repositoryARN builds the ARN for a public repository. Confirmed against AWS
// docs and the terraform-provider-aws ecrpublic_repository resource: unlike
// private ECR, the region segment is empty --
// arn:aws:ecr-public::<account>:repository/<name>.
func repositoryARN(region, accountID, name string) string {
	return arn.BuildGlobal(ecrPublicService, region, accountID, "repository/"+name)
}

// registryARN builds the ARN for the caller's public registry itself
// (as opposed to a repository within it).
func registryARN(region, accountID string) string {
	return arn.BuildGlobal(ecrPublicService, region, accountID, "registry")
}

// repositoryURI builds the docker pull URI for a public repository:
// public.ecr.aws/<registryAlias>/<name>.
func repositoryURI(alias, name string) string {
	return fmt.Sprintf("%s/%s/%s", repositoryURIHost, alias, name)
}

// resolveRegistryIDLocked validates a caller-supplied registryId against this
// single-tenant emulator's own account. An empty registryId defaults to the
// caller's own account, matching AWS. Caller must hold b.mu.
func (b *InMemoryBackend) resolveRegistryIDLocked(registryID string) error {
	if registryID == "" || registryID == b.accountID {
		return nil
	}

	return fmt.Errorf("%w: %s", ErrRegistryNotFound, registryID)
}
