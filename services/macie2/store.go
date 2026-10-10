package macie2

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultFrequency = "SIX_HOURS"
	statusEnabled    = "ENABLED"
	statusPaused     = "PAUSED"
	statusDisabled   = "DISABLED"
	defaultMatchDist = int32(50)
	// defaultFindingScore is 2 (Medium): real types.Severity.Score is an
	// integer 1 (Low) to 3 (High), not an arbitrary float.
	defaultFindingScore = int64(2)
	// categoryClassification is the real FindingCategory enum value for a
	// sensitive-data finding ("CLASSIFICATION"); "POLICY" is the other. Not
	// to be confused with the JSON "category" ClassificationDetails
	// substructure of the same finding.
	categoryClassification = "CLASSIFICATION"
	keyType                = "type"
	keyCreatedAt           = "createdAt"
	keyUpdatedAt           = "updatedAt"
	keyJobStatus           = "jobStatus"
	defaultPageSize        = 50

	valueNone           = "NONE"
	valueRecommended    = "RECOMMENDED"
	jobStatusRunning    = "RUNNING"
	jobStatusIdle       = "IDLE"
	jobStatusCancelled  = "CANCELLED"
	jobStatusUserPaused = "USER_PAUSED"

	errResourceNotFound    = "ResourceNotFoundException"
	errConflictException   = "ConflictException"
	errValidation          = "ValidationException"
	errMacieNotEnabled     = "AccessDeniedException"
	errUnprocessableEntity = "UnprocessableEntityException"
	errInternalServer      = "InternalServerException"

	maxTagCount    = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxMatchDist   = int32(300)
	minMatchDist   = int32(1)
)

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	orgConfig             *OrgConfig
	findingsPubConfig     *FindingsPublicationConfig
	allowLists            *store.Table[storedAllowList]
	customDataIDs         *store.Table[storedCustomDataID]
	findingsFilters       *store.Table[storedFindingsFilter]
	findings              *store.Table[storedFinding]
	s3Buckets             *store.Table[S3BucketMetadata]
	tags                  map[string]map[string]string
	session               *Session
	classificationJobs    *store.Table[ClassificationJob]
	members               *store.Table[Member]
	invitations           *store.Table[Invitation]
	administrator         *AdministratorAccount
	orgAdminAccounts      *store.Table[OrgAdminAccount]
	registry              *store.Registry
	autoDiscoveryAccounts *store.Table[AutoDiscoveryAccount]
	mu                    *lockmetrics.RWMutex
	classExportConfig     *ClassificationExportConfig
	classScopes           *store.Table[ClassificationScope]
	autoDiscoveryConfig   *AutoDiscoveryConfig
	resourceProfiles      *store.Table[ResourceProfile]
	resourceDetections    map[string][]ResourceProfileDetection
	revealConfig          *RevealConfiguration
	retrievalConfig       *RetrievalConfiguration
	sensitivityTemplates  *store.Table[SensitivityInspectionTemplate]
	paginationSecret      string
	accountID             string
	region                string
	lifecycleDelay        time.Duration
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                  lockmetrics.New("macie2"),
		accountID:           accountID,
		region:              region,
		registry:            store.NewRegistry(),
		tags:                make(map[string]map[string]string),
		orgConfig:           &OrgConfig{AutoEnable: false},
		autoDiscoveryConfig: &AutoDiscoveryConfig{Status: statusDisabled},
		resourceDetections:  make(map[string][]ResourceProfileDetection),
		paginationSecret:    uuid.New().String(),
	}

	registerAllTables(b)

	return b
}

// listPaginated locks for reading, projects/sorts items, and paginates,
// returning the page plus continuation token. Shared by the List* methods.
// itemsFn runs under the lock; pass the table's .All method value, not its result,
// or the read races concurrent writers.
func listPaginated[T any, R any](
	b *InMemoryBackend,
	lockName string,
	itemsFn func() []T,
	mapFn func(T) (R, bool),
	sortFn func([]R),
	token string,
	limit int,
) ([]R, string, error) {
	b.mu.RLock(lockName)
	defer b.mu.RUnlock()

	if token != "" && page.DecodeHMACToken(token, b.paginationSecret) == 0 {
		return nil, "", fmt.Errorf("%w: nextToken is not valid", ErrValidation)
	}

	data, next := mapSortPaginate(itemsFn(), mapFn, sortFn, token, b.paginationSecret, limit)

	return data, next, nil
}

func mapSortPaginate[T any, R any](
	items []T,
	mapFn func(T) (R, bool),
	sortFn func([]R),
	token string,
	secret string,
	limit int,
) ([]R, string) {
	result := make([]R, 0, len(items))

	for _, item := range items {
		if mapped, ok := mapFn(item); ok {
			result = append(result, mapped)
		}
	}

	sortFn(result)

	data, next := paginate(result, token, secret, limit)

	return data, next
}

func paginate[T any](data []T, token, secret string, limit int) ([]T, string) {
	p := page.NewHMAC(data, token, secret, limit, defaultPageSize)

	return p.Data, p.Next
}

// PaginationSecret returns the HMAC secret signing page tokens.
func (b *InMemoryBackend) PaginationSecret() string { return b.paginationSecret }

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.session = nil
	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	b.administrator = nil
	b.orgConfig = &OrgConfig{AutoEnable: false}
	b.autoDiscoveryConfig = &AutoDiscoveryConfig{Status: statusDisabled}
	b.classExportConfig = nil
	b.findingsPubConfig = nil
	b.resourceDetections = make(map[string][]ResourceProfileDetection)
	b.revealConfig = nil
	b.retrievalConfig = nil
}
