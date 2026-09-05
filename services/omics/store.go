package omics

import (
	"context"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// WithRegion returns a context with the given region set.
func WithRegion(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, regionContextKey{}, region)
}

const (
	errResourceNotFound = "ResourceNotFoundException"
	errConflict         = "ConflictException"
	errValidation       = "ValidationException"

	statusActive    = "ACTIVE"
	statusDeleting  = "DELETING"
	statusCreating  = "CREATING"
	statusFailed    = "FAILED"
	statusCompleted = "COMPLETED"
	statusRunning   = "RUNNING"
	statusCancelled = "CANCELLED"
	statusPending   = "PENDING"

	// statusProcessed and statusRunsDeleted are RunBatch-only BatchStatus
	// values (real AWS BatchStatus has no "COMPLETED" member -- the
	// successful-terminal state for a batch is "PROCESSED"; "RUNS_DELETED"
	// is the terminal state after DeleteRunBatch removes the batch's runs).
	statusProcessed   = "PROCESSED"
	statusRunsDeleted = "RUNS_DELETED"

	// networkingModeRestricted is StartRunInput.NetworkingMode's documented
	// default ("If not specified, this will default to RESTRICTED.",
	// omics@v1.49.5 api_op_StartRun.go:136-138).
	networkingModeRestricted = "RESTRICTED"

	// cacheBehaviorOnFailure is CreateRunCacheInput.CacheBehavior's
	// documented default ("If you don't specify a value, the default
	// behavior is CACHE_ON_FAILURE").
	cacheBehaviorOnFailure = "CACHE_ON_FAILURE"

	// retentionModeRetain is StartRunInput.RetentionMode's documented
	// default ("The default value is RETAIN").
	retentionModeRetain = "RETAIN"

	// scratchStorageModeShared is StartRunInput.ScratchStorageMode's
	// documented default ("If not specified, this will default to SHARED").
	scratchStorageModeShared = "SHARED"

	// storageTypeStatic is StartRunInput.StorageType's documented default
	// ("By default, the run uses STATIC storage type").
	storageTypeStatic  = "STATIC"
	storageTypeDynamic = "DYNAMIC"

	// storageCapacityDefaultGiB is StartRunInput.StorageCapacity's
	// documented default ("The default run storage capacity is 1200 GiB"),
	// applied only when StorageType is STATIC (the SDK doc states DYNAMIC
	// ignores any value entered).
	storageCapacityDefaultGiB = 1200

	// workflowTypePrivate is StartRunInput.WorkflowType's documented default
	// ("If you are running a PRIVATE workflow (default), you do not need to
	// include the workflow type").
	workflowTypePrivate   = "PRIVATE"
	workflowTypeReady2Run = "READY2RUN"

	maxPageSize = 100
	maxTags     = 200

	// keyContentLength is the real FileInformation JSON key (omics@v1.49.5
	// deserializers.go:22978) shared by ReferenceMetadata.Files and
	// ReadSetMetadata.Files.
	keyContentLength = "contentLength"

	stubTaskCPUs   = 2
	stubTaskMemory = 4096

	// pollCountRunningToCompleted is the poll count at which a Run/RunTask
	// that has already reached RUNNING advances to COMPLETED (one poll to
	// enter RUNNING, one more to finish) -- see advanceRunStatus and
	// GetRunTask.
	pollCountRunningToCompleted = 2
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New(errConflict, awserr.ErrAlreadyExists)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
	// ErrInvalidState is returned when a resource is not in the state
	// required for the requested operation (e.g. deleting a RunBatch that
	// hasn't reached a terminal status).
	ErrInvalidState = awserr.New(errConflict, awserr.ErrConflict)
)

// InMemoryBackend is the in-memory backend for HealthOmics.
//
// Every resource collection that carries its own identity field (ID, Name, or
// ShareID) is a directly keyed [store.Table]; every collection that was
// previously nested by a parent store/workflow/run/annotation-store is a
// [store.Table] keyed by the composite "parent|id" string (see parentKey),
// with a companion [store.Index] grouping entries by parent for per-parent
// scans -- the same pattern services/emr and services/codeartifact use for
// their region-nested resources. HealthOmics's "region" dimension itself was
// never actually exercised (every pre-refactor call site read/wrote
// b.region(b.defaultRegion) only -- WithRegion's context value never reached
// a lookup), so unlike emr/codeartifact no region qualifier is threaded
// through these keys; only the genuine parent/child nesting is preserved.
//
// uploadParts (order-sensitive: part listing walks it in append order, not
// sorted order), uploadPartData/readSetBytes/referenceBytes (raw []byte
// values, not *V), and tags (map[string]string values, not *V) are not
// eligible for store.Table and remain plain nested maps.
// Field order below is deliberate: it was determined by running
// fieldalignment -fix on an isolated scratch copy of this struct (never
// directly on this package -- see the fieldalignment hazard note in
// .claude/memories/parity-principles.md-adjacent process docs) and then
// hand-applied here. It shaves 8 pointer-bytes of padding off the struct;
// do not reorder without re-checking on a scratch copy.
type InMemoryBackend struct {
	referenceImportJobs *store.Table[ReferenceImportJob]
	readSets            *store.Table[ReadSetMetadata]

	mu       *lockmetrics.RWMutex
	registry *store.Registry

	referenceStores      *store.Table[ReferenceStore]
	sequenceStores       *store.Table[SequenceStore]
	runGroups            *store.Table[RunGroup]
	runs                 *store.Table[Run]
	workflows            *store.Table[Workflow]
	annotationStores     *store.Table[AnnotationStore]
	annotationImportJobs *store.Table[AnnotationImportJob]
	variantStores        *store.Table[VariantStore]
	variantImportJobs    *store.Table[VariantImportJob]
	shares               *store.Table[Share]
	runCaches            *store.Table[RunCache]
	runBatches           *store.Table[RunBatch]
	configurations       *store.Table[Configuration]

	referenceImportJobsByStore *store.Index[ReferenceImportJob]
	references                 *store.Table[ReferenceMetadata]
	referencesByStore          *store.Index[ReferenceMetadata]

	// Left-raw maps: non-*V values or order-sensitive, ineligible for store.Table.
	tags           map[string]map[string]string
	referenceBytes map[string]map[string][]byte

	s3AccessPolicies *store.Table[S3AccessPolicy]

	readSetsByStore              *store.Index[ReadSetMetadata]
	readSetActivationJobs        *store.Table[ReadSetActivationJob]
	readSetActivationJobsByStore *store.Index[ReadSetActivationJob]
	readSetExportJobs            *store.Table[ReadSetExportJob]
	readSetExportJobsByStore     *store.Index[ReadSetExportJob]
	readSetImportJobs            *store.Table[ReadSetImportJob]
	readSetImportJobsByStore     *store.Index[ReadSetImportJob]
	multipartUploads             *store.Table[MultipartReadSetUpload]
	multipartUploadsByStore      *store.Index[MultipartReadSetUpload]

	runTasks      *store.Table[RunTask]
	runTasksByRun *store.Index[RunTask]

	workflowVersions           *store.Table[WorkflowVersion]
	workflowVersionsByWorkflow *store.Index[WorkflowVersion]

	annotationVersions        *store.Table[AnnotationStoreVersion]
	annotationVersionsByStore *store.Index[AnnotationStoreVersion]

	uploadParts    map[string]map[string][]*ReadSetUploadPart
	uploadPartData map[string]map[string]map[string]map[int][]byte
	readSetBytes   map[string]map[string][]byte

	accountID     string
	defaultRegion string
}

// parentKey builds the composite store.Table primary key ("parent|id") shared
// by every parent-nested resource collection below.
func parentKey(parent, id string) string { return parent + "|" + id }

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("omics"),
		registry:      store.NewRegistry(),

		uploadParts:    make(map[string]map[string][]*ReadSetUploadPart),
		uploadPartData: make(map[string]map[string]map[string]map[int][]byte),
		readSetBytes:   make(map[string]map[string][]byte),
		referenceBytes: make(map[string]map[string][]byte),
		tags:           make(map[string]map[string]string),
	}

	registerAllTables(b)

	return b
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()

	b.uploadParts = make(map[string]map[string][]*ReadSetUploadPart)
	b.uploadPartData = make(map[string]map[string]map[string]map[int][]byte)
	b.readSetBytes = make(map[string]map[string][]byte)
	b.referenceBytes = make(map[string]map[string][]byte)
	b.tags = make(map[string]map[string]string)
}

func newID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")[:12]
}

func copyTags(tags map[string]string) map[string]string {
	if tags == nil {
		return map[string]string{}
	}

	return maps.Clone(tags)
}

// filterIDs extracts the key (via keyFn) of every item in items that
// satisfies keep, in order. It's the shared tail of every parent-scoped
// List* op below: fetch the parent's child group (a []*T from a
// [store.Index.Get]), optionally filter it, then collect ids for
// paginatedCopies. Factored out to keep e.g. ListReferences/
// ListReferenceImportJobs/ListRunTasks/ListWorkflowVersions from being
// flagged as near-duplicates of each other -- their only real difference is
// this key/predicate pair.
func filterIDs[T any](items []*T, keyFn func(*T) string, keep func(*T) bool) []string {
	ids := make([]string, 0, len(items))

	for _, item := range items {
		if keep(item) {
			ids = append(ids, keyFn(item))
		}
	}

	return ids
}

// listChildFiltered combines filterIDs and paginatedCopies into the single
// call every parent-scoped List* op (ListReferences, ListReferenceImportJobs,
// ListRunTasks, ListWorkflowVersions, ...) makes once it has its child
// group and existence guard out of the way -- keeping each of those callers
// down to "guard, fetch group, listChildFiltered, return" is what keeps them
// under dupl's clone threshold despite sharing the same overall shape.
func listChildFiltered[T any](
	group []*T,
	keyFn func(*T) string,
	keep func(*T) bool,
	nextToken string,
	maxResults int,
	get func(string) (*T, bool),
) ([]*T, string) {
	ids := filterIDs(group, keyFn, keep)

	return paginatedCopies(ids, nextToken, maxResults, get)
}

// stringSet builds a lookup set from a (possibly nil) id list, used by the
// annotation/variant import job List operations' optional "ids" filter.
func stringSet(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}

	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}

	return set
}

// importJobMatchesFilter reports whether an annotation/variant import job
// satisfies the shared ImportJobFilter (status + owning store name) and, if
// idSet is non-nil, that its id is a member of the explicit "ids" list --
// mirrors real ListAnnotationImportJobs/ListVariantImportJobs body semantics.
func importJobMatchesFilter(status, storeName string, filter *ImportJobFilter, idSet map[string]bool, id string) bool {
	if idSet != nil && !idSet[id] {
		return false
	}

	if filter == nil {
		return true
	}

	return (filter.Status == "" || status == filter.Status) &&
		(filter.StoreName == "" || storeName == filter.StoreName)
}

// storeMatchesFilter reports whether an annotation/variant store or
// annotation store version satisfies the shared StoreStatusFilter (status
// only) and, if idSet is non-nil, that its id is a member of the explicit
// "ids" list -- mirrors real ListAnnotationStores/ListVariantStores/
// ListAnnotationStoreVersions body semantics.
func storeMatchesFilter(status, id string, filter *StoreStatusFilter, idSet map[string]bool) bool {
	if idSet != nil && !idSet[id] {
		return false
	}

	if filter == nil {
		return true
	}

	return filter.Status == "" || status == filter.Status
}

// shareMatchesFilter reports whether a share satisfies the ListShares body
// "filter" (real AWS types.Filter: resourceArns/status/type are each an "any
// of" list -- an empty list means the caller applied no constraint on that
// field).
func shareMatchesFilter(resourceARN, status, resourceType string, filter *ShareFilter) bool {
	if filter == nil {
		return true
	}

	if len(filter.ResourceArns) > 0 && !slices.Contains(filter.ResourceArns, resourceARN) {
		return false
	}

	if len(filter.Status) > 0 && !slices.Contains(filter.Status, status) {
		return false
	}

	if len(filter.Type) > 0 && !slices.Contains(filter.Type, resourceType) {
		return false
	}

	return true
}

func paginateStrings(ids []string, nextToken string, maxResults int) ([]string, string) {
	if maxResults <= 0 || maxResults > maxPageSize {
		maxResults = maxPageSize
	}

	start := 0

	if nextToken != "" {
		start = len(ids)

		for i, id := range ids {
			if id >= nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+maxResults, len(ids))
	page := ids[start:end]

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return page, outToken
}

// paginatedCopies sorts ids, paginates them via paginateStrings, then
// re-fetches and shallow-copies each page member via get. It is the shared
// tail of every List* operation below: collect candidate IDs (from a
// [store.Table.All] scan or a [store.Index.Get] group, optionally filtered),
// then hand off here for the identical sort/paginate/copy sequence every one
// of them performs.
func paginatedCopies[T any](
	ids []string,
	nextToken string,
	maxResults int,
	get func(string) (*T, bool),
) ([]*T, string) {
	sort.Strings(ids)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*T, 0, len(page))

	for _, id := range page {
		v, _ := get(id)
		cp := *v
		result = append(result, &cp)
	}

	return result, outToken
}
