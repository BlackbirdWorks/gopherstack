package omics

import (
	"context"
	"maps"
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

	maxPageSize = 100
	maxTags     = 200

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

func paginateStrings(ids []string, nextToken string, maxResults int) ([]string, string) {
	if maxResults <= 0 || maxResults > maxPageSize {
		maxResults = maxPageSize
	}

	start := 0

	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
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
