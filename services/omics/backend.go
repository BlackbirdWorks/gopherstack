package omics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// ────────────────────────────────────────────────────────────────────────────
// ReferenceStore
// ────────────────────────────────────────────────────────────────────────────

// CreateReferenceStore creates a new reference store.
func (b *InMemoryBackend) CreateReferenceStore(
	name, description string,
	tags map[string]string,
) (*ReferenceStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateReferenceStore")
	defer b.mu.Unlock()

	rs := &ReferenceStore{
		ID:           newID(),
		Name:         name,
		Description:  description,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	rs.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "referenceStore/"+rs.ID)

	b.referenceStores.Put(rs)
	b.referenceBytes[rs.ID] = make(map[string][]byte)

	if tags != nil {
		b.tags[rs.Arn] = copyTags(tags)
	}

	result := *rs

	return &result, nil
}

// DeleteReferenceStore deletes a reference store by ID.
func (b *InMemoryBackend) DeleteReferenceStore(id string) error {
	b.mu.Lock("DeleteReferenceStore")
	defer b.mu.Unlock()

	rs, ok := b.referenceStores.Get(id)
	if !ok {
		return fmt.Errorf("%w: reference store %s not found", ErrNotFound, id)
	}

	delete(b.tags, rs.Arn)
	b.referenceStores.Delete(id)

	for _, ref := range slices.Clone(b.referencesByStore.Get(id)) {
		b.references.Delete(parentKey(id, ref.ID))
	}

	for _, job := range slices.Clone(b.referenceImportJobsByStore.Get(id)) {
		b.referenceImportJobs.Delete(parentKey(id, job.ID))
	}

	delete(b.referenceBytes, id)

	return nil
}

// GetReferenceStore retrieves a reference store by ID.
func (b *InMemoryBackend) GetReferenceStore(id string) (*ReferenceStore, error) {
	b.mu.RLock("GetReferenceStore")
	defer b.mu.RUnlock()

	rs, ok := b.referenceStores.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: reference store %s not found", ErrNotFound, id)
	}

	result := *rs

	return &result, nil
}

// ListReferenceStores lists reference stores with optional name filter.
func (b *InMemoryBackend) ListReferenceStores(
	filter *ReferenceStoreFilter,
	maxResults int,
	nextToken string,
) ([]*ReferenceStore, string, error) {
	b.mu.RLock("ListReferenceStores")
	defer b.mu.RUnlock()

	all := b.referenceStores.All()
	ids := make([]string, 0, len(all))

	for _, rs := range all {
		if filter != nil && filter.Name != "" && rs.Name != filter.Name {
			continue
		}

		ids = append(ids, rs.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.referenceStores.Get)

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Reference
// ────────────────────────────────────────────────────────────────────────────

// DeleteReference deletes a reference by store ID and reference ID.
func (b *InMemoryBackend) DeleteReference(referenceStoreID, id string) error {
	b.mu.Lock("DeleteReference")
	defer b.mu.Unlock()

	if !b.referenceStores.Has(referenceStoreID) {
		return fmt.Errorf("%w: reference store %s not found", ErrNotFound, referenceStoreID)
	}

	ref, ok := b.references.Get(parentKey(referenceStoreID, id))
	if !ok {
		return fmt.Errorf("%w: reference %s not found", ErrNotFound, id)
	}

	delete(b.tags, ref.Arn)
	b.references.Delete(parentKey(referenceStoreID, id))

	return nil
}

// GetReferenceMetadata retrieves reference metadata.
func (b *InMemoryBackend) GetReferenceMetadata(
	referenceStoreID, id string,
) (*ReferenceMetadata, error) {
	b.mu.RLock("GetReferenceMetadata")
	defer b.mu.RUnlock()

	if !b.referenceStores.Has(referenceStoreID) {
		return nil, fmt.Errorf("%w: reference store %s not found", ErrNotFound, referenceStoreID)
	}

	ref, ok := b.references.Get(parentKey(referenceStoreID, id))
	if !ok {
		return nil, fmt.Errorf("%w: reference %s not found", ErrNotFound, id)
	}

	result := *ref

	return &result, nil
}

// ListReferences lists references in a reference store.
func (b *InMemoryBackend) ListReferences(
	referenceStoreID string,
	filter *ReferenceFilter,
	maxResults int,
	nextToken string,
) ([]*ReferenceMetadata, string, error) {
	b.mu.RLock("ListReferences")
	defer b.mu.RUnlock()

	if !b.referenceStores.Has(referenceStoreID) {
		return nil, "", fmt.Errorf(
			"%w: reference store %s not found",
			ErrNotFound,
			referenceStoreID,
		)
	}

	group := b.referencesByStore.Get(referenceStoreID)
	ids := make([]string, 0, len(group))

	for _, ref := range group {
		if filter != nil && filter.Name != "" && ref.Name != filter.Name {
			continue
		}

		ids = append(ids, ref.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReferenceMetadata, bool) {
		return b.references.Get(parentKey(referenceStoreID, id))
	})

	return result, outToken, nil
}

// StartReferenceImportJob creates a reference import job.
func (b *InMemoryBackend) StartReferenceImportJob(
	referenceStoreID, roleARN string,
	sources []ReferenceImportJobSource,
) (*ReferenceImportJob, error) {
	b.mu.Lock("StartReferenceImportJob")
	defer b.mu.Unlock()

	if !b.referenceStores.Has(referenceStoreID) {
		return nil, fmt.Errorf("%w: reference store %s not found", ErrNotFound, referenceStoreID)
	}

	job := &ReferenceImportJob{
		ID:               newID(),
		ReferenceStoreID: referenceStoreID,
		RoleARN:          roleARN,
		Sources:          sources,
		Status:           statusCompleted,
		CreationTime:     time.Now().UTC(),
	}
	now := time.Now().UTC()
	job.CompletionTime = &now

	// Create reference entries for each source
	for _, src := range sources {
		refID := newID()
		ref := &ReferenceMetadata{
			ID:               refID,
			ReferenceStoreID: referenceStoreID,
			Arn: arn.Build(
				"omics",
				b.defaultRegion,
				b.accountID,
				fmt.Sprintf("referenceStore/%s/reference/%s", referenceStoreID, refID),
			),
			Name:         src.Name,
			Description:  src.Description,
			Status:       statusActive,
			CreationTime: time.Now().UTC(),
			UpdateTime:   time.Now().UTC(),
		}
		b.references.Put(ref)
		b.referenceBytes[referenceStoreID][refID] = []byte{}
	}

	b.referenceImportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetReferenceImportJob retrieves a reference import job.
func (b *InMemoryBackend) GetReferenceImportJob(
	referenceStoreID, jobID string,
) (*ReferenceImportJob, error) {
	b.mu.RLock("GetReferenceImportJob")
	defer b.mu.RUnlock()

	if !b.referenceStores.Has(referenceStoreID) {
		return nil, fmt.Errorf("%w: reference store %s not found", ErrNotFound, referenceStoreID)
	}

	job, ok := b.referenceImportJobs.Get(parentKey(referenceStoreID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: reference import job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListReferenceImportJobs lists reference import jobs for a store.
func (b *InMemoryBackend) ListReferenceImportJobs(
	referenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*ReferenceImportJob, string, error) {
	b.mu.RLock("ListReferenceImportJobs")
	defer b.mu.RUnlock()

	if !b.referenceStores.Has(referenceStoreID) {
		return nil, "", fmt.Errorf(
			"%w: reference store %s not found",
			ErrNotFound,
			referenceStoreID,
		)
	}

	group := b.referenceImportJobsByStore.Get(referenceStoreID)
	ids := make([]string, 0, len(group))

	for _, j := range group {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReferenceImportJob, bool) {
		return b.referenceImportJobs.Get(parentKey(referenceStoreID, id))
	})

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// SequenceStore
// ────────────────────────────────────────────────────────────────────────────

// CreateSequenceStore creates a new sequence store.
func (b *InMemoryBackend) CreateSequenceStore(
	name, description string,
	tags map[string]string,
) (*SequenceStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateSequenceStore")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	ss := &SequenceStore{
		ID:           newID(),
		Name:         name,
		Description:  description,
		Status:       statusActive,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	ss.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "sequenceStore/"+ss.ID)

	b.sequenceStores.Put(ss)
	b.uploadParts[ss.ID] = make(map[string][]*ReadSetUploadPart)
	b.uploadPartData[ss.ID] = make(map[string]map[string]map[int][]byte)
	b.readSetBytes[ss.ID] = make(map[string][]byte)

	if tags != nil {
		b.tags[ss.Arn] = copyTags(tags)
	}

	result := *ss

	return &result, nil
}

// DeleteSequenceStore deletes a sequence store by ID.
func (b *InMemoryBackend) DeleteSequenceStore(id string) error {
	b.mu.Lock("DeleteSequenceStore")
	defer b.mu.Unlock()

	ss, ok := b.sequenceStores.Get(id)
	if !ok {
		return fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	delete(b.tags, ss.Arn)
	b.sequenceStores.Delete(id)

	for _, rs := range slices.Clone(b.readSetsByStore.Get(id)) {
		b.readSets.Delete(parentKey(id, rs.ID))
	}

	for _, j := range slices.Clone(b.readSetActivationJobsByStore.Get(id)) {
		b.readSetActivationJobs.Delete(parentKey(id, j.ID))
	}

	for _, j := range slices.Clone(b.readSetExportJobsByStore.Get(id)) {
		b.readSetExportJobs.Delete(parentKey(id, j.ID))
	}

	for _, j := range slices.Clone(b.readSetImportJobsByStore.Get(id)) {
		b.readSetImportJobs.Delete(parentKey(id, j.ID))
	}

	for _, u := range slices.Clone(b.multipartUploadsByStore.Get(id)) {
		b.multipartUploads.Delete(parentKey(id, u.UploadID))
	}

	delete(b.uploadParts, id)
	delete(b.uploadPartData, id)
	delete(b.readSetBytes, id)

	return nil
}

// GetSequenceStore retrieves a sequence store by ID.
func (b *InMemoryBackend) GetSequenceStore(id string) (*SequenceStore, error) {
	b.mu.RLock("GetSequenceStore")
	defer b.mu.RUnlock()

	ss, ok := b.sequenceStores.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	result := *ss

	return &result, nil
}

// ListSequenceStores lists sequence stores.
func (b *InMemoryBackend) ListSequenceStores(
	filter *SequenceStoreFilter,
	maxResults int,
	nextToken string,
) ([]*SequenceStore, string, error) {
	b.mu.RLock("ListSequenceStores")
	defer b.mu.RUnlock()

	all := b.sequenceStores.All()
	ids := make([]string, 0, len(all))

	for _, ss := range all {
		if filter != nil && filter.Name != "" && ss.Name != filter.Name {
			continue
		}

		ids = append(ids, ss.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.sequenceStores.Get)

	return result, outToken, nil
}

// UpdateSequenceStore updates a sequence store's name and description.
func (b *InMemoryBackend) UpdateSequenceStore(
	id, name, description string,
) (*SequenceStore, error) {
	b.mu.Lock("UpdateSequenceStore")
	defer b.mu.Unlock()

	ss, ok := b.sequenceStores.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	if name != "" {
		ss.Name = name
	}

	if description != "" {
		ss.Description = description
	}

	ss.UpdateTime = time.Now().UTC()
	result := *ss

	return &result, nil
}

// ────────────────────────────────────────────────────────────────────────────
// ReadSet
// ────────────────────────────────────────────────────────────────────────────

// BatchDeleteReadSet deletes multiple read sets.
func (b *InMemoryBackend) BatchDeleteReadSet(
	sequenceStoreID string,
	ids []string,
) ([]ReadSetBatchError, error) {
	b.mu.Lock("BatchDeleteReadSet")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	var errs []ReadSetBatchError

	for _, id := range ids {
		rs, ok := b.readSets.Get(parentKey(sequenceStoreID, id))
		if !ok {
			errs = append(errs, ReadSetBatchError{
				ID:      id,
				Code:    errResourceNotFound,
				Message: fmt.Sprintf("read set %s not found", id),
			})

			continue
		}

		delete(b.tags, rs.Arn)
		b.readSets.Delete(parentKey(sequenceStoreID, id))
	}

	return errs, nil
}

// GetReadSetMetadata retrieves read set metadata.
func (b *InMemoryBackend) GetReadSetMetadata(sequenceStoreID, id string) (*ReadSetMetadata, error) {
	b.mu.RLock("GetReadSetMetadata")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	rs, ok := b.readSets.Get(parentKey(sequenceStoreID, id))
	if !ok {
		return nil, fmt.Errorf("%w: read set %s not found", ErrNotFound, id)
	}

	result := *rs

	return &result, nil
}

// ListReadSets lists read sets in a sequence store.
func (b *InMemoryBackend) ListReadSets(
	sequenceStoreID string,
	filter *ReadSetFilter,
	maxResults int,
	nextToken string,
) ([]*ReadSetMetadata, string, error) {
	b.mu.RLock("ListReadSets")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, rs := range group {
		if filter != nil {
			if filter.Name != "" && rs.Name != filter.Name {
				continue
			}

			if filter.Status != "" && rs.Status != filter.Status {
				continue
			}
		}

		ids = append(ids, rs.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetMetadata, bool) {
		return b.readSets.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// StartReadSetActivationJob creates a read set activation job.
func (b *InMemoryBackend) StartReadSetActivationJob(
	sequenceStoreID string,
	sources []ReadSetActivationJobSource,
) (*ReadSetActivationJob, error) {
	b.mu.Lock("StartReadSetActivationJob")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job := &ReadSetActivationJob{
		ID:              newID(),
		SequenceStoreID: sequenceStoreID,
		Sources:         sources,
		Status:          statusCompleted,
		CreationTime:    time.Now().UTC(),
	}
	now := time.Now().UTC()
	job.CompletionTime = &now
	b.readSetActivationJobs.Put(job)

	result := *job

	return &result, nil
}

// GetReadSetActivationJob retrieves a read set activation job.
func (b *InMemoryBackend) GetReadSetActivationJob(
	sequenceStoreID, jobID string,
) (*ReadSetActivationJob, error) {
	b.mu.RLock("GetReadSetActivationJob")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := b.readSetActivationJobs.Get(parentKey(sequenceStoreID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: activation job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListReadSetActivationJobs lists read set activation jobs.
func (b *InMemoryBackend) ListReadSetActivationJobs(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetActivationJob, string, error) {
	b.mu.RLock("ListReadSetActivationJobs")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetActivationJobsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, j := range group {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetActivationJob, bool) {
		return b.readSetActivationJobs.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// StartReadSetExportJob creates a read set export job.
func (b *InMemoryBackend) StartReadSetExportJob(
	sequenceStoreID, destination string,
	sources []ReadSetExportJobSource,
) (*ReadSetExportJob, error) {
	b.mu.Lock("StartReadSetExportJob")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job := &ReadSetExportJob{
		ID:              newID(),
		SequenceStoreID: sequenceStoreID,
		Destination:     destination,
		Sources:         sources,
		Status:          statusCompleted,
		CreationTime:    time.Now().UTC(),
	}
	now := time.Now().UTC()
	job.CompletionTime = &now
	b.readSetExportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetReadSetExportJob retrieves a read set export job.
func (b *InMemoryBackend) GetReadSetExportJob(
	sequenceStoreID, jobID string,
) (*ReadSetExportJob, error) {
	b.mu.RLock("GetReadSetExportJob")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := b.readSetExportJobs.Get(parentKey(sequenceStoreID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: export job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListReadSetExportJobs lists read set export jobs.
func (b *InMemoryBackend) ListReadSetExportJobs(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetExportJob, string, error) {
	b.mu.RLock("ListReadSetExportJobs")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetExportJobsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, j := range group {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetExportJob, bool) {
		return b.readSetExportJobs.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// StartReadSetImportJob creates a read set import job.
func (b *InMemoryBackend) StartReadSetImportJob(
	sequenceStoreID, roleARN string,
	sources []ReadSetImportJobSource,
) (*ReadSetImportJob, error) {
	b.mu.Lock("StartReadSetImportJob")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job := &ReadSetImportJob{
		ID:              newID(),
		SequenceStoreID: sequenceStoreID,
		RoleARN:         roleARN,
		Sources:         sources,
		Status:          statusCompleted,
		CreationTime:    time.Now().UTC(),
	}
	now := time.Now().UTC()
	job.CompletionTime = &now

	// Create read set entries for each source
	for _, src := range sources {
		rsID := newID()
		rs := &ReadSetMetadata{
			ID:              rsID,
			SequenceStoreID: sequenceStoreID,
			Arn: arn.Build(
				"omics",
				b.defaultRegion,
				b.accountID,
				fmt.Sprintf("sequenceStore/%s/readSet/%s", sequenceStoreID, rsID),
			),
			Name:         src.Name,
			Description:  src.Description,
			SequenceType: src.SourceFileType,
			SubjectID:    src.SubjectID,
			SampleID:     src.SampleID,
			ReferenceARN: src.ReferenceARN,
			Status:       statusActive,
			CreationTime: time.Now().UTC(),
		}
		b.readSets.Put(rs)
	}

	b.readSetImportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetReadSetImportJob retrieves a read set import job.
func (b *InMemoryBackend) GetReadSetImportJob(
	sequenceStoreID, jobID string,
) (*ReadSetImportJob, error) {
	b.mu.RLock("GetReadSetImportJob")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := b.readSetImportJobs.Get(parentKey(sequenceStoreID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: import job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListReadSetImportJobs lists read set import jobs.
func (b *InMemoryBackend) ListReadSetImportJobs(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetImportJob, string, error) {
	b.mu.RLock("ListReadSetImportJobs")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetImportJobsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, j := range group {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetImportJob, bool) {
		return b.readSetImportJobs.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Multipart Upload
// ────────────────────────────────────────────────────────────────────────────

// CreateMultipartReadSetUpload creates a multipart read set upload.
func (b *InMemoryBackend) CreateMultipartReadSetUpload(
	sequenceStoreID, name, sequenceType string,
	tags map[string]string,
) (*MultipartReadSetUpload, error) {
	b.mu.Lock("CreateMultipartReadSetUpload")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	upload := &MultipartReadSetUpload{
		UploadID:        newID(),
		SequenceStoreID: sequenceStoreID,
		Name:            name,
		SequenceType:    sequenceType,
		Tags:            copyTags(tags),
		Status:          "IN_PROGRESS",
		CreationTime:    time.Now().UTC(),
	}
	b.multipartUploads.Put(upload)
	b.uploadParts[sequenceStoreID][upload.UploadID] = nil
	b.uploadPartData[sequenceStoreID][upload.UploadID] = make(map[string]map[int][]byte)

	result := *upload

	return &result, nil
}

// AbortMultipartReadSetUpload aborts a multipart read set upload.
func (b *InMemoryBackend) AbortMultipartReadSetUpload(sequenceStoreID, uploadID string) error {
	b.mu.Lock("AbortMultipartReadSetUpload")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if !b.multipartUploads.Has(parentKey(sequenceStoreID, uploadID)) {
		return fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	b.multipartUploads.Delete(parentKey(sequenceStoreID, uploadID))
	delete(b.uploadParts[sequenceStoreID], uploadID)
	delete(b.uploadPartData[sequenceStoreID], uploadID)

	return nil
}

// CompleteMultipartReadSetUpload completes a multipart read set upload.
func (b *InMemoryBackend) CompleteMultipartReadSetUpload(
	sequenceStoreID, uploadID string,
) (*ReadSetMetadata, error) {
	b.mu.Lock("CompleteMultipartReadSetUpload")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	upload, ok := b.multipartUploads.Get(parentKey(sequenceStoreID, uploadID))
	if !ok {
		return nil, fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	rsID := newID()
	rs := &ReadSetMetadata{
		ID:              rsID,
		SequenceStoreID: sequenceStoreID,
		Arn: arn.Build(
			"omics",
			b.defaultRegion,
			b.accountID,
			fmt.Sprintf("sequenceStore/%s/readSet/%s", sequenceStoreID, rsID),
		),
		Name:         upload.Name,
		SequenceType: upload.SequenceType,
		Status:       statusActive,
		CreationTime: time.Now().UTC(),
		Tags:         maps.Clone(upload.Tags),
	}
	b.readSets.Put(rs)

	// Concatenate stored part bytes (SOURCE1 then SOURCE2, in part-number order) as the read set body.
	partData := b.uploadPartData[sequenceStoreID][uploadID]
	var combined []byte
	for _, src := range []string{"SOURCE1", "SOURCE2"} {
		srcParts := partData[src]
		partNums := make([]int, 0, len(srcParts))
		for n := range srcParts {
			partNums = append(partNums, n)
		}
		sort.Ints(partNums)
		for _, n := range partNums {
			combined = append(combined, srcParts[n]...)
		}
	}
	b.readSetBytes[sequenceStoreID][rsID] = combined

	b.multipartUploads.Delete(parentKey(sequenceStoreID, uploadID))
	delete(b.uploadParts[sequenceStoreID], uploadID)
	delete(b.uploadPartData[sequenceStoreID], uploadID)

	result := *rs

	return &result, nil
}

// ListMultipartReadSetUploads lists in-progress multipart uploads.
func (b *InMemoryBackend) ListMultipartReadSetUploads(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*MultipartReadSetUpload, string, error) {
	b.mu.RLock("ListMultipartReadSetUploads")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.multipartUploadsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, u := range group {
		ids = append(ids, u.UploadID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*MultipartReadSetUpload, bool) {
		return b.multipartUploads.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// ListReadSetUploadParts lists parts for a multipart read set upload.
func (b *InMemoryBackend) ListReadSetUploadParts(
	sequenceStoreID, uploadID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetUploadPart, string, error) {
	b.mu.RLock("ListReadSetUploadParts")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if !b.multipartUploads.Has(parentKey(sequenceStoreID, uploadID)) {
		return nil, "", fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	parts := b.uploadParts[sequenceStoreID][uploadID]

	if maxResults <= 0 || maxResults > maxPageSize {
		maxResults = maxPageSize
	}

	start := 0

	if nextToken != "" {
		for i, p := range parts {
			if strconv.Itoa(p.PartNumber) == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+maxResults, len(parts))
	page := parts[start:end]

	var outToken string
	if end < len(parts) {
		outToken = strconv.Itoa(parts[end].PartNumber)
	}

	result := make([]*ReadSetUploadPart, len(page))

	for i, p := range page {
		cp := *p
		result[i] = &cp
	}

	return result, outToken, nil
}

// UploadReadSetPart stores binary data for a single part and returns its SHA256 checksum.
func (b *InMemoryBackend) UploadReadSetPart(
	sequenceStoreID, uploadID string,
	partNumber int,
	partSource string,
	data []byte,
) (string, error) {
	b.mu.Lock("UploadReadSetPart")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if !b.multipartUploads.Has(parentKey(sequenceStoreID, uploadID)) {
		return "", fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	if b.uploadPartData[sequenceStoreID][uploadID] == nil {
		b.uploadPartData[sequenceStoreID][uploadID] = make(map[string]map[int][]byte)
	}
	if b.uploadPartData[sequenceStoreID][uploadID][partSource] == nil {
		b.uploadPartData[sequenceStoreID][uploadID][partSource] = make(map[int][]byte)
	}
	b.uploadPartData[sequenceStoreID][uploadID][partSource][partNumber] = data

	// Update or append the upload part metadata.
	parts := b.uploadParts[sequenceStoreID][uploadID]
	found := false
	for _, p := range parts {
		if p.PartNumber == partNumber && p.Source == partSource {
			p.PartSize = int64(len(data))
			p.LastUpdatedTime = time.Now().UTC()
			found = true

			break
		}
	}

	if !found {
		parts = append(parts, &ReadSetUploadPart{
			PartNumber:      partNumber,
			Source:          partSource,
			PartSize:        int64(len(data)),
			LastUpdatedTime: time.Now().UTC(),
		})
		b.uploadParts[sequenceStoreID][uploadID] = parts
	}

	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:]), nil
}

// GetReadSetBytes returns the stored binary body for a read set.
func (b *InMemoryBackend) GetReadSetBytes(sequenceStoreID, id string) ([]byte, error) {
	b.mu.RLock("GetReadSetBytes")
	defer b.mu.RUnlock()

	if !b.readSets.Has(parentKey(sequenceStoreID, id)) {
		return nil, fmt.Errorf("%w: read set %s not found", ErrNotFound, id)
	}

	return b.readSetBytes[sequenceStoreID][id], nil
}

// GetReferenceBytes returns the stored binary body for a reference.
func (b *InMemoryBackend) GetReferenceBytes(referenceStoreID, id string) ([]byte, error) {
	b.mu.RLock("GetReferenceBytes")
	defer b.mu.RUnlock()

	if !b.references.Has(parentKey(referenceStoreID, id)) {
		return nil, fmt.Errorf("%w: reference %s not found", ErrNotFound, id)
	}

	return b.referenceBytes[referenceStoreID][id], nil
}

// ────────────────────────────────────────────────────────────────────────────
// RunGroup
// ────────────────────────────────────────────────────────────────────────────

// CreateRunGroup creates a new run group.
func (b *InMemoryBackend) CreateRunGroup(
	name string,
	maxCPUs, maxRuns, maxDuration int,
	maxGPUs int,
	tags map[string]string,
) (*RunGroup, error) {
	b.mu.Lock("CreateRunGroup")
	defer b.mu.Unlock()

	id := newID()
	rg := &RunGroup{
		ID:           id,
		Name:         name,
		MaxCPUs:      maxCPUs,
		MaxRuns:      maxRuns,
		MaxDuration:  maxDuration,
		MaxGPUs:      maxGPUs,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	rg.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "runGroup/"+id)

	b.runGroups.Put(rg)

	if tags != nil {
		b.tags[rg.Arn] = copyTags(tags)
	}

	result := *rg

	return &result, nil
}

// DeleteRunGroup deletes a run group.
func (b *InMemoryBackend) DeleteRunGroup(id string) error {
	b.mu.Lock("DeleteRunGroup")
	defer b.mu.Unlock()

	rg, ok := b.runGroups.Get(id)
	if !ok {
		return fmt.Errorf("%w: run group %s not found", ErrNotFound, id)
	}

	delete(b.tags, rg.Arn)
	b.runGroups.Delete(id)

	return nil
}

// GetRunGroup retrieves a run group.
func (b *InMemoryBackend) GetRunGroup(id string) (*RunGroup, error) {
	b.mu.RLock("GetRunGroup")
	defer b.mu.RUnlock()

	rg, ok := b.runGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run group %s not found", ErrNotFound, id)
	}

	result := *rg

	return &result, nil
}

// ListRunGroups lists run groups.
func (b *InMemoryBackend) ListRunGroups(
	maxResults int,
	nextToken string,
) ([]*RunGroup, string, error) {
	b.mu.RLock("ListRunGroups")
	defer b.mu.RUnlock()

	all := b.runGroups.All()
	ids := make([]string, 0, len(all))

	for _, rg := range all {
		ids = append(ids, rg.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runGroups.Get)

	return result, outToken, nil
}

// UpdateRunGroup updates a run group.
func (b *InMemoryBackend) UpdateRunGroup(
	id, name string,
	maxCPUs, maxRuns, maxDuration int,
	maxGPUs int,
) (*RunGroup, error) {
	b.mu.Lock("UpdateRunGroup")
	defer b.mu.Unlock()

	rg, ok := b.runGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run group %s not found", ErrNotFound, id)
	}

	if name != "" {
		rg.Name = name
	}

	if maxCPUs > 0 {
		rg.MaxCPUs = maxCPUs
	}

	if maxRuns > 0 {
		rg.MaxRuns = maxRuns
	}

	if maxDuration > 0 {
		rg.MaxDuration = maxDuration
	}

	if maxGPUs > 0 {
		rg.MaxGPUs = maxGPUs
	}

	result := *rg

	return &result, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Run
// ────────────────────────────────────────────────────────────────────────────

// StartRun starts a new workflow run.
func (b *InMemoryBackend) StartRun(
	workflowID, roleARN, name, runBatchID string,
	params map[string]any,
	tags map[string]string,
) (*Run, error) {
	b.mu.Lock("StartRun")
	defer b.mu.Unlock()

	id := newID()
	now := time.Now().UTC()
	run := &Run{
		ID:           id,
		Name:         name,
		WorkflowID:   workflowID,
		RoleARN:      roleARN,
		RunBatchID:   runBatchID,
		Params:       params,
		Tags:         copyTags(tags),
		Status:       statusPending,
		CreationTime: now,
	}
	run.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "run/"+id)

	b.runs.Put(run)

	taskID := newID()
	b.runTasks.Put(&RunTask{
		TaskID:       taskID,
		RunID:        id,
		Name:         "task-1",
		Status:       statusPending,
		CPUs:         stubTaskCPUs,
		Memory:       stubTaskMemory,
		CreationTime: now,
	})

	if tags != nil {
		b.tags[run.Arn] = copyTags(tags)
	}

	result := *run

	return &result, nil
}

// CancelRun cancels a run.
func (b *InMemoryBackend) CancelRun(id string) error {
	b.mu.Lock("CancelRun")
	defer b.mu.Unlock()

	run, ok := b.runs.Get(id)
	if !ok {
		return fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	if run.Status == statusCompleted || run.Status == statusCancelled || run.Status == statusFailed {
		return fmt.Errorf("%w: run %s is already in terminal state %s", ErrValidation, id, run.Status)
	}

	run.Status = statusCancelled

	return nil
}

// DeleteRun deletes a run.
func (b *InMemoryBackend) DeleteRun(id string) error {
	b.mu.Lock("DeleteRun")
	defer b.mu.Unlock()

	run, ok := b.runs.Get(id)
	if !ok {
		return fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	delete(b.tags, run.Arn)
	b.runs.Delete(id)

	for _, t := range slices.Clone(b.runTasksByRun.Get(id)) {
		b.runTasks.Delete(parentKey(id, t.TaskID))
	}

	return nil
}

// GetRun retrieves a run.
func (b *InMemoryBackend) GetRun(id string) (*Run, error) {
	b.mu.RLock("GetRun")
	defer b.mu.RUnlock()

	run, ok := b.runs.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	result := *run

	return &result, nil
}

// ListRuns lists runs.
func (b *InMemoryBackend) ListRuns(maxResults int, nextToken string) ([]*Run, string, error) {
	b.mu.RLock("ListRuns")
	defer b.mu.RUnlock()

	all := b.runs.All()
	ids := make([]string, 0, len(all))

	for _, r := range all {
		ids = append(ids, r.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runs.Get)

	return result, outToken, nil
}

// GetRunTask retrieves a task within a run.
func (b *InMemoryBackend) GetRunTask(runID, taskID string) (*RunTask, error) {
	b.mu.RLock("GetRunTask")
	defer b.mu.RUnlock()

	if !b.runs.Has(runID) {
		return nil, fmt.Errorf("%w: run %s not found", ErrNotFound, runID)
	}

	task, ok := b.runTasks.Get(parentKey(runID, taskID))
	if !ok {
		return nil, fmt.Errorf("%w: task %s not found", ErrNotFound, taskID)
	}

	result := *task

	return &result, nil
}

// ListRunTasks lists tasks within a run.
func (b *InMemoryBackend) ListRunTasks(
	runID string,
	maxResults int,
	nextToken string,
) ([]*RunTask, string, error) {
	b.mu.RLock("ListRunTasks")
	defer b.mu.RUnlock()

	if !b.runs.Has(runID) {
		return nil, "", fmt.Errorf("%w: run %s not found", ErrNotFound, runID)
	}

	group := b.runTasksByRun.Get(runID)
	ids := make([]string, 0, len(group))

	for _, t := range group {
		ids = append(ids, t.TaskID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*RunTask, bool) {
		return b.runTasks.Get(parentKey(runID, id))
	})

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Workflow
// ────────────────────────────────────────────────────────────────────────────

// CreateWorkflow creates a new workflow.
func (b *InMemoryBackend) CreateWorkflow(
	name, description, _ /* definitionZip */, _ /* definitionURI */, engine string,
	tags map[string]string,
) (*Workflow, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateWorkflow")
	defer b.mu.Unlock()

	id := newID()
	wf := &Workflow{
		ID:           id,
		Name:         name,
		Description:  description,
		Engine:       engine,
		Type:         "PRIVATE",
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	wf.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "workflow/"+id)

	b.workflows.Put(wf)

	if tags != nil {
		b.tags[wf.Arn] = copyTags(tags)
	}

	result := *wf

	return &result, nil
}

// DeleteWorkflow deletes a workflow.
func (b *InMemoryBackend) DeleteWorkflow(id string) error {
	b.mu.Lock("DeleteWorkflow")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(id)
	if !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, id)
	}

	delete(b.tags, wf.Arn)
	b.workflows.Delete(id)

	for _, v := range slices.Clone(b.workflowVersionsByWorkflow.Get(id)) {
		b.workflowVersions.Delete(parentKey(id, v.VersionName))
	}

	return nil
}

// GetWorkflow retrieves a workflow.
func (b *InMemoryBackend) GetWorkflow(id string) (*Workflow, error) {
	b.mu.RLock("GetWorkflow")
	defer b.mu.RUnlock()

	wf, ok := b.workflows.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, id)
	}

	result := *wf

	return &result, nil
}

// ListWorkflows lists workflows.
func (b *InMemoryBackend) ListWorkflows(
	maxResults int,
	nextToken string,
) ([]*Workflow, string, error) {
	b.mu.RLock("ListWorkflows")
	defer b.mu.RUnlock()

	all := b.workflows.All()
	ids := make([]string, 0, len(all))

	for _, wf := range all {
		ids = append(ids, wf.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.workflows.Get)

	return result, outToken, nil
}

// UpdateWorkflow updates a workflow.
func (b *InMemoryBackend) UpdateWorkflow(id, name, description string) error {
	b.mu.Lock("UpdateWorkflow")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(id)
	if !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, id)
	}

	if name != "" {
		wf.Name = name
	}

	if description != "" {
		wf.Description = description
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// WorkflowVersion
// ────────────────────────────────────────────────────────────────────────────

// CreateWorkflowVersion creates a new workflow version.
func (b *InMemoryBackend) CreateWorkflowVersion(
	workflowID, versionName, description string,
	tags map[string]string,
) (*WorkflowVersion, error) {
	b.mu.Lock("CreateWorkflowVersion")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(workflowID)
	if !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	if b.workflowVersions.Has(parentKey(workflowID, versionName)) {
		return nil, fmt.Errorf(
			"%w: workflow version %s already exists",
			ErrAlreadyExists,
			versionName,
		)
	}

	wv := &WorkflowVersion{
		WorkflowID:   workflowID,
		VersionName:  versionName,
		Description:  description,
		Engine:       wf.Engine,
		Type:         wf.Type,
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	wv.Arn = arn.Build(
		"omics",
		b.defaultRegion,
		b.accountID,
		fmt.Sprintf("workflow/%s/version/%s", workflowID, versionName),
	)

	b.workflowVersions.Put(wv)

	if tags != nil {
		b.tags[wv.Arn] = copyTags(tags)
	}

	result := *wv

	return &result, nil
}

// DeleteWorkflowVersion deletes a workflow version.
func (b *InMemoryBackend) DeleteWorkflowVersion(workflowID, versionName string) error {
	b.mu.Lock("DeleteWorkflowVersion")
	defer b.mu.Unlock()

	if !b.workflows.Has(workflowID) {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := b.workflowVersions.Get(parentKey(workflowID, versionName))
	if !ok {
		return fmt.Errorf("%w: workflow version %s not found", ErrNotFound, versionName)
	}

	delete(b.tags, wv.Arn)
	b.workflowVersions.Delete(parentKey(workflowID, versionName))

	return nil
}

// GetWorkflowVersion retrieves a workflow version.
func (b *InMemoryBackend) GetWorkflowVersion(
	workflowID, versionName string,
) (*WorkflowVersion, error) {
	b.mu.RLock("GetWorkflowVersion")
	defer b.mu.RUnlock()

	if !b.workflows.Has(workflowID) {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := b.workflowVersions.Get(parentKey(workflowID, versionName))
	if !ok {
		return nil, fmt.Errorf("%w: workflow version %s not found", ErrNotFound, versionName)
	}

	result := *wv

	return &result, nil
}

// ListWorkflowVersions lists versions of a workflow.
func (b *InMemoryBackend) ListWorkflowVersions(
	workflowID string,
	maxResults int,
	nextToken string,
) ([]*WorkflowVersion, string, error) {
	b.mu.RLock("ListWorkflowVersions")
	defer b.mu.RUnlock()

	if !b.workflows.Has(workflowID) {
		return nil, "", fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	group := b.workflowVersionsByWorkflow.Get(workflowID)
	names := make([]string, 0, len(group))

	for _, wv := range group {
		names = append(names, wv.VersionName)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, func(id string) (*WorkflowVersion, bool) {
		return b.workflowVersions.Get(parentKey(workflowID, id))
	})

	return result, outToken, nil
}

// UpdateWorkflowVersion updates a workflow version.
func (b *InMemoryBackend) UpdateWorkflowVersion(workflowID, versionName, description string) error {
	b.mu.Lock("UpdateWorkflowVersion")
	defer b.mu.Unlock()

	if !b.workflows.Has(workflowID) {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := b.workflowVersions.Get(parentKey(workflowID, versionName))
	if !ok {
		return fmt.Errorf("%w: workflow version %s not found", ErrNotFound, versionName)
	}

	if description != "" {
		wv.Description = description
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// AnnotationStore
// ────────────────────────────────────────────────────────────────────────────

// CreateAnnotationStore creates a new annotation store.
func (b *InMemoryBackend) CreateAnnotationStore(
	name, storeFormat string,
	reference, sseConfig, storeOptions map[string]any,
	tags map[string]string,
) (*AnnotationStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateAnnotationStore")
	defer b.mu.Unlock()

	if b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	as := &AnnotationStore{
		ID:           newID(),
		Name:         name,
		StoreFormat:  storeFormat,
		Reference:    reference,
		SseConfig:    sseConfig,
		StoreOptions: storeOptions,
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	as.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "annotationStore/"+name)
	b.annotationStores.Put(as)

	if tags != nil {
		b.tags[as.Arn] = copyTags(tags)
	}

	result := *as

	return &result, nil
}

// DeleteAnnotationStore deletes an annotation store.
func (b *InMemoryBackend) DeleteAnnotationStore(name string) (*AnnotationStore, error) {
	b.mu.Lock("DeleteAnnotationStore")
	defer b.mu.Unlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	delete(b.tags, as.Arn)
	b.annotationStores.Delete(name)

	for _, v := range slices.Clone(b.annotationVersionsByStore.Get(name)) {
		b.annotationVersions.Delete(parentKey(name, v.VersionName))
	}

	result := *as
	result.Status = statusDeleting

	return &result, nil
}

// GetAnnotationStore retrieves an annotation store.
func (b *InMemoryBackend) GetAnnotationStore(name string) (*AnnotationStore, error) {
	b.mu.RLock("GetAnnotationStore")
	defer b.mu.RUnlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	result := *as

	return &result, nil
}

// ListAnnotationStores lists annotation stores.
func (b *InMemoryBackend) ListAnnotationStores(
	maxResults int,
	nextToken string,
) ([]*AnnotationStore, string, error) {
	b.mu.RLock("ListAnnotationStores")
	defer b.mu.RUnlock()

	all := b.annotationStores.All()
	names := make([]string, 0, len(all))

	for _, as := range all {
		names = append(names, as.Name)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, b.annotationStores.Get)

	return result, outToken, nil
}

// UpdateAnnotationStore updates an annotation store.
func (b *InMemoryBackend) UpdateAnnotationStore(
	name, description string,
) (*AnnotationStore, error) {
	b.mu.Lock("UpdateAnnotationStore")
	defer b.mu.Unlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	if description != "" {
		as.Description = description
	}

	as.UpdateTime = time.Now().UTC()
	result := *as

	return &result, nil
}

// StartAnnotationImportJob starts an annotation import job.
func (b *InMemoryBackend) StartAnnotationImportJob(
	destinationName, roleARN string,
	items []AnnotationImportItem,
) (*AnnotationImportJob, error) {
	b.mu.Lock("StartAnnotationImportJob")
	defer b.mu.Unlock()

	if !b.annotationStores.Has(destinationName) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, destinationName)
	}

	now := time.Now().UTC()
	job := &AnnotationImportJob{
		ID:              newID(),
		DestinationName: destinationName,
		RoleARN:         roleARN,
		Items:           items,
		Status:          statusCompleted,
		CreationTime:    now,
		CompletionTime:  &now,
	}
	b.annotationImportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetAnnotationImportJob retrieves an annotation import job.
func (b *InMemoryBackend) GetAnnotationImportJob(jobID string) (*AnnotationImportJob, error) {
	b.mu.RLock("GetAnnotationImportJob")
	defer b.mu.RUnlock()

	job, ok := b.annotationImportJobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: annotation import job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListAnnotationImportJobs lists annotation import jobs.
func (b *InMemoryBackend) ListAnnotationImportJobs(
	maxResults int,
	nextToken string,
) ([]*AnnotationImportJob, string, error) {
	b.mu.RLock("ListAnnotationImportJobs")
	defer b.mu.RUnlock()

	all := b.annotationImportJobs.All()
	ids := make([]string, 0, len(all))

	for _, j := range all {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.annotationImportJobs.Get)

	return result, outToken, nil
}

// CancelAnnotationImportJob cancels an annotation import job.
func (b *InMemoryBackend) CancelAnnotationImportJob(jobID string) error {
	b.mu.Lock("CancelAnnotationImportJob")
	defer b.mu.Unlock()

	job, ok := b.annotationImportJobs.Get(jobID)
	if !ok {
		return fmt.Errorf("%w: annotation import job %s not found", ErrNotFound, jobID)
	}

	job.Status = statusCancelled

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// AnnotationStoreVersion
// ────────────────────────────────────────────────────────────────────────────

// CreateAnnotationStoreVersion creates a version of an annotation store.
func (b *InMemoryBackend) CreateAnnotationStoreVersion(
	name, versionName, description string,
	tags map[string]string,
) (*AnnotationStoreVersion, error) {
	b.mu.Lock("CreateAnnotationStoreVersion")
	defer b.mu.Unlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	if b.annotationVersions.Has(parentKey(name, versionName)) {
		return nil, fmt.Errorf(
			"%w: annotation store version %s already exists",
			ErrAlreadyExists,
			versionName,
		)
	}

	now := time.Now().UTC()
	v := &AnnotationStoreVersion{
		StoreID:      as.ID,
		StoreName:    name,
		VersionName:  versionName,
		Description:  description,
		Status:       statusActive,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	v.Arn = arn.Build(
		"omics",
		b.defaultRegion,
		b.accountID,
		fmt.Sprintf("annotationStore/%s/version/%s", name, versionName),
	)
	b.annotationVersions.Put(v)

	if tags != nil {
		b.tags[v.Arn] = copyTags(tags)
	}

	result := *v

	return &result, nil
}

// DeleteAnnotationStoreVersions deletes one or more annotation store versions.
func (b *InMemoryBackend) DeleteAnnotationStoreVersions(
	name string,
	versionNames []string,
) ([]VersionDeleteError, error) {
	b.mu.Lock("DeleteAnnotationStoreVersions")
	defer b.mu.Unlock()

	if !b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	var errs []VersionDeleteError

	for _, vn := range versionNames {
		v, ok := b.annotationVersions.Get(parentKey(name, vn))
		if !ok {
			errs = append(errs, VersionDeleteError{
				VersionName: vn,
				Code:        errResourceNotFound,
				Message:     fmt.Sprintf("version %s not found", vn),
			})

			continue
		}

		delete(b.tags, v.Arn)
		b.annotationVersions.Delete(parentKey(name, vn))
	}

	return errs, nil
}

// GetAnnotationStoreVersion retrieves an annotation store version.
func (b *InMemoryBackend) GetAnnotationStoreVersion(
	name, versionName string,
) (*AnnotationStoreVersion, error) {
	b.mu.RLock("GetAnnotationStoreVersion")
	defer b.mu.RUnlock()

	if !b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	v, ok := b.annotationVersions.Get(parentKey(name, versionName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: annotation store version %s not found",
			ErrNotFound,
			versionName,
		)
	}

	result := *v

	return &result, nil
}

// ListAnnotationStoreVersions lists versions of an annotation store.
func (b *InMemoryBackend) ListAnnotationStoreVersions(
	name string,
	maxResults int,
	nextToken string,
) ([]*AnnotationStoreVersion, string, error) {
	b.mu.RLock("ListAnnotationStoreVersions")
	defer b.mu.RUnlock()

	if !b.annotationStores.Has(name) {
		return nil, "", fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	group := b.annotationVersionsByStore.Get(name)
	names := make([]string, 0, len(group))

	for _, v := range group {
		names = append(names, v.VersionName)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, func(id string) (*AnnotationStoreVersion, bool) {
		return b.annotationVersions.Get(parentKey(name, id))
	})

	return result, outToken, nil
}

// UpdateAnnotationStoreVersion updates an annotation store version.
func (b *InMemoryBackend) UpdateAnnotationStoreVersion(
	name, versionName, description string,
) (*AnnotationStoreVersion, error) {
	b.mu.Lock("UpdateAnnotationStoreVersion")
	defer b.mu.Unlock()

	if !b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	v, ok := b.annotationVersions.Get(parentKey(name, versionName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: annotation store version %s not found",
			ErrNotFound,
			versionName,
		)
	}

	if description != "" {
		v.Description = description
	}

	v.UpdateTime = time.Now().UTC()
	result := *v

	return &result, nil
}

// ────────────────────────────────────────────────────────────────────────────
// VariantStore
// ────────────────────────────────────────────────────────────────────────────

// CreateVariantStore creates a new variant store.
func (b *InMemoryBackend) CreateVariantStore(
	name string,
	reference map[string]any,
	tags map[string]string,
) (*VariantStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateVariantStore")
	defer b.mu.Unlock()

	if b.variantStores.Has(name) {
		return nil, fmt.Errorf("%w: variant store %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	vs := &VariantStore{
		ID:           newID(),
		Name:         name,
		Reference:    reference,
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	vs.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "variantStore/"+name)
	b.variantStores.Put(vs)

	if tags != nil {
		b.tags[vs.Arn] = copyTags(tags)
	}

	result := *vs

	return &result, nil
}

// DeleteVariantStore deletes a variant store.
func (b *InMemoryBackend) DeleteVariantStore(name string) (*VariantStore, error) {
	b.mu.Lock("DeleteVariantStore")
	defer b.mu.Unlock()

	vs, ok := b.variantStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, name)
	}

	delete(b.tags, vs.Arn)
	b.variantStores.Delete(name)

	result := *vs
	result.Status = statusDeleting

	return &result, nil
}

// GetVariantStore retrieves a variant store.
func (b *InMemoryBackend) GetVariantStore(name string) (*VariantStore, error) {
	b.mu.RLock("GetVariantStore")
	defer b.mu.RUnlock()

	vs, ok := b.variantStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, name)
	}

	result := *vs

	return &result, nil
}

// ListVariantStores lists variant stores.
func (b *InMemoryBackend) ListVariantStores(
	maxResults int,
	nextToken string,
) ([]*VariantStore, string, error) {
	b.mu.RLock("ListVariantStores")
	defer b.mu.RUnlock()

	all := b.variantStores.All()
	names := make([]string, 0, len(all))

	for _, vs := range all {
		names = append(names, vs.Name)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, b.variantStores.Get)

	return result, outToken, nil
}

// UpdateVariantStore updates a variant store.
func (b *InMemoryBackend) UpdateVariantStore(name, description string) (*VariantStore, error) {
	b.mu.Lock("UpdateVariantStore")
	defer b.mu.Unlock()

	vs, ok := b.variantStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, name)
	}

	if description != "" {
		vs.Description = description
	}

	vs.UpdateTime = time.Now().UTC()
	result := *vs

	return &result, nil
}

// StartVariantImportJob starts a variant import job.
func (b *InMemoryBackend) StartVariantImportJob(
	destinationName, roleARN string,
	items []VariantImportItem,
) (*VariantImportJob, error) {
	b.mu.Lock("StartVariantImportJob")
	defer b.mu.Unlock()

	if !b.variantStores.Has(destinationName) {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, destinationName)
	}

	now := time.Now().UTC()
	job := &VariantImportJob{
		ID:              newID(),
		DestinationName: destinationName,
		RoleARN:         roleARN,
		Items:           items,
		Status:          statusCompleted,
		CreationTime:    now,
		CompletionTime:  &now,
	}
	b.variantImportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetVariantImportJob retrieves a variant import job.
func (b *InMemoryBackend) GetVariantImportJob(jobID string) (*VariantImportJob, error) {
	b.mu.RLock("GetVariantImportJob")
	defer b.mu.RUnlock()

	job, ok := b.variantImportJobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: variant import job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListVariantImportJobs lists variant import jobs.
func (b *InMemoryBackend) ListVariantImportJobs(
	maxResults int,
	nextToken string,
) ([]*VariantImportJob, string, error) {
	b.mu.RLock("ListVariantImportJobs")
	defer b.mu.RUnlock()

	all := b.variantImportJobs.All()
	ids := make([]string, 0, len(all))

	for _, j := range all {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.variantImportJobs.Get)

	return result, outToken, nil
}

// CancelVariantImportJob cancels a variant import job.
func (b *InMemoryBackend) CancelVariantImportJob(jobID string) error {
	b.mu.Lock("CancelVariantImportJob")
	defer b.mu.Unlock()

	job, ok := b.variantImportJobs.Get(jobID)
	if !ok {
		return fmt.Errorf("%w: variant import job %s not found", ErrNotFound, jobID)
	}

	job.Status = statusCancelled

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Share
// ────────────────────────────────────────────────────────────────────────────

// CreateShare creates a new resource share.
func (b *InMemoryBackend) CreateShare(
	resourceARN, principalSubscriber, name string,
) (*Share, error) {
	b.mu.Lock("CreateShare")
	defer b.mu.Unlock()

	id := newID()
	now := time.Now().UTC()
	share := &Share{
		ShareID:             id,
		ResourceARN:         resourceARN,
		PrincipalSubscriber: principalSubscriber,
		Name:                name,
		Status:              "PENDING",
		CreationTime:        now,
	}
	b.shares.Put(share)

	result := *share

	return &result, nil
}

// AcceptShare accepts a share invitation.
func (b *InMemoryBackend) AcceptShare(shareID string) (*Share, error) {
	b.mu.Lock("AcceptShare")
	defer b.mu.Unlock()

	share, ok := b.shares.Get(shareID)
	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	share.Status = "ACTIVATING"
	now := time.Now().UTC()
	share.UpdateTime = &now
	result := *share

	return &result, nil
}

// DeleteShare deletes a share.
func (b *InMemoryBackend) DeleteShare(shareID string) (*Share, error) {
	b.mu.Lock("DeleteShare")
	defer b.mu.Unlock()

	share, ok := b.shares.Get(shareID)
	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	b.shares.Delete(shareID)
	result := *share
	result.Status = "DELETED"

	return &result, nil
}

// GetShare retrieves a share.
func (b *InMemoryBackend) GetShare(shareID string) (*Share, error) {
	b.mu.RLock("GetShare")
	defer b.mu.RUnlock()

	share, ok := b.shares.Get(shareID)
	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	result := *share

	return &result, nil
}

// ListShares lists shares by resource owner.
func (b *InMemoryBackend) ListShares(
	resourceOwner string,
	maxResults int,
	nextToken string,
) ([]*Share, string, error) {
	b.mu.RLock("ListShares")
	defer b.mu.RUnlock()

	all := b.shares.All()

	var ids []string

	for _, s := range all {
		isSelf := strings.Contains(s.ResourceARN, ":"+b.accountID+":")

		switch resourceOwner {
		case "SELF":
			if isSelf {
				ids = append(ids, s.ShareID)
			}
		case "OTHER":
			if !isSelf {
				ids = append(ids, s.ShareID)
			}
		default:
			ids = append(ids, s.ShareID)
		}
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.shares.Get)

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// RunCache
// ────────────────────────────────────────────────────────────────────────────

// CreateRunCache creates a new run cache.
func (b *InMemoryBackend) CreateRunCache(
	name, cacheS3Location string,
	tags map[string]string,
) (*RunCache, error) {
	b.mu.Lock("CreateRunCache")
	defer b.mu.Unlock()

	id := newID()
	rc := &RunCache{
		ID:              id,
		Name:            name,
		CacheS3Location: cacheS3Location,
		Status:          statusActive,
		Tags:            copyTags(tags),
		CreationTime:    time.Now().UTC(),
	}
	rc.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "runCache/"+id)

	b.runCaches.Put(rc)

	if tags != nil {
		b.tags[rc.Arn] = copyTags(tags)
	}

	result := *rc

	return &result, nil
}

// DeleteRunCache deletes a run cache.
func (b *InMemoryBackend) DeleteRunCache(id string) error {
	b.mu.Lock("DeleteRunCache")
	defer b.mu.Unlock()

	rc, ok := b.runCaches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	delete(b.tags, rc.Arn)
	b.runCaches.Delete(id)

	return nil
}

// GetRunCache retrieves a run cache.
func (b *InMemoryBackend) GetRunCache(id string) (*RunCache, error) {
	b.mu.RLock("GetRunCache")
	defer b.mu.RUnlock()

	rc, ok := b.runCaches.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	result := *rc

	return &result, nil
}

// ListRunCaches lists run caches.
func (b *InMemoryBackend) ListRunCaches(
	maxResults int,
	nextToken string,
) ([]*RunCache, string, error) {
	b.mu.RLock("ListRunCaches")
	defer b.mu.RUnlock()

	all := b.runCaches.All()
	ids := make([]string, 0, len(all))

	for _, rc := range all {
		ids = append(ids, rc.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runCaches.Get)

	return result, outToken, nil
}

// UpdateRunCache updates a run cache.
func (b *InMemoryBackend) UpdateRunCache(id, name, description string) error {
	b.mu.Lock("UpdateRunCache")
	defer b.mu.Unlock()

	rc, ok := b.runCaches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	if name != "" {
		rc.Name = name
	}

	if description != "" {
		rc.Description = description
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// RunBatch
// ────────────────────────────────────────────────────────────────────────────

// StartRunBatch starts a new run batch.
func (b *InMemoryBackend) StartRunBatch(workflowID, roleARN, name string) (*RunBatch, error) {
	b.mu.Lock("StartRunBatch")
	defer b.mu.Unlock()

	id := newID()
	rb := &RunBatch{
		ID:           id,
		Name:         name,
		WorkflowID:   workflowID,
		RoleARN:      roleARN,
		Status:       statusCompleted,
		CreationTime: time.Now().UTC(),
	}
	rb.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "runBatch/"+id)

	b.runBatches.Put(rb)

	result := *rb

	return &result, nil
}

// CancelRunBatch cancels a run batch.
func (b *InMemoryBackend) CancelRunBatch(id string) error {
	b.mu.Lock("CancelRunBatch")
	defer b.mu.Unlock()

	rb, ok := b.runBatches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	if rb.Status == statusCompleted || rb.Status == statusCancelled || rb.Status == statusFailed {
		return fmt.Errorf("%w: run batch %s is already in terminal state %s", ErrValidation, id, rb.Status)
	}

	rb.Status = statusCancelled

	return nil
}

// DeleteRunBatch deletes a single run batch.
func (b *InMemoryBackend) DeleteRunBatch(id string) error {
	b.mu.Lock("DeleteRunBatch")
	defer b.mu.Unlock()

	rb, ok := b.runBatches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	delete(b.tags, rb.Arn)
	b.runBatches.Delete(id)

	return nil
}

// GetRunBatch retrieves a run batch.
func (b *InMemoryBackend) GetRunBatch(id string) (*RunBatch, error) {
	b.mu.RLock("GetRunBatch")
	defer b.mu.RUnlock()

	rb, ok := b.runBatches.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	result := *rb

	return &result, nil
}

// ListRunBatches lists run batches.
func (b *InMemoryBackend) ListRunBatches(
	maxResults int,
	nextToken string,
) ([]*RunBatch, string, error) {
	b.mu.RLock("ListRunBatches")
	defer b.mu.RUnlock()

	all := b.runBatches.All()
	ids := make([]string, 0, len(all))

	for _, rb := range all {
		ids = append(ids, rb.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runBatches.Get)

	return result, outToken, nil
}

// DeleteRunBatches deletes multiple run batches.
func (b *InMemoryBackend) DeleteRunBatches(ids []string) ([]RunBatchDeleteError, error) {
	b.mu.Lock("DeleteRunBatches")
	defer b.mu.Unlock()

	var errs []RunBatchDeleteError

	for _, id := range ids {
		rb, ok := b.runBatches.Get(id)
		if !ok {
			errs = append(errs, RunBatchDeleteError{
				ID:      id,
				Code:    errResourceNotFound,
				Message: fmt.Sprintf("run batch %s not found", id),
			})

			continue
		}

		delete(b.tags, rb.Arn)
		b.runBatches.Delete(id)
	}

	return errs, nil
}

// ListRunsInBatch lists runs that belong to a run batch.
func (b *InMemoryBackend) ListRunsInBatch(
	batchID string,
	maxResults int,
	nextToken string,
) ([]*Run, string, error) {
	b.mu.RLock("ListRunsInBatch")
	defer b.mu.RUnlock()

	if !b.runBatches.Has(batchID) {
		return nil, "", fmt.Errorf("%w: run batch %s not found", ErrNotFound, batchID)
	}

	var ids []string

	for _, r := range b.runs.All() {
		if r.RunBatchID == batchID {
			ids = append(ids, r.ID)
		}
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runs.Get)

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Configuration
// ────────────────────────────────────────────────────────────────────────────

// CreateConfiguration creates a configuration.
func (b *InMemoryBackend) CreateConfiguration(name, description string) (*Configuration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	if b.configurations.Has(name) {
		return nil, fmt.Errorf("%w: configuration %s already exists", ErrAlreadyExists, name)
	}

	cfg := &Configuration{
		Name:         name,
		Description:  description,
		CreationTime: time.Now().UTC(),
	}
	b.configurations.Put(cfg)

	result := *cfg

	return &result, nil
}

// DeleteConfiguration deletes a configuration.
func (b *InMemoryBackend) DeleteConfiguration(name string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	if !b.configurations.Has(name) {
		return fmt.Errorf("%w: configuration %s not found", ErrNotFound, name)
	}

	b.configurations.Delete(name)

	return nil
}

// GetConfiguration retrieves a configuration.
func (b *InMemoryBackend) GetConfiguration(name string) (*Configuration, error) {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, name)
	}

	result := *cfg

	return &result, nil
}

// ListConfigurations lists configurations.
func (b *InMemoryBackend) ListConfigurations(
	maxResults int,
	nextToken string,
) ([]*Configuration, string, error) {
	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	all := b.configurations.All()
	names := make([]string, 0, len(all))

	for _, cfg := range all {
		names = append(names, cfg.Name)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, b.configurations.Get)

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// S3 Access Policy
// ────────────────────────────────────────────────────────────────────────────

// PutS3AccessPolicy stores an S3 access policy.
func (b *InMemoryBackend) PutS3AccessPolicy(s3AccessPointARN, policy string) error {
	b.mu.Lock("PutS3AccessPolicy")
	defer b.mu.Unlock()

	b.s3AccessPolicies.Put(&S3AccessPolicy{
		S3AccessPointARN: s3AccessPointARN,
		Policy:           policy,
	})

	return nil
}

// GetS3AccessPolicy retrieves an S3 access policy.
func (b *InMemoryBackend) GetS3AccessPolicy(s3AccessPointARN string) (*S3AccessPolicy, error) {
	b.mu.RLock("GetS3AccessPolicy")
	defer b.mu.RUnlock()

	p, ok := b.s3AccessPolicies.Get(s3AccessPointARN)
	if !ok {
		return nil, fmt.Errorf(
			"%w: S3 access policy for %s not found",
			ErrNotFound,
			s3AccessPointARN,
		)
	}

	result := *p

	return &result, nil
}

// DeleteS3AccessPolicy deletes an S3 access policy.
func (b *InMemoryBackend) DeleteS3AccessPolicy(s3AccessPointARN string) error {
	b.mu.Lock("DeleteS3AccessPolicy")
	defer b.mu.Unlock()

	if !b.s3AccessPolicies.Has(s3AccessPointARN) {
		return fmt.Errorf("%w: S3 access policy for %s not found", ErrNotFound, s3AccessPointARN)
	}

	b.s3AccessPolicies.Delete(s3AccessPointARN)

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Tags
// ────────────────────────────────────────────────────────────────────────────

// TagResource adds tags to a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.tags[resourceARN]

	if tags == nil {
		return map[string]string{}, nil
	}

	return maps.Clone(tags), nil
}
