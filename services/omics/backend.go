package omics

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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

// regionState holds all HealthOmics resources for a single region.
type regionState struct {
	referenceStores       map[string]*ReferenceStore
	references            map[string]map[string]*ReferenceMetadata
	referenceImportJobs   map[string]map[string]*ReferenceImportJob
	sequenceStores        map[string]*SequenceStore
	readSets              map[string]map[string]*ReadSetMetadata
	readSetActivationJobs map[string]map[string]*ReadSetActivationJob
	readSetExportJobs     map[string]map[string]*ReadSetExportJob
	readSetImportJobs     map[string]map[string]*ReadSetImportJob
	multipartUploads      map[string]map[string]*MultipartReadSetUpload
	uploadParts           map[string]map[string][]*ReadSetUploadPart
	runGroups             map[string]*RunGroup
	runs                  map[string]*Run
	runTasks              map[string]map[string]*RunTask
	workflows             map[string]*Workflow
	workflowVersions      map[string]map[string]*WorkflowVersion
	annotationStores      map[string]*AnnotationStore
	annotationVersions    map[string]map[string]*AnnotationStoreVersion
	annotationImportJobs  map[string]*AnnotationImportJob
	variantStores         map[string]*VariantStore
	variantImportJobs     map[string]*VariantImportJob
	shares                map[string]*Share
	runCaches             map[string]*RunCache
	runBatches            map[string]*RunBatch
	configurations        map[string]*Configuration
	s3AccessPolicies      map[string]*S3AccessPolicy
	tags                  map[string]map[string]string
}

func newRegionState() *regionState {
	return &regionState{
		referenceStores:       make(map[string]*ReferenceStore),
		references:            make(map[string]map[string]*ReferenceMetadata),
		referenceImportJobs:   make(map[string]map[string]*ReferenceImportJob),
		sequenceStores:        make(map[string]*SequenceStore),
		readSets:              make(map[string]map[string]*ReadSetMetadata),
		readSetActivationJobs: make(map[string]map[string]*ReadSetActivationJob),
		readSetExportJobs:     make(map[string]map[string]*ReadSetExportJob),
		readSetImportJobs:     make(map[string]map[string]*ReadSetImportJob),
		multipartUploads:      make(map[string]map[string]*MultipartReadSetUpload),
		uploadParts:           make(map[string]map[string][]*ReadSetUploadPart),
		runGroups:             make(map[string]*RunGroup),
		runs:                  make(map[string]*Run),
		runTasks:              make(map[string]map[string]*RunTask),
		workflows:             make(map[string]*Workflow),
		workflowVersions:      make(map[string]map[string]*WorkflowVersion),
		annotationStores:      make(map[string]*AnnotationStore),
		annotationVersions:    make(map[string]map[string]*AnnotationStoreVersion),
		annotationImportJobs:  make(map[string]*AnnotationImportJob),
		variantStores:         make(map[string]*VariantStore),
		variantImportJobs:     make(map[string]*VariantImportJob),
		shares:                make(map[string]*Share),
		runCaches:             make(map[string]*RunCache),
		runBatches:            make(map[string]*RunBatch),
		configurations:        make(map[string]*Configuration),
		s3AccessPolicies:      make(map[string]*S3AccessPolicy),
		tags:                  make(map[string]map[string]string),
	}
}

// InMemoryBackend is the in-memory backend for HealthOmics.
type InMemoryBackend struct {
	regions       map[string]*regionState
	accountID     string
	defaultRegion string
	mu            sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:     accountID,
		defaultRegion: region,
		regions:       make(map[string]*regionState),
	}
	b.regions[region] = newRegionState()

	return b
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.regions = make(map[string]*regionState)
	b.regions[b.defaultRegion] = newRegionState()
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data, _ := json.Marshal(b.regions)

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return json.Unmarshal(data, &b.regions)
}

// region returns the regionState for the given region, creating it if needed.
func (b *InMemoryBackend) region(r string) *regionState {
	if s, ok := b.regions[r]; ok {
		return s
	}

	s := newRegionState()
	b.regions[r] = s

	return s
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

	b.mu.Lock()
	defer b.mu.Unlock()

	rs := &ReferenceStore{
		ID:           newID(),
		Name:         name,
		Description:  description,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	rs.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "referenceStore/"+rs.ID)

	st := b.region(b.defaultRegion)
	st.referenceStores[rs.ID] = rs
	st.references[rs.ID] = make(map[string]*ReferenceMetadata)
	st.referenceImportJobs[rs.ID] = make(map[string]*ReferenceImportJob)

	if tags != nil {
		st.tags[rs.Arn] = copyTags(tags)
	}

	result := *rs

	return &result, nil
}

// DeleteReferenceStore deletes a reference store by ID.
func (b *InMemoryBackend) DeleteReferenceStore(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	rs, ok := st.referenceStores[id]

	if !ok {
		return fmt.Errorf("%w: reference store %s not found", ErrNotFound, id)
	}

	delete(st.tags, rs.Arn)
	delete(st.referenceStores, id)
	delete(st.references, id)
	delete(st.referenceImportJobs, id)

	return nil
}

// GetReferenceStore retrieves a reference store by ID.
func (b *InMemoryBackend) GetReferenceStore(id string) (*ReferenceStore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	rs, ok := st.referenceStores[id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := make([]string, 0, len(st.referenceStores))

	for id, rs := range st.referenceStores {
		if filter != nil && filter.Name != "" && rs.Name != filter.Name {
			continue
		}

		ids = append(ids, id)
	}

	sort.Strings(ids)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*ReferenceStore, 0, len(page))

	for _, id := range page {
		rs := *st.referenceStores[id]
		result = append(result, &rs)
	}

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Reference
// ────────────────────────────────────────────────────────────────────────────

// DeleteReference deletes a reference by store ID and reference ID.
func (b *InMemoryBackend) DeleteReference(referenceStoreID, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.referenceStores[referenceStoreID]; !ok {
		return fmt.Errorf("%w: reference store %s not found", ErrNotFound, referenceStoreID)
	}

	refs := st.references[referenceStoreID]
	ref, ok := refs[id]

	if !ok {
		return fmt.Errorf("%w: reference %s not found", ErrNotFound, id)
	}

	delete(st.tags, ref.Arn)
	delete(refs, id)

	return nil
}

// GetReferenceMetadata retrieves reference metadata.
func (b *InMemoryBackend) GetReferenceMetadata(
	referenceStoreID, id string,
) (*ReferenceMetadata, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.referenceStores[referenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: reference store %s not found", ErrNotFound, referenceStoreID)
	}

	ref, ok := st.references[referenceStoreID][id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.referenceStores[referenceStoreID]; !ok {
		return nil, "", fmt.Errorf(
			"%w: reference store %s not found",
			ErrNotFound,
			referenceStoreID,
		)
	}

	refs := st.references[referenceStoreID]
	ids := make([]string, 0, len(refs))

	for id, ref := range refs {
		if filter != nil && filter.Name != "" && ref.Name != filter.Name {
			continue
		}

		ids = append(ids, id)
	}

	sort.Strings(ids)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*ReferenceMetadata, 0, len(page))

	for _, id := range page {
		r := *refs[id]
		result = append(result, &r)
	}

	return result, outToken, nil
}

// StartReferenceImportJob creates a reference import job.
func (b *InMemoryBackend) StartReferenceImportJob(
	referenceStoreID, roleARN string,
	sources []ReferenceImportJobSource,
) (*ReferenceImportJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.referenceStores[referenceStoreID]; !ok {
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
		st.references[referenceStoreID][refID] = ref
	}

	st.referenceImportJobs[referenceStoreID][job.ID] = job

	result := *job

	return &result, nil
}

// GetReferenceImportJob retrieves a reference import job.
func (b *InMemoryBackend) GetReferenceImportJob(
	referenceStoreID, jobID string,
) (*ReferenceImportJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.referenceStores[referenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: reference store %s not found", ErrNotFound, referenceStoreID)
	}

	job, ok := st.referenceImportJobs[referenceStoreID][jobID]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.referenceStores[referenceStoreID]; !ok {
		return nil, "", fmt.Errorf(
			"%w: reference store %s not found",
			ErrNotFound,
			referenceStoreID,
		)
	}

	jobs := st.referenceImportJobs[referenceStoreID]
	ids := make([]string, 0, len(jobs))

	for id := range jobs {
		ids = append(ids, id)
	}

	sort.Strings(ids)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*ReferenceImportJob, 0, len(page))

	for _, id := range page {
		j := *jobs[id]
		result = append(result, &j)
	}

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

	b.mu.Lock()
	defer b.mu.Unlock()

	ss := &SequenceStore{
		ID:           newID(),
		Name:         name,
		Description:  description,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	ss.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "sequenceStore/"+ss.ID)

	st := b.region(b.defaultRegion)
	st.sequenceStores[ss.ID] = ss
	st.readSets[ss.ID] = make(map[string]*ReadSetMetadata)
	st.readSetActivationJobs[ss.ID] = make(map[string]*ReadSetActivationJob)
	st.readSetExportJobs[ss.ID] = make(map[string]*ReadSetExportJob)
	st.readSetImportJobs[ss.ID] = make(map[string]*ReadSetImportJob)
	st.multipartUploads[ss.ID] = make(map[string]*MultipartReadSetUpload)
	st.uploadParts[ss.ID] = make(map[string][]*ReadSetUploadPart)

	if tags != nil {
		st.tags[ss.Arn] = copyTags(tags)
	}

	result := *ss

	return &result, nil
}

// DeleteSequenceStore deletes a sequence store by ID.
func (b *InMemoryBackend) DeleteSequenceStore(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	ss, ok := st.sequenceStores[id]

	if !ok {
		return fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	delete(st.tags, ss.Arn)
	delete(st.sequenceStores, id)
	delete(st.readSets, id)
	delete(st.readSetActivationJobs, id)
	delete(st.readSetExportJobs, id)
	delete(st.readSetImportJobs, id)
	delete(st.multipartUploads, id)
	delete(st.uploadParts, id)

	return nil
}

// GetSequenceStore retrieves a sequence store by ID.
func (b *InMemoryBackend) GetSequenceStore(id string) (*SequenceStore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ss, ok := st.sequenceStores[id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := make([]string, 0, len(st.sequenceStores))

	for id, ss := range st.sequenceStores {
		if filter != nil && filter.Name != "" && ss.Name != filter.Name {
			continue
		}

		ids = append(ids, id)
	}

	sort.Strings(ids)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*SequenceStore, 0, len(page))

	for _, id := range page {
		ss := *st.sequenceStores[id]
		result = append(result, &ss)
	}

	return result, outToken, nil
}

// UpdateSequenceStore updates a sequence store's name and description.
func (b *InMemoryBackend) UpdateSequenceStore(
	id, name, description string,
) (*SequenceStore, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	ss, ok := st.sequenceStores[id]

	if !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	if name != "" {
		ss.Name = name
	}

	if description != "" {
		ss.Description = description
	}

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	readSets := st.readSets[sequenceStoreID]
	var errs []ReadSetBatchError

	for _, id := range ids {
		rs, ok := readSets[id]
		if !ok {
			errs = append(errs, ReadSetBatchError{
				ID:      id,
				Code:    errResourceNotFound,
				Message: fmt.Sprintf("read set %s not found", id),
			})

			continue
		}

		delete(st.tags, rs.Arn)
		delete(readSets, id)
	}

	return errs, nil
}

// GetReadSetMetadata retrieves read set metadata.
func (b *InMemoryBackend) GetReadSetMetadata(sequenceStoreID, id string) (*ReadSetMetadata, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	rs, ok := st.readSets[sequenceStoreID][id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	readSets := st.readSets[sequenceStoreID]
	ids := make([]string, 0, len(readSets))

	for id, rs := range readSets {
		if filter != nil {
			if filter.Name != "" && rs.Name != filter.Name {
				continue
			}

			if filter.Status != "" && rs.Status != filter.Status {
				continue
			}
		}

		ids = append(ids, id)
	}

	sort.Strings(ids)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*ReadSetMetadata, 0, len(page))

	for _, id := range page {
		rs := *readSets[id]
		result = append(result, &rs)
	}

	return result, outToken, nil
}

// StartReadSetActivationJob creates a read set activation job.
func (b *InMemoryBackend) StartReadSetActivationJob(
	sequenceStoreID string,
	sources []ReadSetActivationJobSource,
) (*ReadSetActivationJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
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
	st.readSetActivationJobs[sequenceStoreID][job.ID] = job

	result := *job

	return &result, nil
}

// GetReadSetActivationJob retrieves a read set activation job.
func (b *InMemoryBackend) GetReadSetActivationJob(
	sequenceStoreID, jobID string,
) (*ReadSetActivationJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := st.readSetActivationJobs[sequenceStoreID][jobID]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	jobs := st.readSetActivationJobs[sequenceStoreID]
	ids := sortedKeys2(jobs)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*ReadSetActivationJob, 0, len(page))

	for _, id := range page {
		j := *jobs[id]
		result = append(result, &j)
	}

	return result, outToken, nil
}

// StartReadSetExportJob creates a read set export job.
func (b *InMemoryBackend) StartReadSetExportJob(
	sequenceStoreID, destination string,
	sources []ReadSetExportJobSource,
) (*ReadSetExportJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
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
	st.readSetExportJobs[sequenceStoreID][job.ID] = job

	result := *job

	return &result, nil
}

// GetReadSetExportJob retrieves a read set export job.
func (b *InMemoryBackend) GetReadSetExportJob(
	sequenceStoreID, jobID string,
) (*ReadSetExportJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := st.readSetExportJobs[sequenceStoreID][jobID]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	jobs := st.readSetExportJobs[sequenceStoreID]
	ids := sortedKeys2(jobs)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*ReadSetExportJob, 0, len(page))

	for _, id := range page {
		j := *jobs[id]
		result = append(result, &j)
	}

	return result, outToken, nil
}

// StartReadSetImportJob creates a read set import job.
func (b *InMemoryBackend) StartReadSetImportJob(
	sequenceStoreID, roleARN string,
	sources []ReadSetImportJobSource,
) (*ReadSetImportJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
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
		st.readSets[sequenceStoreID][rsID] = rs
	}

	st.readSetImportJobs[sequenceStoreID][job.ID] = job

	result := *job

	return &result, nil
}

// GetReadSetImportJob retrieves a read set import job.
func (b *InMemoryBackend) GetReadSetImportJob(
	sequenceStoreID, jobID string,
) (*ReadSetImportJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := st.readSetImportJobs[sequenceStoreID][jobID]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	jobs := st.readSetImportJobs[sequenceStoreID]
	ids := sortedKeys2(jobs)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*ReadSetImportJob, 0, len(page))

	for _, id := range page {
		j := *jobs[id]
		result = append(result, &j)
	}

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
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
	st.multipartUploads[sequenceStoreID][upload.UploadID] = upload
	st.uploadParts[sequenceStoreID][upload.UploadID] = nil

	result := *upload

	return &result, nil
}

// AbortMultipartReadSetUpload aborts a multipart read set upload.
func (b *InMemoryBackend) AbortMultipartReadSetUpload(sequenceStoreID, uploadID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if _, ok := st.multipartUploads[sequenceStoreID][uploadID]; !ok {
		return fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	delete(st.multipartUploads[sequenceStoreID], uploadID)
	delete(st.uploadParts[sequenceStoreID], uploadID)

	return nil
}

// CompleteMultipartReadSetUpload completes a multipart read set upload.
func (b *InMemoryBackend) CompleteMultipartReadSetUpload(
	sequenceStoreID, uploadID string,
) (*ReadSetMetadata, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	upload, ok := st.multipartUploads[sequenceStoreID][uploadID]

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
	st.readSets[sequenceStoreID][rsID] = rs
	delete(st.multipartUploads[sequenceStoreID], uploadID)
	delete(st.uploadParts[sequenceStoreID], uploadID)

	result := *rs

	return &result, nil
}

// ListMultipartReadSetUploads lists in-progress multipart uploads.
func (b *InMemoryBackend) ListMultipartReadSetUploads(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*MultipartReadSetUpload, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	uploads := st.multipartUploads[sequenceStoreID]
	ids := sortedKeys2(uploads)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*MultipartReadSetUpload, 0, len(page))

	for _, id := range page {
		u := *uploads[id]
		result = append(result, &u)
	}

	return result, outToken, nil
}

// ListReadSetUploadParts lists parts for a multipart read set upload.
func (b *InMemoryBackend) ListReadSetUploadParts(
	sequenceStoreID, uploadID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetUploadPart, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.sequenceStores[sequenceStoreID]; !ok {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if _, ok := st.multipartUploads[sequenceStoreID][uploadID]; !ok {
		return nil, "", fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	parts := st.uploadParts[sequenceStoreID][uploadID]

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
	b.mu.Lock()
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

	st := b.region(b.defaultRegion)
	st.runGroups[id] = rg

	if tags != nil {
		st.tags[rg.Arn] = copyTags(tags)
	}

	result := *rg

	return &result, nil
}

// DeleteRunGroup deletes a run group.
func (b *InMemoryBackend) DeleteRunGroup(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	rg, ok := st.runGroups[id]

	if !ok {
		return fmt.Errorf("%w: run group %s not found", ErrNotFound, id)
	}

	delete(st.tags, rg.Arn)
	delete(st.runGroups, id)

	return nil
}

// GetRunGroup retrieves a run group.
func (b *InMemoryBackend) GetRunGroup(id string) (*RunGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	rg, ok := st.runGroups[id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.runGroups)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*RunGroup, 0, len(page))

	for _, id := range page {
		rg := *st.runGroups[id]
		result = append(result, &rg)
	}

	return result, outToken, nil
}

// UpdateRunGroup updates a run group.
func (b *InMemoryBackend) UpdateRunGroup(
	id, name string,
	maxCPUs, maxRuns, maxDuration int,
	maxGPUs int,
) (*RunGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	rg, ok := st.runGroups[id]

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
	workflowID, roleARN, name string,
	params map[string]any,
	tags map[string]string,
) (*Run, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := newID()
	now := time.Now().UTC()
	run := &Run{
		ID:           id,
		Name:         name,
		WorkflowID:   workflowID,
		RoleARN:      roleARN,
		Params:       params,
		Tags:         copyTags(tags),
		Status:       statusCompleted,
		CreationTime: now,
		StartTime:    &now,
		StopTime:     &now,
	}
	run.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "run/"+id)

	st := b.region(b.defaultRegion)
	st.runs[id] = run
	st.runTasks[id] = make(map[string]*RunTask)

	taskID := newID()
	st.runTasks[id][taskID] = &RunTask{
		TaskID:       taskID,
		RunID:        id,
		Name:         "task-1",
		Status:       statusCompleted,
		CPUs:         stubTaskCPUs,
		Memory:       stubTaskMemory,
		CreationTime: now,
		StartTime:    &now,
		StopTime:     &now,
	}

	if tags != nil {
		st.tags[run.Arn] = copyTags(tags)
	}

	result := *run

	return &result, nil
}

// CancelRun cancels a run.
func (b *InMemoryBackend) CancelRun(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.runs[id]; !ok {
		return fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	st.runs[id].Status = statusCancelled

	return nil
}

// DeleteRun deletes a run.
func (b *InMemoryBackend) DeleteRun(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	run, ok := st.runs[id]

	if !ok {
		return fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	delete(st.tags, run.Arn)
	delete(st.runs, id)
	delete(st.runTasks, id)

	return nil
}

// GetRun retrieves a run.
func (b *InMemoryBackend) GetRun(id string) (*Run, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	run, ok := st.runs[id]

	if !ok {
		return nil, fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	result := *run

	return &result, nil
}

// ListRuns lists runs.
func (b *InMemoryBackend) ListRuns(maxResults int, nextToken string) ([]*Run, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.runs)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*Run, 0, len(page))

	for _, id := range page {
		r := *st.runs[id]
		result = append(result, &r)
	}

	return result, outToken, nil
}

// GetRunTask retrieves a task within a run.
func (b *InMemoryBackend) GetRunTask(runID, taskID string) (*RunTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.runs[runID]; !ok {
		return nil, fmt.Errorf("%w: run %s not found", ErrNotFound, runID)
	}

	task, ok := st.runTasks[runID][taskID]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.runs[runID]; !ok {
		return nil, "", fmt.Errorf("%w: run %s not found", ErrNotFound, runID)
	}

	tasks := st.runTasks[runID]
	ids := sortedKeys2(tasks)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*RunTask, 0, len(page))

	for _, id := range page {
		t := *tasks[id]
		result = append(result, &t)
	}

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Workflow
// ────────────────────────────────────────────────────────────────────────────

// CreateWorkflow creates a new workflow.
func (b *InMemoryBackend) CreateWorkflow(
	name, description, _ /* definitionZip */, engine string,
	tags map[string]string,
) (*Workflow, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	id := newID()
	wf := &Workflow{
		ID:           id,
		Name:         name,
		Description:  description,
		Engine:       engine,
		Status:       statusActive,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	wf.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "workflow/"+id)

	st := b.region(b.defaultRegion)
	st.workflows[id] = wf
	st.workflowVersions[id] = make(map[string]*WorkflowVersion)

	if tags != nil {
		st.tags[wf.Arn] = copyTags(tags)
	}

	result := *wf

	return &result, nil
}

// DeleteWorkflow deletes a workflow.
func (b *InMemoryBackend) DeleteWorkflow(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	wf, ok := st.workflows[id]

	if !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, id)
	}

	delete(st.tags, wf.Arn)
	delete(st.workflows, id)
	delete(st.workflowVersions, id)

	return nil
}

// GetWorkflow retrieves a workflow.
func (b *InMemoryBackend) GetWorkflow(id string) (*Workflow, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	wf, ok := st.workflows[id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.workflows)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*Workflow, 0, len(page))

	for _, id := range page {
		wf := *st.workflows[id]
		result = append(result, &wf)
	}

	return result, outToken, nil
}

// UpdateWorkflow updates a workflow.
func (b *InMemoryBackend) UpdateWorkflow(id, name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	wf, ok := st.workflows[id]

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	wf, ok := st.workflows[workflowID]

	if !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	if _, exists := st.workflowVersions[workflowID][versionName]; exists {
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
		Status:       statusActive,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	wv.Arn = arn.Build(
		"omics",
		b.defaultRegion,
		b.accountID,
		fmt.Sprintf("workflow/%s/version/%s", workflowID, versionName),
	)

	_ = wf

	st.workflowVersions[workflowID][versionName] = wv

	if tags != nil {
		st.tags[wv.Arn] = copyTags(tags)
	}

	result := *wv

	return &result, nil
}

// DeleteWorkflowVersion deletes a workflow version.
func (b *InMemoryBackend) DeleteWorkflowVersion(workflowID, versionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.workflows[workflowID]; !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := st.workflowVersions[workflowID][versionName]

	if !ok {
		return fmt.Errorf("%w: workflow version %s not found", ErrNotFound, versionName)
	}

	delete(st.tags, wv.Arn)
	delete(st.workflowVersions[workflowID], versionName)

	return nil
}

// GetWorkflowVersion retrieves a workflow version.
func (b *InMemoryBackend) GetWorkflowVersion(
	workflowID, versionName string,
) (*WorkflowVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.workflows[workflowID]; !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := st.workflowVersions[workflowID][versionName]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.workflows[workflowID]; !ok {
		return nil, "", fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	versions := st.workflowVersions[workflowID]
	names := make([]string, 0, len(versions))

	for name := range versions {
		names = append(names, name)
	}

	sort.Strings(names)
	page, outToken := paginateStrings(names, nextToken, maxResults)

	result := make([]*WorkflowVersion, 0, len(page))

	for _, name := range page {
		wv := *versions[name]
		result = append(result, &wv)
	}

	return result, outToken, nil
}

// UpdateWorkflowVersion updates a workflow version.
func (b *InMemoryBackend) UpdateWorkflowVersion(workflowID, versionName, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.workflows[workflowID]; !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := st.workflowVersions[workflowID][versionName]

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
	tags map[string]string,
) (*AnnotationStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, exists := st.annotationStores[name]; exists {
		return nil, fmt.Errorf("%w: annotation store %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	as := &AnnotationStore{
		ID:           newID(),
		Name:         name,
		StoreFormat:  storeFormat,
		Status:       statusActive,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	as.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "annotationStore/"+name)
	st.annotationStores[name] = as
	st.annotationVersions[name] = make(map[string]*AnnotationStoreVersion)

	if tags != nil {
		st.tags[as.Arn] = copyTags(tags)
	}

	result := *as

	return &result, nil
}

// DeleteAnnotationStore deletes an annotation store.
func (b *InMemoryBackend) DeleteAnnotationStore(name string) (*AnnotationStore, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	as, ok := st.annotationStores[name]

	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	delete(st.tags, as.Arn)
	delete(st.annotationStores, name)
	delete(st.annotationVersions, name)

	result := *as
	result.Status = statusDeleting

	return &result, nil
}

// GetAnnotationStore retrieves an annotation store.
func (b *InMemoryBackend) GetAnnotationStore(name string) (*AnnotationStore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	as, ok := st.annotationStores[name]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	names := sortedKeys2(st.annotationStores)
	page, outToken := paginateStrings(names, nextToken, maxResults)

	result := make([]*AnnotationStore, 0, len(page))

	for _, name := range page {
		as := *st.annotationStores[name]
		result = append(result, &as)
	}

	return result, outToken, nil
}

// UpdateAnnotationStore updates an annotation store.
func (b *InMemoryBackend) UpdateAnnotationStore(
	name, description string,
) (*AnnotationStore, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	as, ok := st.annotationStores[name]

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.annotationStores[destinationName]; !ok {
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
	st.annotationImportJobs[job.ID] = job

	result := *job

	return &result, nil
}

// GetAnnotationImportJob retrieves an annotation import job.
func (b *InMemoryBackend) GetAnnotationImportJob(jobID string) (*AnnotationImportJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	job, ok := st.annotationImportJobs[jobID]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.annotationImportJobs)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*AnnotationImportJob, 0, len(page))

	for _, id := range page {
		j := *st.annotationImportJobs[id]
		result = append(result, &j)
	}

	return result, outToken, nil
}

// CancelAnnotationImportJob cancels an annotation import job.
func (b *InMemoryBackend) CancelAnnotationImportJob(jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	job, ok := st.annotationImportJobs[jobID]

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	as, ok := st.annotationStores[name]

	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	if _, exists := st.annotationVersions[name][versionName]; exists {
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
	st.annotationVersions[name][versionName] = v

	if tags != nil {
		st.tags[v.Arn] = copyTags(tags)
	}

	result := *v

	return &result, nil
}

// DeleteAnnotationStoreVersions deletes one or more annotation store versions.
func (b *InMemoryBackend) DeleteAnnotationStoreVersions(
	name string,
	versionNames []string,
) ([]VersionDeleteError, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.annotationStores[name]; !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	var errs []VersionDeleteError

	for _, vn := range versionNames {
		v, ok := st.annotationVersions[name][vn]
		if !ok {
			errs = append(errs, VersionDeleteError{
				VersionName: vn,
				Code:        errResourceNotFound,
				Message:     fmt.Sprintf("version %s not found", vn),
			})

			continue
		}

		delete(st.tags, v.Arn)
		delete(st.annotationVersions[name], vn)
	}

	return errs, nil
}

// GetAnnotationStoreVersion retrieves an annotation store version.
func (b *InMemoryBackend) GetAnnotationStoreVersion(
	name, versionName string,
) (*AnnotationStoreVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.annotationStores[name]; !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	v, ok := st.annotationVersions[name][versionName]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.annotationStores[name]; !ok {
		return nil, "", fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	versions := st.annotationVersions[name]
	names := sortedKeys2(versions)
	page, outToken := paginateStrings(names, nextToken, maxResults)

	result := make([]*AnnotationStoreVersion, 0, len(page))

	for _, vn := range page {
		v := *versions[vn]
		result = append(result, &v)
	}

	return result, outToken, nil
}

// UpdateAnnotationStoreVersion updates an annotation store version.
func (b *InMemoryBackend) UpdateAnnotationStoreVersion(
	name, versionName, description string,
) (*AnnotationStoreVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.annotationStores[name]; !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	v, ok := st.annotationVersions[name][versionName]

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
	tags map[string]string,
) (*VariantStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, exists := st.variantStores[name]; exists {
		return nil, fmt.Errorf("%w: variant store %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	vs := &VariantStore{
		ID:           newID(),
		Name:         name,
		Status:       statusActive,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	vs.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "variantStore/"+name)
	st.variantStores[name] = vs

	if tags != nil {
		st.tags[vs.Arn] = copyTags(tags)
	}

	result := *vs

	return &result, nil
}

// DeleteVariantStore deletes a variant store.
func (b *InMemoryBackend) DeleteVariantStore(name string) (*VariantStore, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	vs, ok := st.variantStores[name]

	if !ok {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, name)
	}

	delete(st.tags, vs.Arn)
	delete(st.variantStores, name)

	result := *vs
	result.Status = statusDeleting

	return &result, nil
}

// GetVariantStore retrieves a variant store.
func (b *InMemoryBackend) GetVariantStore(name string) (*VariantStore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	vs, ok := st.variantStores[name]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	names := sortedKeys2(st.variantStores)
	page, outToken := paginateStrings(names, nextToken, maxResults)

	result := make([]*VariantStore, 0, len(page))

	for _, name := range page {
		vs := *st.variantStores[name]
		result = append(result, &vs)
	}

	return result, outToken, nil
}

// UpdateVariantStore updates a variant store.
func (b *InMemoryBackend) UpdateVariantStore(name, description string) (*VariantStore, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	vs, ok := st.variantStores[name]

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.variantStores[destinationName]; !ok {
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
	st.variantImportJobs[job.ID] = job

	result := *job

	return &result, nil
}

// GetVariantImportJob retrieves a variant import job.
func (b *InMemoryBackend) GetVariantImportJob(jobID string) (*VariantImportJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	job, ok := st.variantImportJobs[jobID]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.variantImportJobs)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*VariantImportJob, 0, len(page))

	for _, id := range page {
		j := *st.variantImportJobs[id]
		result = append(result, &j)
	}

	return result, outToken, nil
}

// CancelVariantImportJob cancels a variant import job.
func (b *InMemoryBackend) CancelVariantImportJob(jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	job, ok := st.variantImportJobs[jobID]

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
	b.mu.Lock()
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
	st := b.region(b.defaultRegion)
	st.shares[id] = share

	result := *share

	return &result, nil
}

// AcceptShare accepts a share invitation.
func (b *InMemoryBackend) AcceptShare(shareID string) (*Share, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	share, ok := st.shares[shareID]

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	share, ok := st.shares[shareID]

	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	delete(st.shares, shareID)
	result := *share
	result.Status = "DELETED"

	return &result, nil
}

// GetShare retrieves a share.
func (b *InMemoryBackend) GetShare(shareID string) (*Share, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	share, ok := st.shares[shareID]

	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	result := *share

	return &result, nil
}

// ListShares lists shares by resource owner.
func (b *InMemoryBackend) ListShares(
	_ /* resourceOwner */ string,
	maxResults int,
	nextToken string,
) ([]*Share, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.shares)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*Share, 0, len(page))

	for _, id := range page {
		s := *st.shares[id]
		result = append(result, &s)
	}

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
	b.mu.Lock()
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

	st := b.region(b.defaultRegion)
	st.runCaches[id] = rc

	if tags != nil {
		st.tags[rc.Arn] = copyTags(tags)
	}

	result := *rc

	return &result, nil
}

// DeleteRunCache deletes a run cache.
func (b *InMemoryBackend) DeleteRunCache(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	rc, ok := st.runCaches[id]

	if !ok {
		return fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	delete(st.tags, rc.Arn)
	delete(st.runCaches, id)

	return nil
}

// GetRunCache retrieves a run cache.
func (b *InMemoryBackend) GetRunCache(id string) (*RunCache, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	rc, ok := st.runCaches[id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.runCaches)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*RunCache, 0, len(page))

	for _, id := range page {
		rc := *st.runCaches[id]
		result = append(result, &rc)
	}

	return result, outToken, nil
}

// UpdateRunCache updates a run cache.
func (b *InMemoryBackend) UpdateRunCache(id, name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	rc, ok := st.runCaches[id]

	if !ok {
		return fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	if name != "" {
		rc.Name = name
	}

	_ = description

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// RunBatch
// ────────────────────────────────────────────────────────────────────────────

// StartRunBatch starts a new run batch.
func (b *InMemoryBackend) StartRunBatch(workflowID, roleARN, name string) (*RunBatch, error) {
	b.mu.Lock()
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

	st := b.region(b.defaultRegion)
	st.runBatches[id] = rb

	result := *rb

	return &result, nil
}

// CancelRunBatch cancels a run batch.
func (b *InMemoryBackend) CancelRunBatch(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.runBatches[id]; !ok {
		return fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	st.runBatches[id].Status = statusCancelled

	return nil
}

// DeleteRunBatch deletes a single run batch.
func (b *InMemoryBackend) DeleteRunBatch(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	rb, ok := st.runBatches[id]

	if !ok {
		return fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	delete(st.tags, rb.Arn)
	delete(st.runBatches, id)

	return nil
}

// GetRunBatch retrieves a run batch.
func (b *InMemoryBackend) GetRunBatch(id string) (*RunBatch, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	rb, ok := st.runBatches[id]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	ids := sortedKeys2(st.runBatches)
	page, outToken := paginateStrings(ids, nextToken, maxResults)

	result := make([]*RunBatch, 0, len(page))

	for _, id := range page {
		rb := *st.runBatches[id]
		result = append(result, &rb)
	}

	return result, outToken, nil
}

// DeleteRunBatches deletes multiple run batches.
func (b *InMemoryBackend) DeleteRunBatches(ids []string) ([]RunBatchDeleteError, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	var errs []RunBatchDeleteError

	for _, id := range ids {
		rb, ok := st.runBatches[id]
		if !ok {
			errs = append(errs, RunBatchDeleteError{
				ID:      id,
				Code:    errResourceNotFound,
				Message: fmt.Sprintf("run batch %s not found", id),
			})

			continue
		}

		delete(st.tags, rb.Arn)
		delete(st.runBatches, id)
	}

	return errs, nil
}

// ListRunsInBatch lists runs that belong to a run batch.
func (b *InMemoryBackend) ListRunsInBatch(
	batchID string,
	_ /* maxResults */ int,
	_ /* nextToken */ string,
) ([]*Run, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.runBatches[batchID]; !ok {
		return nil, "", fmt.Errorf("%w: run batch %s not found", ErrNotFound, batchID)
	}

	return []*Run{}, "", nil
}

// ────────────────────────────────────────────────────────────────────────────
// Configuration
// ────────────────────────────────────────────────────────────────────────────

// CreateConfiguration creates a configuration.
func (b *InMemoryBackend) CreateConfiguration(name, description string) (*Configuration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, exists := st.configurations[name]; exists {
		return nil, fmt.Errorf("%w: configuration %s already exists", ErrAlreadyExists, name)
	}

	cfg := &Configuration{
		Name:         name,
		Description:  description,
		CreationTime: time.Now().UTC(),
	}
	st.configurations[name] = cfg

	result := *cfg

	return &result, nil
}

// DeleteConfiguration deletes a configuration.
func (b *InMemoryBackend) DeleteConfiguration(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.configurations[name]; !ok {
		return fmt.Errorf("%w: configuration %s not found", ErrNotFound, name)
	}

	delete(st.configurations, name)

	return nil
}

// GetConfiguration retrieves a configuration.
func (b *InMemoryBackend) GetConfiguration(name string) (*Configuration, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	cfg, ok := st.configurations[name]

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
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	names := sortedKeys2(st.configurations)
	page, outToken := paginateStrings(names, nextToken, maxResults)

	result := make([]*Configuration, 0, len(page))

	for _, name := range page {
		cfg := *st.configurations[name]
		result = append(result, &cfg)
	}

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// S3 Access Policy
// ────────────────────────────────────────────────────────────────────────────

// PutS3AccessPolicy stores an S3 access policy.
func (b *InMemoryBackend) PutS3AccessPolicy(s3AccessPointARN, policy string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)
	st.s3AccessPolicies[s3AccessPointARN] = &S3AccessPolicy{
		S3AccessPointARN: s3AccessPointARN,
		Policy:           policy,
	}

	return nil
}

// GetS3AccessPolicy retrieves an S3 access policy.
func (b *InMemoryBackend) GetS3AccessPolicy(s3AccessPointARN string) (*S3AccessPolicy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	p, ok := st.s3AccessPolicies[s3AccessPointARN]

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
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if _, ok := st.s3AccessPolicies[s3AccessPointARN]; !ok {
		return fmt.Errorf("%w: S3 access policy for %s not found", ErrNotFound, s3AccessPointARN)
	}

	delete(st.s3AccessPolicies, s3AccessPointARN)

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Tags
// ────────────────────────────────────────────────────────────────────────────

// TagResource adds tags to a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	if st.tags[resourceARN] == nil {
		st.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(st.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.region(b.defaultRegion)

	for _, k := range tagKeys {
		delete(st.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st := b.region(b.defaultRegion)
	tags := st.tags[resourceARN]

	if tags == nil {
		return map[string]string{}, nil
	}

	return maps.Clone(tags), nil
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func sortedKeys2[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}
