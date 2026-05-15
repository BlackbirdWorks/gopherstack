package textract

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrJobNotFound is returned when a document job is not found.
	ErrJobNotFound = awserr.New("InvalidJobIdException", awserr.ErrNotFound)
	// ErrAdapterNotFound is returned when an adapter is not found.
	ErrAdapterNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrAdapterVersionNotFound is returned when an adapter version is not found.
	ErrAdapterVersionNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// Job status constants.
const (
	jobStatusSucceeded  = "SUCCEEDED"
	jobStatusInProgress = "IN_PROGRESS"
	jobStatusFailed     = "FAILED"
)

// Job type constants.
const (
	jobTypeDocumentAnalysis = "DocumentAnalysis"
	jobTypeTextDetection    = "TextDetection"
	jobTypeExpenseAnalysis  = "ExpenseAnalysis"
	jobTypeLendingAnalysis  = "LendingAnalysis"
)

// Adapter feature type constants.
const (
	featureTypeTables     = "TABLES"
	featureTypeForms      = "FORMS"
	featureTypeQueries    = "QUERIES"
	featureTypeSignatures = "SIGNATURES"
	featureTypeLayout     = "LAYOUT"
)

// Adapter auto-update constants.
const (
	autoUpdateEnabled  = "ENABLED"
	autoUpdateDisabled = "DISABLED"
)

// adapterVersionActive is the status for a ready adapter version.
const adapterVersionActive = "ACTIVE"

// Block represents a detected text element returned by Textract.
type Block struct {
	BlockType  string  `json:"BlockType"`
	Text       string  `json:"Text"`
	ID         string  `json:"Id"`
	Confidence float64 `json:"Confidence"`
}

// DocumentJob represents an asynchronous Textract document job.
type DocumentJob struct {
	CreationTime time.Time `json:"creationTime"`
	JobID        string    `json:"jobId"`
	JobStatus    string    `json:"jobStatus"`
	JobType      string    `json:"jobType"` // "DocumentAnalysis" or "TextDetection"
	Blocks       []Block   `json:"blocks"`
}

// ExpenseDocument represents a single expense document result.
type ExpenseDocument struct {
	Blocks       []Block `json:"Blocks"`
	ExpenseIndex int     `json:"ExpenseIndex"`
}

// ExpenseJob represents an asynchronous Textract expense analysis job.
type ExpenseJob struct {
	CreationTime     time.Time         `json:"creationTime"`
	JobID            string            `json:"jobId"`
	JobStatus        string            `json:"jobStatus"`
	ExpenseDocuments []ExpenseDocument `json:"expenseDocuments"`
}

// IdentityDocument represents a single identity document result.
type IdentityDocument struct {
	Blocks        []Block `json:"Blocks"`
	DocumentIndex int     `json:"DocumentIndex"`
}

// LendingResult represents a single lending analysis result page.
type LendingResult struct {
	Page int `json:"Page"`
}

// LendingJob represents an asynchronous Textract lending analysis job.
type LendingJob struct {
	CreationTime time.Time       `json:"creationTime"`
	JobID        string          `json:"jobId"`
	JobStatus    string          `json:"jobStatus"`
	Results      []LendingResult `json:"results"`
}

// Adapter represents a Textract Adapter.
type Adapter struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	AdapterID    string            `json:"adapterId"`
	AdapterName  string            `json:"adapterName"`
	AutoUpdate   string            `json:"autoUpdate"`
	Description  string            `json:"description"`
	FeatureTypes []string          `json:"featureTypes"`
}

// AdapterVersion represents a version of a Textract Adapter.
type AdapterVersion struct {
	CreationTime   time.Time         `json:"creationTime"`
	Tags           map[string]string `json:"tags"`
	AdapterID      string            `json:"adapterId"`
	AdapterVersion string            `json:"adapterVersion"`
	Status         string            `json:"status"`
	StatusMessage  string            `json:"statusMessage"`
	FeatureTypes   []string          `json:"featureTypes"`
}

// maxJobHistory is the maximum number of completed jobs retained in memory.
const maxJobHistory = 10000

// MaxJobHistory is the exported value for testing.
const MaxJobHistory = maxJobHistory

// allowedFeatureTypes returns the set of allowed feature type values for an adapter.
func allowedFeatureTypes() map[string]bool {
	return map[string]bool{
		featureTypeTables:     true,
		featureTypeForms:      true,
		featureTypeQueries:    true,
		featureTypeSignatures: true,
		featureTypeLayout:     true,
	}
}

// InMemoryBackend is the in-memory store for Textract jobs.
type InMemoryBackend struct {
	jobs            map[string]*DocumentJob
	expenseJobs     map[string]*ExpenseJob
	lendingJobs     map[string]*LendingJob
	adapters        map[string]*Adapter
	adapterVersions map[string]*AdapterVersion // key: adapterId+"#"+version
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
	maxJobs         int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		jobs:            make(map[string]*DocumentJob),
		expenseJobs:     make(map[string]*ExpenseJob),
		lendingJobs:     make(map[string]*LendingJob),
		adapters:        make(map[string]*Adapter),
		adapterVersions: make(map[string]*AdapterVersion),
		mu:              lockmetrics.New("textract"),
		accountID:       accountID,
		region:          region,
		maxJobs:         maxJobHistory,
	}
}

// AccountID returns the AWS account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored resources, resetting the backend to its initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.jobs = make(map[string]*DocumentJob)
	b.expenseJobs = make(map[string]*ExpenseJob)
	b.lendingJobs = make(map[string]*LendingJob)
	b.adapters = make(map[string]*Adapter)
	b.adapterVersions = make(map[string]*AdapterVersion)
}

const (
	confidencePage  = 99.0
	confidenceLine  = 99.5
	confidenceWord1 = 99.8
	confidenceWord2 = 99.7
	confidenceWord3 = 99.9
	blockTypeWord   = "WORD"
)

// syntheticBlocks returns a fixed set of synthetic text blocks for a document.
func syntheticBlocks(documentURI string) []Block {
	return []Block{
		{BlockType: "PAGE", Text: "", Confidence: confidencePage, ID: uuid.NewString()},
		{
			BlockType:  "LINE",
			Text:       "Synthetic extracted text from " + documentURI,
			Confidence: confidenceLine,
			ID:         uuid.NewString(),
		},
		{BlockType: blockTypeWord, Text: "Synthetic", Confidence: confidenceWord1, ID: uuid.NewString()},
		{BlockType: blockTypeWord, Text: "extracted", Confidence: confidenceWord2, ID: uuid.NewString()},
		{BlockType: blockTypeWord, Text: "text", Confidence: confidenceWord3, ID: uuid.NewString()},
	}
}

// cloneJob returns a deep copy of a DocumentJob.
func cloneJob(j *DocumentJob) *DocumentJob {
	cp := *j
	cp.Blocks = make([]Block, len(j.Blocks))
	copy(cp.Blocks, j.Blocks)

	return &cp
}

// cloneExpenseJob returns a deep copy of an ExpenseJob.
func cloneExpenseJob(j *ExpenseJob) *ExpenseJob {
	cp := *j
	cp.ExpenseDocuments = make([]ExpenseDocument, len(j.ExpenseDocuments))

	for i, doc := range j.ExpenseDocuments {
		docCopy := doc
		docCopy.Blocks = make([]Block, len(doc.Blocks))
		copy(docCopy.Blocks, doc.Blocks)
		cp.ExpenseDocuments[i] = docCopy
	}

	return &cp
}

// cloneLendingJob returns a deep copy of a LendingJob.
func cloneLendingJob(j *LendingJob) *LendingJob {
	cp := *j
	cp.Results = make([]LendingResult, len(j.Results))
	copy(cp.Results, j.Results)

	return &cp
}

// trimJobsIfNeeded removes the oldest jobs when the job count exceeds maxJobs.
// Caller must hold the write lock.
func (b *InMemoryBackend) trimJobsIfNeeded() {
	if len(b.jobs) <= b.maxJobs {
		return
	}

	// Collect jobs sorted by creation time (oldest first).
	type entry struct {
		job *DocumentJob
		id  string
	}

	entries := make([]entry, 0, len(b.jobs))
	for id, j := range b.jobs {
		entries = append(entries, entry{id: id, job: j})
	}

	sort.Slice(entries, func(i, k int) bool {
		return entries[i].job.CreationTime.Before(entries[k].job.CreationTime)
	})

	// Remove oldest entries until we are at the limit.
	excess := len(b.jobs) - b.maxJobs
	for i := range excess {
		delete(b.jobs, entries[i].id)
	}
}

// trimExpenseJobsIfNeeded bounds the expenseJobs map by evicting oldest entries.
// Caller must hold the write lock.
func (b *InMemoryBackend) trimExpenseJobsIfNeeded() {
	if len(b.expenseJobs) <= b.maxJobs {
		return
	}

	type entry struct {
		t  time.Time
		id string
	}

	entries := make([]entry, 0, len(b.expenseJobs))
	for id, j := range b.expenseJobs {
		entries = append(entries, entry{id: id, t: j.CreationTime})
	}

	sort.Slice(entries, func(i, k int) bool { return entries[i].t.Before(entries[k].t) })

	excess := len(b.expenseJobs) - b.maxJobs
	for i := range excess {
		delete(b.expenseJobs, entries[i].id)
	}
}

// trimLendingJobsIfNeeded bounds the lendingJobs map by evicting oldest entries.
// Caller must hold the write lock.
func (b *InMemoryBackend) trimLendingJobsIfNeeded() {
	if len(b.lendingJobs) <= b.maxJobs {
		return
	}

	type entry struct {
		t  time.Time
		id string
	}

	entries := make([]entry, 0, len(b.lendingJobs))
	for id, j := range b.lendingJobs {
		entries = append(entries, entry{id: id, t: j.CreationTime})
	}

	sort.Slice(entries, func(i, k int) bool { return entries[i].t.Before(entries[k].t) })

	excess := len(b.lendingJobs) - b.maxJobs
	for i := range excess {
		delete(b.lendingJobs, entries[i].id)
	}
}

// AnalyzeDocument performs a synchronous document analysis and returns synthetic blocks.
func (b *InMemoryBackend) AnalyzeDocument(documentURI string) []Block {
	return syntheticBlocks(documentURI)
}

// DetectDocumentText performs synchronous text detection and returns synthetic blocks.
func (b *InMemoryBackend) DetectDocumentText(documentURI string) []Block {
	return syntheticBlocks(documentURI)
}

// StartDocumentAnalysis creates an async document analysis job.
func (b *InMemoryBackend) StartDocumentAnalysis(documentURI string) (*DocumentJob, error) {
	b.mu.Lock("StartDocumentAnalysis")
	defer b.mu.Unlock()

	jobID := uuid.NewString()
	job := &DocumentJob{
		JobID:        jobID,
		JobStatus:    jobStatusSucceeded,
		JobType:      jobTypeDocumentAnalysis,
		CreationTime: time.Now(),
		Blocks:       syntheticBlocks(documentURI),
	}
	b.jobs[jobID] = job
	b.trimJobsIfNeeded()

	return cloneJob(job), nil
}

// GetDocumentAnalysis retrieves the results of a document analysis job.
// It returns a direct pointer to the stored job (lazy/CoW — no allocation on read path).
// Callers MUST NOT mutate the returned value.
func (b *InMemoryBackend) GetDocumentAnalysis(jobID string) (*DocumentJob, error) {
	b.mu.RLock("GetDocumentAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok || job.JobType != jobTypeDocumentAnalysis {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return job, nil
}

// StartDocumentTextDetection creates an async text detection job.
func (b *InMemoryBackend) StartDocumentTextDetection(documentURI string) (*DocumentJob, error) {
	b.mu.Lock("StartDocumentTextDetection")
	defer b.mu.Unlock()

	jobID := uuid.NewString()
	job := &DocumentJob{
		JobID:        jobID,
		JobStatus:    jobStatusSucceeded,
		JobType:      jobTypeTextDetection,
		CreationTime: time.Now(),
		Blocks:       syntheticBlocks(documentURI),
	}
	b.jobs[jobID] = job
	b.trimJobsIfNeeded()

	return cloneJob(job), nil
}

// GetDocumentTextDetection retrieves the results of a text detection job.
// It returns a direct pointer to the stored job (lazy/CoW — no allocation on read path).
// Callers MUST NOT mutate the returned value.
func (b *InMemoryBackend) GetDocumentTextDetection(jobID string) (*DocumentJob, error) {
	b.mu.RLock("GetDocumentTextDetection")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok || job.JobType != jobTypeTextDetection {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return job, nil
}

// ListJobs returns all stored jobs sorted by creation time (newest first).
func (b *InMemoryBackend) ListJobs() []DocumentJob {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	out := make([]DocumentJob, 0, len(b.jobs))
	for _, j := range b.jobs {
		out = append(out, *cloneJob(j))
	}

	// Sort newest first by creation time.
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreationTime.After(out[j].CreationTime)
	})

	return out
}

// adapterVersionKey returns the storage key for an adapter version.
func adapterVersionKey(adapterID, version string) string {
	return adapterID + "#" + version
}

// cloneAdapter returns a deep copy of an Adapter.
func cloneAdapter(a *Adapter) *Adapter {
	cp := *a
	cp.FeatureTypes = make([]string, len(a.FeatureTypes))
	copy(cp.FeatureTypes, a.FeatureTypes)
	cp.Tags = cloneTags(a.Tags)

	return &cp
}

// cloneAdapterVersion returns a deep copy of an AdapterVersion.
func cloneAdapterVersion(av *AdapterVersion) *AdapterVersion {
	cp := *av
	cp.FeatureTypes = make([]string, len(av.FeatureTypes))
	copy(cp.FeatureTypes, av.FeatureTypes)
	cp.Tags = cloneTags(av.Tags)

	return &cp
}

// cloneTags returns a non-nil copy of a tags map.
func cloneTags(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// AnalyzeExpense performs a synchronous expense analysis and returns synthetic expense documents.
func (b *InMemoryBackend) AnalyzeExpense(documentURI string) []ExpenseDocument {
	return []ExpenseDocument{
		{
			ExpenseIndex: 1,
			Blocks:       syntheticBlocks(documentURI),
		},
	}
}

// AnalyzeID performs a synchronous ID analysis and returns synthetic identity documents.
func (b *InMemoryBackend) AnalyzeID(documentURIs []string) []IdentityDocument {
	docs := make([]IdentityDocument, 0, len(documentURIs))
	for i, uri := range documentURIs {
		docs = append(docs, IdentityDocument{
			DocumentIndex: i + 1,
			Blocks:        syntheticBlocks(uri),
		})
	}

	return docs
}

// StartExpenseAnalysis creates an async expense analysis job.
func (b *InMemoryBackend) StartExpenseAnalysis(documentURI string) (*ExpenseJob, error) {
	b.mu.Lock("StartExpenseAnalysis")
	defer b.mu.Unlock()

	jobID := uuid.NewString()
	job := &ExpenseJob{
		JobID:        jobID,
		JobStatus:    jobStatusSucceeded,
		CreationTime: time.Now(),
		ExpenseDocuments: []ExpenseDocument{
			{
				ExpenseIndex: 1,
				Blocks:       syntheticBlocks(documentURI),
			},
		},
	}
	b.expenseJobs[jobID] = job
	b.trimExpenseJobsIfNeeded()

	return cloneExpenseJob(job), nil
}

// GetExpenseAnalysis retrieves the results of an expense analysis job.
// It returns a deep clone so callers may safely mutate the returned value.
func (b *InMemoryBackend) GetExpenseAnalysis(jobID string) (*ExpenseJob, error) {
	b.mu.RLock("GetExpenseAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.expenseJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: expense job %s not found", ErrJobNotFound, jobID)
	}

	return cloneExpenseJob(job), nil
}

// StartLendingAnalysis creates an async lending analysis job.
func (b *InMemoryBackend) StartLendingAnalysis(_ string) (*LendingJob, error) {
	b.mu.Lock("StartLendingAnalysis")
	defer b.mu.Unlock()

	jobID := uuid.NewString()
	job := &LendingJob{
		JobID:        jobID,
		JobStatus:    jobStatusSucceeded,
		CreationTime: time.Now(),
		Results:      []LendingResult{{Page: 1}},
	}
	b.lendingJobs[jobID] = job
	b.trimLendingJobsIfNeeded()

	return cloneLendingJob(job), nil
}

// GetLendingAnalysis retrieves the results of a lending analysis job.
// It returns a direct pointer to the stored job (lazy/CoW — no allocation on read path).
// Callers MUST NOT mutate the returned value.
func (b *InMemoryBackend) GetLendingAnalysis(jobID string) (*LendingJob, error) {
	b.mu.RLock("GetLendingAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.lendingJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: lending job %s not found", ErrJobNotFound, jobID)
	}

	return job, nil
}

// validateFeatureTypes checks that the given slice contains at least one valid feature type.
func validateFeatureTypes(featureTypes []string) error {
	if len(featureTypes) == 0 {
		return fmt.Errorf("%w: FeatureTypes must contain at least one value", ErrValidation)
	}

	for _, ft := range featureTypes {
		if !allowedFeatureTypes()[ft] {
			return fmt.Errorf(
				"%w: invalid FeatureType %q (valid: TABLES, FORMS, QUERIES, SIGNATURES, LAYOUT)",
				ErrValidation,
				ft,
			)
		}
	}

	return nil
}

// CreateAdapter creates a new Textract adapter and returns it.
func (b *InMemoryBackend) CreateAdapter(
	name, description, autoUpdate string,
	featureTypes []string,
	tags map[string]string,
) (*Adapter, error) {
	if err := validateFeatureTypes(featureTypes); err != nil {
		return nil, err
	}

	if autoUpdate == "" {
		autoUpdate = autoUpdateDisabled
	} else if autoUpdate != autoUpdateEnabled && autoUpdate != autoUpdateDisabled {
		return nil, fmt.Errorf(
			"%w: AutoUpdate must be ENABLED or DISABLED",
			ErrValidation,
		)
	}

	b.mu.Lock("CreateAdapter")
	defer b.mu.Unlock()

	adapterID := uuid.NewString()
	adapter := &Adapter{
		AdapterID:    adapterID,
		AdapterName:  name,
		AutoUpdate:   autoUpdate,
		CreationTime: time.Now(),
		Description:  description,
		FeatureTypes: append([]string{}, featureTypes...),
		Tags:         cloneTags(tags),
	}
	b.adapters[adapterID] = adapter

	return cloneAdapter(adapter), nil
}

// GetAdapter retrieves an adapter by ID.
func (b *InMemoryBackend) GetAdapter(adapterID string) (*Adapter, error) {
	b.mu.RLock("GetAdapter")
	defer b.mu.RUnlock()

	adapter, ok := b.adapters[adapterID]
	if !ok {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	return cloneAdapter(adapter), nil
}

// UpdateAdapter updates mutable fields on an existing adapter.
func (b *InMemoryBackend) UpdateAdapter(adapterID, description, autoUpdate string) (*Adapter, error) {
	if autoUpdate != "" && autoUpdate != autoUpdateEnabled && autoUpdate != autoUpdateDisabled {
		return nil, fmt.Errorf("%w: AutoUpdate must be ENABLED or DISABLED", ErrValidation)
	}

	b.mu.Lock("UpdateAdapter")
	defer b.mu.Unlock()

	adapter, ok := b.adapters[adapterID]
	if !ok {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	if description != "" {
		adapter.Description = description
	}

	if autoUpdate != "" {
		adapter.AutoUpdate = autoUpdate
	}

	return cloneAdapter(adapter), nil
}

// ListAdapters returns a sorted list of all adapters.
func (b *InMemoryBackend) ListAdapters() []Adapter {
	b.mu.RLock("ListAdapters")
	defer b.mu.RUnlock()

	out := make([]Adapter, 0, len(b.adapters))
	for _, a := range b.adapters {
		out = append(out, *cloneAdapter(a))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AdapterID < out[j].AdapterID
	})

	return out
}

// DeleteAdapter removes an adapter and all its versions by ID.
func (b *InMemoryBackend) DeleteAdapter(adapterID string) error {
	b.mu.Lock("DeleteAdapter")
	defer b.mu.Unlock()

	if _, ok := b.adapters[adapterID]; !ok {
		return fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	delete(b.adapters, adapterID)

	// Remove all versions belonging to this adapter.
	for key, av := range b.adapterVersions {
		if av.AdapterID == adapterID {
			delete(b.adapterVersions, key)
		}
	}

	return nil
}

// CreateAdapterVersion creates a new version for an existing adapter.
func (b *InMemoryBackend) CreateAdapterVersion(adapterID string, tags map[string]string) (*AdapterVersion, error) {
	b.mu.Lock("CreateAdapterVersion")
	defer b.mu.Unlock()

	adapter, ok := b.adapters[adapterID]
	if !ok {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	version := uuid.NewString()
	av := &AdapterVersion{
		AdapterID:      adapterID,
		AdapterVersion: version,
		CreationTime:   time.Now(),
		FeatureTypes:   append([]string{}, adapter.FeatureTypes...),
		Status:         adapterVersionActive,
		Tags:           cloneTags(tags),
	}
	b.adapterVersions[adapterVersionKey(adapterID, version)] = av

	return cloneAdapterVersion(av), nil
}

// GetAdapterVersion retrieves a specific adapter version.
func (b *InMemoryBackend) GetAdapterVersion(adapterID, version string) (*AdapterVersion, error) {
	b.mu.RLock("GetAdapterVersion")
	defer b.mu.RUnlock()

	av, ok := b.adapterVersions[adapterVersionKey(adapterID, version)]
	if !ok {
		return nil, fmt.Errorf("%w: adapter version %s/%s not found", ErrAdapterVersionNotFound, adapterID, version)
	}

	return cloneAdapterVersion(av), nil
}

// ListAdapterVersions returns all versions for a given adapter, sorted by version string.
func (b *InMemoryBackend) ListAdapterVersions(adapterID string) ([]AdapterVersion, error) {
	b.mu.RLock("ListAdapterVersions")
	defer b.mu.RUnlock()

	if _, ok := b.adapters[adapterID]; !ok {
		return nil, fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	out := make([]AdapterVersion, 0, len(b.adapterVersions))
	for _, av := range b.adapterVersions {
		if av.AdapterID == adapterID {
			out = append(out, *cloneAdapterVersion(av))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AdapterVersion < out[j].AdapterVersion
	})

	return out, nil
}

// DeleteAdapterVersion removes a specific adapter version.
func (b *InMemoryBackend) DeleteAdapterVersion(adapterID, version string) error {
	b.mu.Lock("DeleteAdapterVersion")
	defer b.mu.Unlock()

	key := adapterVersionKey(adapterID, version)
	if _, ok := b.adapterVersions[key]; !ok {
		return fmt.Errorf("%w: adapter version %s/%s not found", ErrAdapterVersionNotFound, adapterID, version)
	}

	delete(b.adapterVersions, key)

	return nil
}

// TagResource adds or replaces tags on an adapter.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	// We identify adapters by ARN (resourceARN). For simplicity we also accept
	// adapter IDs directly as the resource identifier.
	for _, a := range b.adapters {
		if a.AdapterID == resourceARN {
			maps.Copy(a.Tags, tags)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// UntagResource removes the specified tag keys from an adapter.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, a := range b.adapters {
		if a.AdapterID == resourceARN {
			for _, k := range tagKeys {
				delete(a.Tags, k)
			}

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// ListTagsForResource returns a copy of the tags for an adapter.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, a := range b.adapters {
		if a.AdapterID == resourceARN {
			return cloneTags(a.Tags), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// GetLendingAnalysisSummary returns a lightweight summary of a lending analysis job.
func (b *InMemoryBackend) GetLendingAnalysisSummary(jobID string) (*LendingJob, error) {
	b.mu.RLock("GetLendingAnalysisSummary")
	defer b.mu.RUnlock()

	job, ok := b.lendingJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: lending job %s not found", ErrJobNotFound, jobID)
	}

	// Return a summary: same job but without per-page lending results.
	cp := *job
	cp.Results = nil

	return &cp, nil
}
