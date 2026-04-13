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
)

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

// adapterVersionActive is the status for a ready adapter version.
const adapterVersionActive = "ACTIVE"

// InMemoryBackend is the in-memory store for Textract jobs.
type InMemoryBackend struct {
	jobs            map[string]*DocumentJob
	expenseJobs     map[string]*ExpenseJob
	lendingJobs     map[string]*LendingJob
	adapters        map[string]*Adapter
	adapterVersions map[string]*AdapterVersion // key: adapterId+"#"+version
	mu              *lockmetrics.RWMutex
	maxJobs         int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		jobs:            make(map[string]*DocumentJob),
		expenseJobs:     make(map[string]*ExpenseJob),
		lendingJobs:     make(map[string]*LendingJob),
		adapters:        make(map[string]*Adapter),
		adapterVersions: make(map[string]*AdapterVersion),
		mu:              lockmetrics.New("textract"),
		maxJobs:         maxJobHistory,
	}
}

const (
	confidencePage  = 99.0
	confidenceLine  = 99.5
	confidenceWord1 = 99.8
	confidenceWord2 = 99.7
	confidenceWord3 = 99.9
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
		{BlockType: "WORD", Text: "Synthetic", Confidence: confidenceWord1, ID: uuid.NewString()},
		{BlockType: "WORD", Text: "extracted", Confidence: confidenceWord2, ID: uuid.NewString()},
		{BlockType: "WORD", Text: "text", Confidence: confidenceWord3, ID: uuid.NewString()},
	}
}

// cloneJob returns a deep copy of a DocumentJob.
func cloneJob(j *DocumentJob) *DocumentJob {
	cp := *j
	cp.Blocks = make([]Block, len(j.Blocks))
	copy(cp.Blocks, j.Blocks)

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
		JobStatus:    "SUCCEEDED",
		JobType:      "DocumentAnalysis",
		CreationTime: time.Now(),
		Blocks:       syntheticBlocks(documentURI),
	}
	b.jobs[jobID] = job
	b.trimJobsIfNeeded()

	return cloneJob(job), nil
}

// GetDocumentAnalysis retrieves the results of a document analysis job.
func (b *InMemoryBackend) GetDocumentAnalysis(jobID string) (*DocumentJob, error) {
	b.mu.RLock("GetDocumentAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok || job.JobType != "DocumentAnalysis" {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return cloneJob(job), nil
}

// StartDocumentTextDetection creates an async text detection job.
func (b *InMemoryBackend) StartDocumentTextDetection(documentURI string) (*DocumentJob, error) {
	b.mu.Lock("StartDocumentTextDetection")
	defer b.mu.Unlock()

	jobID := uuid.NewString()
	job := &DocumentJob{
		JobID:        jobID,
		JobStatus:    "SUCCEEDED",
		JobType:      "TextDetection",
		CreationTime: time.Now(),
		Blocks:       syntheticBlocks(documentURI),
	}
	b.jobs[jobID] = job
	b.trimJobsIfNeeded()

	return cloneJob(job), nil
}

// GetDocumentTextDetection retrieves the results of a text detection job.
func (b *InMemoryBackend) GetDocumentTextDetection(jobID string) (*DocumentJob, error) {
	b.mu.RLock("GetDocumentTextDetection")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok || job.JobType != "TextDetection" {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return cloneJob(job), nil
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

// cloneTags returns a shallow copy of a tags map.
func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return map[string]string{}
	}

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

// CreateAdapter creates a new Textract adapter and returns it.
func (b *InMemoryBackend) CreateAdapter(
	name, description string,
	featureTypes []string,
	tags map[string]string,
) (*Adapter, error) {
	b.mu.Lock("CreateAdapter")
	defer b.mu.Unlock()

	adapterID := uuid.NewString()
	adapter := &Adapter{
		AdapterID:    adapterID,
		AdapterName:  name,
		AutoUpdate:   "DISABLED",
		CreationTime: time.Now(),
		Description:  description,
		FeatureTypes: featureTypes,
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

// DeleteAdapter removes an adapter and all its versions by ID.
func (b *InMemoryBackend) DeleteAdapter(adapterID string) error {
	b.mu.Lock("DeleteAdapter")
	defer b.mu.Unlock()

	if _, ok := b.adapters[adapterID]; !ok {
		return fmt.Errorf("%w: adapter %s not found", ErrAdapterNotFound, adapterID)
	}

	delete(b.adapters, adapterID)

	// Remove all versions belonging to this adapter.
	for key := range b.adapterVersions {
		if b.adapterVersions[key].AdapterID == adapterID {
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

// GetExpenseAnalysis retrieves the results of an expense analysis job.
func (b *InMemoryBackend) GetExpenseAnalysis(jobID string) (*ExpenseJob, error) {
	b.mu.RLock("GetExpenseAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.expenseJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: expense job %s not found", ErrJobNotFound, jobID)
	}

	cp := *job
	cp.ExpenseDocuments = make([]ExpenseDocument, len(job.ExpenseDocuments))
	copy(cp.ExpenseDocuments, job.ExpenseDocuments)

	return &cp, nil
}

// GetLendingAnalysis retrieves the results of a lending analysis job.
func (b *InMemoryBackend) GetLendingAnalysis(jobID string) (*LendingJob, error) {
	b.mu.RLock("GetLendingAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.lendingJobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: lending job %s not found", ErrJobNotFound, jobID)
	}

	cp := *job
	cp.Results = make([]LendingResult, len(job.Results))
	copy(cp.Results, job.Results)

	return &cp, nil
}
