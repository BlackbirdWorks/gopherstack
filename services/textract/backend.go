package textract

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrJobNotFound is returned when a document job is not found.
	ErrJobNotFound = awserr.New("InvalidJobIdException", awserr.ErrNotFound)
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

// InMemoryBackend is the in-memory store for Textract jobs.
type InMemoryBackend struct {
	jobs map[string]*DocumentJob
	mu   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		jobs: make(map[string]*DocumentJob),
		mu:   lockmetrics.New("textract"),
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
