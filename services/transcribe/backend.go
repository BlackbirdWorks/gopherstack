package transcribe

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a transcription job is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a transcription job already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
)

const transcribeDefaultPageSize = 100

// TranscriptionJob represents an Amazon Transcribe transcription job.
type TranscriptionJob struct {
	CreationTime   time.Time `json:"creationTime"`
	CompletionTime time.Time `json:"completionTime"`
	JobName        string    `json:"jobName"`
	JobStatus      string    `json:"jobStatus"`
	LanguageCode   string    `json:"languageCode"`
	MediaFileURI   string    `json:"mediaFileURI"`
	TranscriptText string    `json:"transcriptText"`
}

// InMemoryBackend is the in-memory store for Transcribe jobs.
type InMemoryBackend struct {
	jobs map[string]*TranscriptionJob
	mu   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		jobs: make(map[string]*TranscriptionJob),
		mu:   lockmetrics.New("transcribe"),
	}
}

// StartTranscriptionJob creates a new transcription job with synthetic results.
func (b *InMemoryBackend) StartTranscriptionJob(jobName, languageCode, mediaFileURI string) (*TranscriptionJob, error) {
	b.mu.Lock("StartTranscriptionJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[jobName]; ok {
		return nil, fmt.Errorf("%w: job %s already exists", ErrAlreadyExists, jobName)
	}

	now := time.Now()
	job := &TranscriptionJob{
		JobName:        jobName,
		JobStatus:      "COMPLETED",
		LanguageCode:   languageCode,
		MediaFileURI:   mediaFileURI,
		TranscriptText: "This is a synthetic transcription result for " + jobName + ".",
		CreationTime:   now,
		CompletionTime: now,
	}
	b.jobs[jobName] = job

	cp := *job

	return &cp, nil
}

// GetTranscriptionJob returns a transcription job by name.
func (b *InMemoryBackend) GetTranscriptionJob(jobName string) (*TranscriptionJob, error) {
	b.mu.RLock("GetTranscriptionJob")
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobName]
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	cp := *job

	return &cp, nil
}

// ListTranscriptionJobs returns transcription jobs, optionally filtered by status, with pagination.
func (b *InMemoryBackend) ListTranscriptionJobs(statusFilter, nextToken string) ([]TranscriptionJob, string) {
	b.mu.RLock("ListTranscriptionJobs")
	defer b.mu.RUnlock()

	all := make([]TranscriptionJob, 0, len(b.jobs))
	for _, j := range b.jobs {
		if statusFilter == "" || j.JobStatus == statusFilter {
			all = append(all, *j)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].JobName < all[j].JobName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []TranscriptionJob{}, ""
	}
	end := startIdx + transcribeDefaultPageSize
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

// DeleteTranscriptionJob removes a transcription job by name.
func (b *InMemoryBackend) DeleteTranscriptionJob(jobName string) error {
	b.mu.Lock("DeleteTranscriptionJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[jobName]; !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	delete(b.jobs, jobName)

	return nil
}

// parseNextToken parses a pagination token (integer offset) into a slice index.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}
