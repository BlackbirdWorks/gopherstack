package transcribe

import (
	"fmt"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a transcription job is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a transcription job already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
)

// TranscriptionJob represents an Amazon Transcribe transcription job.
type TranscriptionJob struct {
	CreationTime   time.Time
	CompletionTime time.Time
	JobName        string
	JobStatus      string
	LanguageCode   string
	MediaFileURI   string
	TranscriptText string
}

// InMemoryBackend is the in-memory store for Transcribe jobs.
type InMemoryBackend struct {
	jobs map[string]*TranscriptionJob
	mu   sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		jobs: make(map[string]*TranscriptionJob),
	}
}

// StartTranscriptionJob creates a new transcription job with synthetic results.
func (b *InMemoryBackend) StartTranscriptionJob(jobName, languageCode, mediaFileURI string) (*TranscriptionJob, error) {
	b.mu.Lock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobName]
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	cp := *job

	return &cp, nil
}

// ListTranscriptionJobs returns all transcription jobs, optionally filtered by status.
func (b *InMemoryBackend) ListTranscriptionJobs(statusFilter string) []TranscriptionJob {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]TranscriptionJob, 0, len(b.jobs))
	for _, j := range b.jobs {
		if statusFilter == "" || j.JobStatus == statusFilter {
			out = append(out, *j)
		}
	}

	return out
}
