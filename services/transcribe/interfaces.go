package transcribe

// StorageBackend defines the interface for Transcribe backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	StartTranscriptionJob(jobName, languageCode, mediaFileURI string) (*TranscriptionJob, error)
	GetTranscriptionJob(jobName string) (*TranscriptionJob, error)
	ListTranscriptionJobs(statusFilter, nextToken string) ([]TranscriptionJob, string)
	DeleteTranscriptionJob(jobName string) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
