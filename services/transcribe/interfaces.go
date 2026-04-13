package transcribe

// StorageBackend defines the interface for Transcribe backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Transcription jobs
	StartTranscriptionJob(jobName, languageCode, mediaFileURI string) (*TranscriptionJob, error)
	GetTranscriptionJob(jobName string) (*TranscriptionJob, error)
	ListTranscriptionJobs(statusFilter, nextToken string) ([]TranscriptionJob, string)
	DeleteTranscriptionJob(jobName string) error

	// Call Analytics categories
	CreateCallAnalyticsCategory(categoryName, inputType string) (*CallAnalyticsCategory, error)
	DeleteCallAnalyticsCategory(categoryName string) error

	// Language models
	CreateLanguageModel(modelName, baseModelName, languageCode string) (*LanguageModel, error)
	DeleteLanguageModel(modelName string) error

	// Vocabularies
	CreateMedicalVocabulary(vocabularyName, languageCode, vocabularyFileURI string) (*MedicalVocabulary, error)
	CreateVocabulary(vocabularyName, languageCode string) (*Vocabulary, error)
	CreateVocabularyFilter(vocabularyFilterName, languageCode string) (*VocabularyFilter, error)

	// Job cleanup
	DeleteCallAnalyticsJob(jobName string) error
	DeleteMedicalScribeJob(jobName string) error
	DeleteMedicalTranscriptionJob(jobName string) error

	// Lifecycle
	Snapshot() []byte
	Restore(data []byte) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
