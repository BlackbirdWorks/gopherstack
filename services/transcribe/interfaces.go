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

	// Call Analytics jobs
	StartCallAnalyticsJob(jobName, languageCode, mediaFileURI string) (*CallAnalyticsJob, error)
	GetCallAnalyticsJob(jobName string) (*CallAnalyticsJob, error)
	ListCallAnalyticsJobs(statusFilter, nextToken string) ([]CallAnalyticsJob, string)
	DeleteCallAnalyticsJob(jobName string) error

	// Call Analytics categories (Get/Update/List)
	GetCallAnalyticsCategory(categoryName string) (*CallAnalyticsCategory, error)
	UpdateCallAnalyticsCategory(categoryName, inputType string) (*CallAnalyticsCategory, error)
	ListCallAnalyticsCategories(nextToken string) ([]CallAnalyticsCategory, string)

	// Vocabulary CRUD
	GetVocabulary(vocabularyName string) (*Vocabulary, error)
	UpdateVocabulary(vocabularyName, languageCode string) (*Vocabulary, error)
	DeleteVocabulary(vocabularyName string) error
	ListVocabularies(stateFilter, nextToken string) ([]Vocabulary, string)

	// Vocabulary filter CRUD
	GetVocabularyFilter(vocabularyFilterName string) (*VocabularyFilter, error)
	UpdateVocabularyFilter(vocabularyFilterName, languageCode string) (*VocabularyFilter, error)
	DeleteVocabularyFilter(vocabularyFilterName string) error
	ListVocabularyFilters(nextToken string) ([]VocabularyFilter, string)

	// Medical vocabulary CRUD
	GetMedicalVocabulary(vocabularyName string) (*MedicalVocabulary, error)
	UpdateMedicalVocabulary(vocabularyName, languageCode, vocabularyFileURI string) (*MedicalVocabulary, error)
	DeleteMedicalVocabulary(vocabularyName string) error
	ListMedicalVocabularies(stateFilter, nextToken string) ([]MedicalVocabulary, string)

	// Medical Scribe jobs
	StartMedicalScribeJob(jobName, mediaFileURI string) (*MedicalScribeJob, error)
	GetMedicalScribeJob(jobName string) (*MedicalScribeJob, error)
	ListMedicalScribeJobs(statusFilter, nextToken string) ([]MedicalScribeJob, string)
	DeleteMedicalScribeJob(jobName string) error

	// Medical Transcription jobs
	StartMedicalTranscriptionJob(jobName, languageCode, mediaFileURI string) (*MedicalTranscriptionJob, error)
	GetMedicalTranscriptionJob(jobName string) (*MedicalTranscriptionJob, error)
	ListMedicalTranscriptionJobs(statusFilter, nextToken string) ([]MedicalTranscriptionJob, string)
	DeleteMedicalTranscriptionJob(jobName string) error

	// Language model CRUD
	DescribeLanguageModel(modelName string) (*LanguageModel, error)
	ListLanguageModels(statusFilter, nextToken string) ([]LanguageModel, string)

	// Lifecycle
	Snapshot() []byte
	Restore(data []byte) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
