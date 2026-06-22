package transcribe

import "context"

// StorageBackend defines the interface for Transcribe backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Transcription jobs
	StartTranscriptionJob(input *TranscriptionJob) (*TranscriptionJob, error)
	GetTranscriptionJob(jobName string) (*TranscriptionJob, error)
	ListTranscriptionJobs(statusFilter, nextToken string) ([]TranscriptionJob, string)
	DeleteTranscriptionJob(jobName string) error

	// Call Analytics categories
	CreateCallAnalyticsCategory(cat *CallAnalyticsCategory) (*CallAnalyticsCategory, error)
	DeleteCallAnalyticsCategory(categoryName string) error

	// Language models
	CreateLanguageModel(model *LanguageModel) (*LanguageModel, error)
	DeleteLanguageModel(modelName string) error

	// Vocabularies
	CreateMedicalVocabulary(vocabularyName, languageCode, vocabularyFileURI string) (*MedicalVocabulary, error)
	CreateVocabulary(vocab *Vocabulary) (*Vocabulary, error)
	CreateVocabularyFilter(vf *VocabularyFilter) (*VocabularyFilter, error)

	// Call Analytics jobs
	StartCallAnalyticsJob(job *CallAnalyticsJob) (*CallAnalyticsJob, error)
	GetCallAnalyticsJob(jobName string) (*CallAnalyticsJob, error)
	ListCallAnalyticsJobs(statusFilter, nextToken string) ([]CallAnalyticsJob, string)
	DeleteCallAnalyticsJob(jobName string) error

	// Call Analytics categories (Get/Update/List)
	GetCallAnalyticsCategory(categoryName string) (*CallAnalyticsCategory, error)
	UpdateCallAnalyticsCategory(cat *CallAnalyticsCategory) (*CallAnalyticsCategory, error)
	ListCallAnalyticsCategories(nextToken string) ([]CallAnalyticsCategory, string)

	// Vocabulary CRUD
	GetVocabulary(vocabularyName string) (*Vocabulary, error)
	UpdateVocabulary(vocab *Vocabulary) (*Vocabulary, error)
	DeleteVocabulary(vocabularyName string) error
	ListVocabularies(stateFilter, nextToken string) ([]Vocabulary, string)

	// Vocabulary filter CRUD
	GetVocabularyFilter(vocabularyFilterName string) (*VocabularyFilter, error)
	UpdateVocabularyFilter(vf *VocabularyFilter) (*VocabularyFilter, error)
	DeleteVocabularyFilter(vocabularyFilterName string) error
	ListVocabularyFilters(nextToken string) ([]VocabularyFilter, string)

	// Medical vocabulary CRUD
	GetMedicalVocabulary(vocabularyName string) (*MedicalVocabulary, error)
	UpdateMedicalVocabulary(vocabularyName, languageCode, vocabularyFileURI string) (*MedicalVocabulary, error)
	DeleteMedicalVocabulary(vocabularyName string) error
	ListMedicalVocabularies(stateFilter, nextToken string) ([]MedicalVocabulary, string)

	// Medical Scribe jobs
	StartMedicalScribeJob(job *MedicalScribeJob) (*MedicalScribeJob, error)
	GetMedicalScribeJob(jobName string) (*MedicalScribeJob, error)
	ListMedicalScribeJobs(statusFilter, nextToken string) ([]MedicalScribeJob, string)
	DeleteMedicalScribeJob(jobName string) error

	// Medical Transcription jobs
	StartMedicalTranscriptionJob(job *MedicalTranscriptionJob) (*MedicalTranscriptionJob, error)
	GetMedicalTranscriptionJob(jobName string) (*MedicalTranscriptionJob, error)
	ListMedicalTranscriptionJobs(statusFilter, nextToken string) ([]MedicalTranscriptionJob, string)
	DeleteMedicalTranscriptionJob(jobName string) error

	// Language model CRUD
	DescribeLanguageModel(modelName string) (*LanguageModel, error)
	ListLanguageModels(statusFilter, nextToken string) ([]LanguageModel, string)

	// Tags
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error
	ListTagsForResource(resourceArn string) (map[string]string, error)

	// Lifecycle
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
