package transcribe

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	// ErrNotFound is returned when a transcription job is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the given name already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid or missing input parameters.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrVocabularyNotFound is returned when a vocabulary is not found.
	// Real AWS returns BadRequestException (400) for missing vocabularies, not NotFoundException (404).
	ErrVocabularyNotFound = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

const (
	transcribeDefaultPageSize = 100

	// job/scribe status constants.
	jobStatusQueued     = "QUEUED"
	jobStatusInProgress = "IN_PROGRESS"
	jobStatusCompleted  = "COMPLETED"
	jobStatusFailed     = "FAILED"

	// vocabulary state constants.
	vocabStateReady = "READY"

	// language model status constant – gopherstack immediately completes models.
	modelStatusCompleted = "COMPLETED"

	// defaultAccountID is the synthetic AWS account ID used by the in-memory backend.
	defaultAccountID = "123456789012"
)

// TranscriptionJob represents an Amazon Transcribe transcription job.
type TranscriptionJob struct {
	StartTime                 time.Time                   `json:"startTime"`
	CompletionTime            time.Time                   `json:"completionTime"`
	CreationTime              time.Time                   `json:"creationTime"`
	Tags                      map[string]string           `json:"tags,omitempty"`
	Subtitles                 *SubtitlesOutput            `json:"subtitles,omitempty"`
	ContentRedaction          *ContentRedaction           `json:"contentRedaction,omitempty"`
	ModelSettings             *ModelSettings              `json:"modelSettings,omitempty"`
	JobExecutionSettings      *JobExecutionSettings       `json:"jobExecutionSettings,omitempty"`
	Settings                  *TranscriptionSettings      `json:"settings,omitempty"`
	Media                     Media                       `json:"media"`
	LanguageCode              string                      `json:"languageCode"`
	JobStatus                 string                      `json:"jobStatus"`
	MediaFormat               string                      `json:"mediaFormat,omitempty"`
	OutputBucketName          string                      `json:"outputBucketName,omitempty"`
	OutputKey                 string                      `json:"outputKey,omitempty"`
	OutputEncryptionKMSKeyID  string                      `json:"outputEncryptionKMSKeyId,omitempty"`
	TranscriptText            string                      `json:"transcriptText"`
	JobName                   string                      `json:"jobName"`
	FailureReason             string                      `json:"failureReason,omitempty"`
	LanguageOptions           []string                    `json:"languageOptions,omitempty"`
	ToxicityDetection         []ToxicityDetectionSettings `json:"toxicityDetection,omitempty"`
	LanguageCodes             []LanguageCodeItem          `json:"languageCodes,omitempty"`
	TranscriptJSON            []byte                      `json:"-"`
	MediaSampleRateHertz      int32                       `json:"mediaSampleRateHertz,omitempty"`
	IdentifiedLanguageScore   float32                     `json:"identifiedLanguageScore,omitempty"`
	IdentifyLanguage          bool                        `json:"identifyLanguage,omitempty"`
	IdentifyMultipleLanguages bool                        `json:"identifyMultipleLanguages,omitempty"`
}

// CallAnalyticsCategory represents an Amazon Transcribe Call Analytics category.
type CallAnalyticsCategory struct {
	CreateTime     time.Time           `json:"createTime"`
	LastUpdateTime time.Time           `json:"lastUpdateTime"`
	CategoryName   string              `json:"categoryName"`
	InputType      string              `json:"inputType"`
	Rules          []CallAnalyticsRule `json:"rules,omitempty"`
}

// LanguageModel represents a custom Amazon Transcribe language model.
type LanguageModel struct {
	CreateTime          time.Time        `json:"createTime"`
	LastModifiedTime    time.Time        `json:"lastModifiedTime"`
	InputDataConfig     *InputDataConfig `json:"inputDataConfig,omitempty"`
	ModelName           string           `json:"modelName"`
	BaseModelName       string           `json:"baseModelName"`
	LanguageCode        string           `json:"languageCode"`
	ModelStatus         string           `json:"modelStatus"`
	UpgradeAvailability bool             `json:"upgradeAvailability,omitempty"`
}

// MedicalVocabulary represents an Amazon Transcribe Medical custom vocabulary.
type MedicalVocabulary struct {
	LastModifiedTime  time.Time `json:"lastModifiedTime"`
	VocabularyName    string    `json:"vocabularyName"`
	LanguageCode      string    `json:"languageCode"`
	VocabularyState   string    `json:"vocabularyState"`
	VocabularyFileURI string    `json:"vocabularyFileUri"`
}

// Vocabulary represents an Amazon Transcribe custom vocabulary.
type Vocabulary struct {
	LastModifiedTime  time.Time `json:"lastModifiedTime"`
	VocabularyName    string    `json:"vocabularyName"`
	LanguageCode      string    `json:"languageCode"`
	VocabularyState   string    `json:"vocabularyState"`
	VocabularyFileURI string    `json:"vocabularyFileUri,omitempty"`
	Phrases           []string  `json:"phrases,omitempty"`
}

// VocabularyFilter represents an Amazon Transcribe custom vocabulary filter.
type VocabularyFilter struct {
	LastModifiedTime        time.Time `json:"lastModifiedTime"`
	VocabularyFilterName    string    `json:"vocabularyFilterName"`
	LanguageCode            string    `json:"languageCode"`
	VocabularyFilterFileURI string    `json:"vocabularyFilterFileUri,omitempty"`
	Words                   []string  `json:"words,omitempty"`
}

// CallAnalyticsJob represents an Amazon Transcribe Call Analytics job.
type CallAnalyticsJob struct {
	StartTime               time.Time              `json:"startTime"`
	CompletionTime          time.Time              `json:"completionTime"`
	CreationTime            time.Time              `json:"creationTime"`
	Tags                    map[string]string      `json:"tags,omitempty"`
	Settings                *CallAnalyticsSettings `json:"settings,omitempty"`
	Media                   Media                  `json:"media"`
	CallAnalyticsJobStatus  string                 `json:"callAnalyticsJobStatus"`
	LanguageCode            string                 `json:"languageCode"`
	MediaFormat             string                 `json:"mediaFormat,omitempty"`
	DataAccessRoleArn       string                 `json:"dataAccessRoleArn,omitempty"`
	FailureReason           string                 `json:"failureReason,omitempty"`
	CallAnalyticsJobName    string                 `json:"callAnalyticsJobName"`
	ChannelDefinitions      []ChannelDefinition    `json:"channelDefinitions,omitempty"`
	TranscriptJSON          []byte                 `json:"-"`
	IdentifiedLanguageScore float32                `json:"identifiedLanguageScore,omitempty"`
	MediaSampleRateHertz    int32                  `json:"mediaSampleRateHertz,omitempty"`
}

// MedicalScribeJob represents an Amazon Transcribe Medical Scribe job.
type MedicalScribeJob struct {
	StartTime                      time.Time                        `json:"startTime"`
	CompletionTime                 time.Time                        `json:"completionTime"`
	CreationTime                   time.Time                        `json:"creationTime"`
	Tags                           map[string]string                `json:"tags,omitempty"`
	ClinicalNoteGenerationSettings *ClinicalNoteGenerationSettings  `json:"clinicalNoteGenerationSettings,omitempty"`
	Settings                       *MedicalScribeSettings           `json:"settings,omitempty"`
	Media                          Media                            `json:"media"`
	LanguageCode                   string                           `json:"languageCode,omitempty"`
	DataAccessRoleArn              string                           `json:"dataAccessRoleArn,omitempty"`
	OutputBucketName               string                           `json:"outputBucketName,omitempty"`
	FailureReason                  string                           `json:"failureReason,omitempty"`
	MedicalScribeJobStatus         string                           `json:"medicalScribeJobStatus"`
	MedicalScribeJobName           string                           `json:"medicalScribeJobName"`
	ChannelDefinitions             []MedicalScribeChannelDefinition `json:"channelDefinitions,omitempty"`
}

// MedicalTranscriptionJob represents an Amazon Transcribe Medical transcription job.
type MedicalTranscriptionJob struct {
	CreationTime                     time.Time                     `json:"creationTime"`
	StartTime                        time.Time                     `json:"startTime"`
	CompletionTime                   time.Time                     `json:"completionTime"`
	Tags                             map[string]string             `json:"tags,omitempty"`
	Settings                         *MedicalTranscriptionSettings `json:"settings,omitempty"`
	Media                            Media                         `json:"media"`
	Type                             string                        `json:"type,omitempty"`
	Specialty                        string                        `json:"specialty,omitempty"`
	LanguageCode                     string                        `json:"languageCode"`
	MediaFormat                      string                        `json:"mediaFormat,omitempty"`
	FailureReason                    string                        `json:"failureReason,omitempty"`
	OutputBucketName                 string                        `json:"outputBucketName,omitempty"`
	OutputKey                        string                        `json:"outputKey,omitempty"`
	TranscriptionJobStatus           string                        `json:"transcriptionJobStatus"`
	MedicalContentIdentificationType string                        `json:"medicalContentIdentificationType,omitempty"`
	MedicalTranscriptionJobName      string                        `json:"medicalTranscriptionJobName"`
	TranscriptJSON                   []byte                        `json:"-"`
	MediaSampleRateHertz             int32                         `json:"mediaSampleRateHertz,omitempty"`
}

// InMemoryBackend is the in-memory store for Transcribe jobs.
type InMemoryBackend struct {
	jobs                     *store.Table[TranscriptionJob]
	callAnalyticsCategories  *store.Table[CallAnalyticsCategory]
	languageModels           *store.Table[LanguageModel]
	medicalVocabularies      *store.Table[MedicalVocabulary]
	vocabularies             *store.Table[Vocabulary]
	vocabularyFilters        *store.Table[VocabularyFilter]
	callAnalyticsJobs        *store.Table[CallAnalyticsJob]
	medicalScribeJobs        *store.Table[MedicalScribeJob]
	medicalTranscriptionJobs *store.Table[MedicalTranscriptionJob]
	registry                 *store.Registry
	resourceTags             map[string]map[string]string // ARN → tag map
	mu                       *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("transcribe"),
		registry: store.NewRegistry(),
	}
	registerAllTables(b)
	b.resourceTags = make(map[string]map[string]string)

	return b
}

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.resourceTags = make(map[string]map[string]string)
}

// AccountID returns the synthetic AWS account ID used by this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

// StartTranscriptionJob creates a new transcription job with synthetic results.
// The input struct carries all supported fields; validation is performed before storage.
func (b *InMemoryBackend) StartTranscriptionJob(input *TranscriptionJob) (*TranscriptionJob, error) {
	if err := validateTranscriptionJobInput(input); err != nil {
		return nil, err
	}

	b.mu.Lock("StartTranscriptionJob")
	defer b.mu.Unlock()

	if b.jobs.Has(input.JobName) {
		return nil, fmt.Errorf("%w: job %s already exists", ErrAlreadyExists, input.JobName)
	}

	job := buildCompletedTranscriptionJob(input)
	if input.JobExecutionSettings != nil && input.JobExecutionSettings.AllowDeferredExecution {
		job = buildQueuedTranscriptionJob(input)
	}

	b.jobs.Put(&job)
	cp := job

	return &cp, nil
}

// buildQueuedTranscriptionJob initialises a deferred job before execution starts.
func buildQueuedTranscriptionJob(input *TranscriptionJob) TranscriptionJob {
	job := *input
	job.JobStatus = jobStatusQueued
	job.CreationTime = time.Now()

	return job
}

// buildCompletedTranscriptionJob initialises a job as COMPLETED with synthetic results.
func buildCompletedTranscriptionJob(input *TranscriptionJob) TranscriptionJob {
	now := time.Now()
	mediaURI := ""
	if input.Media.MediaFileURI != "" {
		mediaURI = input.Media.MediaFileURI
	}
	transcriptText := deriveTranscriptText(input.JobName, mediaURI)

	job := *input
	job.JobStatus = jobStatusCompleted
	job.CreationTime = now
	job.StartTime = now
	job.CompletionTime = now
	job.TranscriptText = transcriptText
	job.TranscriptJSON = synthesizeTranscriptJSON(input.JobName, transcriptText)

	// Synthesize subtitle file URIs when subtitles were requested.
	if input.Subtitles != nil && len(input.Subtitles.Formats) > 0 {
		uris := make([]string, 0, len(input.Subtitles.Formats))
		for _, f := range input.Subtitles.Formats {
			uris = append(uris, "s3://synthetic-transcripts/"+input.JobName+"."+f)
		}

		job.Subtitles = &SubtitlesOutput{
			Formats:          input.Subtitles.Formats,
			SubtitleFileURIs: uris,
			OutputStartIndex: input.Subtitles.OutputStartIndex,
		}
	}

	// Synthesize identified language score when language identification is enabled.
	if input.IdentifyLanguage || input.IdentifyMultipleLanguages {
		job.LanguageCode = resolveEffectiveLanguageCode(input.LanguageCode, input.LanguageOptions)
		job.IdentifiedLanguageScore = syntheticIdentifiedLanguageScore
	}

	return job
}

func advanceDeferredTranscriptionJob(job *TranscriptionJob) {
	switch job.JobStatus {
	case jobStatusQueued:
		job.JobStatus = jobStatusInProgress
		job.StartTime = time.Now()
	case jobStatusInProgress:
		job.CompletionTime = time.Now()
		if job.FailureReason != "" {
			job.JobStatus = jobStatusFailed

			return
		}

		completed := buildCompletedTranscriptionJob(job)
		completed.CreationTime = job.CreationTime
		completed.StartTime = job.StartTime
		completed.CompletionTime = time.Now()
		*job = completed
	}
}

// syntheticIdentifiedLanguageScore is the confidence score returned when language identification is enabled.
const syntheticIdentifiedLanguageScore = float32(0.97)

// resolveEffectiveLanguageCode returns the best language code to use when language identification is on.
func resolveEffectiveLanguageCode(languageCode string, options []string) string {
	if languageCode != "" {
		return languageCode
	}

	if len(options) > 0 {
		return options[0]
	}

	return "en-US"
}

// subtitlesInputFromOutput converts a SubtitlesOutput back to input for validation.
func subtitlesInputFromOutput(s *SubtitlesOutput) *SubtitlesInput {
	if s == nil {
		return nil
	}

	return &SubtitlesInput{Formats: s.Formats, OutputStartIndex: s.OutputStartIndex}
}

// GetTranscriptionJob returns a transcription job by name.
func (b *InMemoryBackend) GetTranscriptionJob(jobName string) (*TranscriptionJob, error) {
	b.mu.Lock("GetTranscriptionJob")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	advanceDeferredTranscriptionJob(job)
	cp := *job

	return &cp, nil
}

// ListTranscriptionJobs returns transcription jobs, optionally filtered by status, with pagination.
func (b *InMemoryBackend) ListTranscriptionJobs(
	statusFilter, nextToken string,
) ([]TranscriptionJob, string) {
	b.mu.RLock("ListTranscriptionJobs")
	defer b.mu.RUnlock()

	all := make([]TranscriptionJob, 0, b.jobs.Len())
	for _, j := range b.jobs.All() {
		if statusFilter == "" || j.JobStatus == statusFilter {
			all = append(all, *j)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].JobName < all[j].JobName })

	return paginateList(all, nextToken)
}

// DeleteTranscriptionJob removes a transcription job by name.
func (b *InMemoryBackend) DeleteTranscriptionJob(jobName string) error {
	if jobName == "" {
		return fmt.Errorf("%w: TranscriptionJobName is required", ErrValidation)
	}

	b.mu.Lock("DeleteTranscriptionJob")
	defer b.mu.Unlock()

	if !b.jobs.Delete(jobName) {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	return nil
}

// CreateCallAnalyticsCategory creates a new Call Analytics category.
func (b *InMemoryBackend) CreateCallAnalyticsCategory(input *CallAnalyticsCategory) (*CallAnalyticsCategory, error) {
	if input.CategoryName == "" {
		return nil, fmt.Errorf("%w: CategoryName is required", ErrValidation)
	}

	if err := validateCallAnalyticsInputType(input.InputType); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCallAnalyticsCategory")
	defer b.mu.Unlock()

	if b.callAnalyticsCategories.Has(input.CategoryName) {
		return nil, fmt.Errorf("%w: category %s already exists", ErrAlreadyExists, input.CategoryName)
	}

	now := time.Now()
	cat := *input
	cat.CreateTime = now
	cat.LastUpdateTime = now
	b.callAnalyticsCategories.Put(&cat)

	cp := cat

	return &cp, nil
}

// DeleteCallAnalyticsCategory removes a Call Analytics category by name.
func (b *InMemoryBackend) DeleteCallAnalyticsCategory(categoryName string) error {
	if categoryName == "" {
		return fmt.Errorf("%w: CategoryName is required", ErrValidation)
	}

	b.mu.Lock("DeleteCallAnalyticsCategory")
	defer b.mu.Unlock()

	if !b.callAnalyticsCategories.Delete(categoryName) {
		return fmt.Errorf("%w: category %s not found", ErrNotFound, categoryName)
	}

	return nil
}

// CreateLanguageModel creates a new custom language model.
func (b *InMemoryBackend) CreateLanguageModel(input *LanguageModel) (*LanguageModel, error) {
	if input.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", ErrValidation)
	}

	if err := validateBaseModelName(input.BaseModelName); err != nil {
		return nil, err
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if input.LanguageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	if input.InputDataConfig != nil {
		if input.InputDataConfig.S3Uri == "" {
			return nil, fmt.Errorf("%w: InputDataConfig.S3Uri is required", ErrValidation)
		}

		if input.InputDataConfig.DataAccessRoleArn == "" {
			return nil, fmt.Errorf("%w: InputDataConfig.DataAccessRoleArn is required", ErrValidation)
		}
	}

	b.mu.Lock("CreateLanguageModel")
	defer b.mu.Unlock()

	if b.languageModels.Has(input.ModelName) {
		return nil, fmt.Errorf("%w: language model %s already exists", ErrAlreadyExists, input.ModelName)
	}

	now := time.Now()
	m := *input
	m.ModelStatus = modelStatusCompleted
	m.CreateTime = now
	m.LastModifiedTime = now
	b.languageModels.Put(&m)

	cp := m

	return &cp, nil
}

// DeleteLanguageModel removes a custom language model by name.
func (b *InMemoryBackend) DeleteLanguageModel(modelName string) error {
	if modelName == "" {
		return fmt.Errorf("%w: ModelName is required", ErrValidation)
	}

	b.mu.Lock("DeleteLanguageModel")
	defer b.mu.Unlock()

	if !b.languageModels.Delete(modelName) {
		return fmt.Errorf("%w: language model %s not found", ErrNotFound, modelName)
	}

	return nil
}

// CreateMedicalVocabulary creates a new medical custom vocabulary.
func (b *InMemoryBackend) CreateMedicalVocabulary(
	vocabularyName, languageCode, vocabularyFileURI string,
) (*MedicalVocabulary, error) {
	if vocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if languageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	if vocabularyFileURI == "" {
		return nil, fmt.Errorf("%w: VocabularyFileURI is required", ErrValidation)
	}

	b.mu.Lock("CreateMedicalVocabulary")
	defer b.mu.Unlock()

	if b.medicalVocabularies.Has(vocabularyName) {
		return nil, fmt.Errorf(
			"%w: medical vocabulary %s already exists",
			ErrAlreadyExists,
			vocabularyName,
		)
	}

	now := time.Now()
	v := &MedicalVocabulary{
		VocabularyName:    vocabularyName,
		LanguageCode:      languageCode,
		VocabularyState:   vocabStateReady,
		VocabularyFileURI: vocabularyFileURI,
		LastModifiedTime:  now,
	}
	b.medicalVocabularies.Put(v)

	cp := *v

	return &cp, nil
}

// CreateVocabulary creates a new custom vocabulary.
func (b *InMemoryBackend) CreateVocabulary(input *Vocabulary) (*Vocabulary, error) {
	if input.VocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if input.LanguageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	// Exactly one of Phrases or VocabularyFileURI must be set.
	if len(input.Phrases) > 0 && input.VocabularyFileURI != "" {
		return nil, fmt.Errorf("%w: provide either Phrases or VocabularyFileURI, not both", ErrValidation)
	}

	if err := validateVocabularyPhrases(input.Phrases); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVocabulary")
	defer b.mu.Unlock()

	if b.vocabularies.Has(input.VocabularyName) {
		return nil, fmt.Errorf("%w: vocabulary %s already exists", ErrAlreadyExists, input.VocabularyName)
	}

	now := time.Now()
	v := *input
	v.VocabularyState = vocabStateReady
	v.LastModifiedTime = now
	b.vocabularies.Put(&v)

	cp := v

	return &cp, nil
}

// CreateVocabularyFilter creates a new custom vocabulary filter.
func (b *InMemoryBackend) CreateVocabularyFilter(input *VocabularyFilter) (*VocabularyFilter, error) {
	if input.VocabularyFilterName == "" {
		return nil, fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if input.LanguageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	// Exactly one of Words or VocabularyFilterFileURI must be set.
	if len(input.Words) > 0 && input.VocabularyFilterFileURI != "" {
		return nil, fmt.Errorf("%w: provide either Words or VocabularyFilterFileURI, not both", ErrValidation)
	}

	if len(input.Words) == 0 && input.VocabularyFilterFileURI == "" {
		return nil, fmt.Errorf("%w: one of Words or VocabularyFilterFileURI is required", ErrValidation)
	}

	if err := validateVocabularyFilterWords(input.Words); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVocabularyFilter")
	defer b.mu.Unlock()

	if b.vocabularyFilters.Has(input.VocabularyFilterName) {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s already exists",
			ErrAlreadyExists,
			input.VocabularyFilterName,
		)
	}

	now := time.Now()
	f := *input
	f.LastModifiedTime = now
	b.vocabularyFilters.Put(&f)

	cp := f

	return &cp, nil
}

// DeleteCallAnalyticsJob removes a Call Analytics job by name.
func (b *InMemoryBackend) DeleteCallAnalyticsJob(jobName string) error {
	if jobName == "" {
		return fmt.Errorf("%w: CallAnalyticsJobName is required", ErrValidation)
	}

	b.mu.Lock("DeleteCallAnalyticsJob")
	defer b.mu.Unlock()

	if !b.callAnalyticsJobs.Delete(jobName) {
		return fmt.Errorf("%w: call analytics job %s not found", ErrNotFound, jobName)
	}

	return nil
}

// DeleteMedicalScribeJob removes a Medical Scribe job by name.
func (b *InMemoryBackend) DeleteMedicalScribeJob(jobName string) error {
	if jobName == "" {
		return fmt.Errorf("%w: MedicalScribeJobName is required", ErrValidation)
	}

	b.mu.Lock("DeleteMedicalScribeJob")
	defer b.mu.Unlock()

	if !b.medicalScribeJobs.Delete(jobName) {
		return fmt.Errorf("%w: medical scribe job %s not found", ErrNotFound, jobName)
	}

	return nil
}

// DeleteMedicalTranscriptionJob removes a medical transcription job by name.
func (b *InMemoryBackend) DeleteMedicalTranscriptionJob(jobName string) error {
	if jobName == "" {
		return fmt.Errorf("%w: MedicalTranscriptionJobName is required", ErrValidation)
	}

	b.mu.Lock("DeleteMedicalTranscriptionJob")
	defer b.mu.Unlock()

	if !b.medicalTranscriptionJobs.Delete(jobName) {
		return fmt.Errorf("%w: medical transcription job %s not found", ErrNotFound, jobName)
	}

	return nil
}

// AddCallAnalyticsJobInternal seeds a Call Analytics job directly (test helper).
func (b *InMemoryBackend) AddCallAnalyticsJobInternal(job *CallAnalyticsJob) {
	b.mu.Lock("AddCallAnalyticsJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.callAnalyticsJobs.Put(&cp)
}

// AddMedicalScribeJobInternal seeds a Medical Scribe job directly (test helper).
func (b *InMemoryBackend) AddMedicalScribeJobInternal(job *MedicalScribeJob) {
	b.mu.Lock("AddMedicalScribeJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.medicalScribeJobs.Put(&cp)
}

// AddMedicalTranscriptionJobInternal seeds a Medical Transcription job directly (test helper).
func (b *InMemoryBackend) AddMedicalTranscriptionJobInternal(job *MedicalTranscriptionJob) {
	b.mu.Lock("AddMedicalTranscriptionJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.medicalTranscriptionJobs.Put(&cp)
}

// AddCallAnalyticsCategoryInternal seeds a Call Analytics category directly (test helper).
func (b *InMemoryBackend) AddCallAnalyticsCategoryInternal(cat *CallAnalyticsCategory) {
	b.mu.Lock("AddCallAnalyticsCategoryInternal")
	defer b.mu.Unlock()

	cp := *cat
	b.callAnalyticsCategories.Put(&cp)
}

// AddLanguageModelInternal seeds a language model directly (test helper).
func (b *InMemoryBackend) AddLanguageModelInternal(m *LanguageModel) {
	b.mu.Lock("AddLanguageModelInternal")
	defer b.mu.Unlock()

	cp := *m
	b.languageModels.Put(&cp)
}

// AddMedicalVocabularyInternal seeds a medical vocabulary directly (test helper).
func (b *InMemoryBackend) AddMedicalVocabularyInternal(v *MedicalVocabulary) {
	b.mu.Lock("AddMedicalVocabularyInternal")
	defer b.mu.Unlock()

	cp := *v
	b.medicalVocabularies.Put(&cp)
}

// AddVocabularyInternal seeds a vocabulary directly (test helper).
func (b *InMemoryBackend) AddVocabularyInternal(v *Vocabulary) {
	b.mu.Lock("AddVocabularyInternal")
	defer b.mu.Unlock()

	cp := *v
	b.vocabularies.Put(&cp)
}

// AddVocabularyFilterInternal seeds a vocabulary filter directly (test helper).
func (b *InMemoryBackend) AddVocabularyFilterInternal(f *VocabularyFilter) {
	b.mu.Lock("AddVocabularyFilterInternal")
	defer b.mu.Unlock()

	cp := *f
	b.vocabularyFilters.Put(&cp)
}

// --- Call Analytics jobs ---

// StartCallAnalyticsJob creates a new Call Analytics job.
func (b *InMemoryBackend) StartCallAnalyticsJob(input *CallAnalyticsJob) (*CallAnalyticsJob, error) {
	if err := validateJobName(input.CallAnalyticsJobName); err != nil {
		return nil, fmt.Errorf("%w: CallAnalyticsJobName is required", ErrValidation)
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if err := validateChannelDefinitions(input.ChannelDefinitions); err != nil {
		return nil, err
	}

	b.mu.Lock("StartCallAnalyticsJob")
	defer b.mu.Unlock()

	if b.callAnalyticsJobs.Has(input.CallAnalyticsJobName) {
		return nil, fmt.Errorf(
			"%w: call analytics job %s already exists",
			ErrAlreadyExists,
			input.CallAnalyticsJobName,
		)
	}

	now := time.Now()
	job := *input
	job.CallAnalyticsJobStatus = jobStatusCompleted
	job.CreationTime = now
	job.StartTime = now
	job.CompletionTime = now
	b.callAnalyticsJobs.Put(&job)

	cp := job

	return &cp, nil
}

// GetCallAnalyticsJob returns a Call Analytics job by name.
func (b *InMemoryBackend) GetCallAnalyticsJob(jobName string) (*CallAnalyticsJob, error) {
	b.mu.RLock("GetCallAnalyticsJob")
	defer b.mu.RUnlock()

	job, ok := b.callAnalyticsJobs.Get(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: call analytics job %s not found", ErrNotFound, jobName)
	}

	cp := *job

	return &cp, nil
}

// ListCallAnalyticsJobs returns Call Analytics jobs with optional status filter and pagination.
func (b *InMemoryBackend) ListCallAnalyticsJobs(
	statusFilter, nextToken string,
) ([]CallAnalyticsJob, string) {
	b.mu.RLock("ListCallAnalyticsJobs")
	defer b.mu.RUnlock()

	all := make([]CallAnalyticsJob, 0, b.callAnalyticsJobs.Len())
	for _, j := range b.callAnalyticsJobs.All() {
		if statusFilter == "" || j.CallAnalyticsJobStatus == statusFilter {
			all = append(all, *j)
		}
	}

	sort.Slice(
		all,
		func(i, j int) bool { return all[i].CallAnalyticsJobName < all[j].CallAnalyticsJobName },
	)

	return paginateList(all, nextToken)
}

// --- Call Analytics categories (Get/Update/List) ---

// GetCallAnalyticsCategory returns a Call Analytics category by name.
func (b *InMemoryBackend) GetCallAnalyticsCategory(
	categoryName string,
) (*CallAnalyticsCategory, error) {
	b.mu.RLock("GetCallAnalyticsCategory")
	defer b.mu.RUnlock()

	cat, ok := b.callAnalyticsCategories.Get(categoryName)
	if !ok {
		return nil, fmt.Errorf("%w: category %s not found", ErrNotFound, categoryName)
	}

	cp := *cat

	return &cp, nil
}

// UpdateCallAnalyticsCategory updates an existing Call Analytics category.
func (b *InMemoryBackend) UpdateCallAnalyticsCategory(input *CallAnalyticsCategory) (*CallAnalyticsCategory, error) {
	if input.CategoryName == "" {
		return nil, fmt.Errorf("%w: CategoryName is required", ErrValidation)
	}

	if err := validateCallAnalyticsInputType(input.InputType); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateCallAnalyticsCategory")
	defer b.mu.Unlock()

	cat, ok := b.callAnalyticsCategories.Get(input.CategoryName)
	if !ok {
		return nil, fmt.Errorf("%w: category %s not found", ErrNotFound, input.CategoryName)
	}

	cat.InputType = input.InputType
	cat.Rules = input.Rules
	cat.LastUpdateTime = time.Now()

	cp := *cat

	return &cp, nil
}

// ListCallAnalyticsCategories returns all Call Analytics categories with pagination.
func (b *InMemoryBackend) ListCallAnalyticsCategories(
	nextToken string,
) ([]CallAnalyticsCategory, string) {
	b.mu.RLock("ListCallAnalyticsCategories")
	defer b.mu.RUnlock()

	all := make([]CallAnalyticsCategory, 0, b.callAnalyticsCategories.Len())
	for _, c := range b.callAnalyticsCategories.All() {
		all = append(all, *c)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CategoryName < all[j].CategoryName })

	return paginateList(all, nextToken)
}

// --- Vocabulary CRUD ---

// GetVocabulary returns a custom vocabulary by name.
func (b *InMemoryBackend) GetVocabulary(vocabularyName string) (*Vocabulary, error) {
	b.mu.RLock("GetVocabulary")
	defer b.mu.RUnlock()

	v, ok := b.vocabularies.Get(vocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: vocabulary %s not found", ErrVocabularyNotFound, vocabularyName)
	}

	cp := *v

	return &cp, nil
}

// UpdateVocabulary updates an existing custom vocabulary.
//
//nolint:dupl // mirrors UpdateVocabularyFilter but operates on Vocabulary not VocabularyFilter
func (b *InMemoryBackend) UpdateVocabulary(
	input *Vocabulary,
) (*Vocabulary, error) {
	if input.VocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if err := validateVocabularyPhrases(input.Phrases); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateVocabulary")
	defer b.mu.Unlock()

	v, ok := b.vocabularies.Get(input.VocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: vocabulary %s not found", ErrNotFound, input.VocabularyName)
	}

	if input.LanguageCode != "" {
		v.LanguageCode = input.LanguageCode
	}

	if len(input.Phrases) > 0 {
		v.Phrases = input.Phrases
		v.VocabularyFileURI = ""
	} else if input.VocabularyFileURI != "" {
		v.VocabularyFileURI = input.VocabularyFileURI
		v.Phrases = nil
	}

	v.LastModifiedTime = time.Now()

	cp := *v

	return &cp, nil
}

// DeleteVocabulary removes a custom vocabulary by name.
func (b *InMemoryBackend) DeleteVocabulary(vocabularyName string) error {
	if vocabularyName == "" {
		return fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	b.mu.Lock("DeleteVocabulary")
	defer b.mu.Unlock()

	if !b.vocabularies.Delete(vocabularyName) {
		return fmt.Errorf("%w: vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	return nil
}

// ListVocabularies returns custom vocabularies with optional state filter and pagination.
func (b *InMemoryBackend) ListVocabularies(stateFilter, nextToken string) ([]Vocabulary, string) {
	b.mu.RLock("ListVocabularies")
	defer b.mu.RUnlock()

	all := make([]Vocabulary, 0, b.vocabularies.Len())
	for _, v := range b.vocabularies.All() {
		if stateFilter == "" || v.VocabularyState == stateFilter {
			all = append(all, *v)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].VocabularyName < all[j].VocabularyName })

	return paginateList(all, nextToken)
}

// --- Vocabulary filter CRUD ---

// GetVocabularyFilter returns a custom vocabulary filter by name.
func (b *InMemoryBackend) GetVocabularyFilter(
	vocabularyFilterName string,
) (*VocabularyFilter, error) {
	b.mu.RLock("GetVocabularyFilter")
	defer b.mu.RUnlock()

	f, ok := b.vocabularyFilters.Get(vocabularyFilterName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s not found",
			ErrNotFound,
			vocabularyFilterName,
		)
	}

	cp := *f

	return &cp, nil
}

// UpdateVocabularyFilter updates an existing vocabulary filter.
//
//nolint:dupl // mirrors UpdateVocabulary but operates on VocabularyFilter not Vocabulary
func (b *InMemoryBackend) UpdateVocabularyFilter(
	input *VocabularyFilter,
) (*VocabularyFilter, error) {
	if input.VocabularyFilterName == "" {
		return nil, fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	if err := validateVocabularyFilterWords(input.Words); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateVocabularyFilter")
	defer b.mu.Unlock()

	f, ok := b.vocabularyFilters.Get(input.VocabularyFilterName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s not found",
			ErrNotFound,
			input.VocabularyFilterName,
		)
	}

	if input.LanguageCode != "" {
		f.LanguageCode = input.LanguageCode
	}

	if len(input.Words) > 0 {
		f.Words = input.Words
		f.VocabularyFilterFileURI = ""
	} else if input.VocabularyFilterFileURI != "" {
		f.VocabularyFilterFileURI = input.VocabularyFilterFileURI
		f.Words = nil
	}

	f.LastModifiedTime = time.Now()

	cp := *f

	return &cp, nil
}

// DeleteVocabularyFilter removes a vocabulary filter by name.
func (b *InMemoryBackend) DeleteVocabularyFilter(vocabularyFilterName string) error {
	if vocabularyFilterName == "" {
		return fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	b.mu.Lock("DeleteVocabularyFilter")
	defer b.mu.Unlock()

	if !b.vocabularyFilters.Delete(vocabularyFilterName) {
		return fmt.Errorf("%w: vocabulary filter %s not found", ErrNotFound, vocabularyFilterName)
	}

	return nil
}

// ListVocabularyFilters returns all vocabulary filters with pagination.
func (b *InMemoryBackend) ListVocabularyFilters(nextToken string) ([]VocabularyFilter, string) {
	b.mu.RLock("ListVocabularyFilters")
	defer b.mu.RUnlock()

	all := make([]VocabularyFilter, 0, b.vocabularyFilters.Len())
	for _, f := range b.vocabularyFilters.All() {
		all = append(all, *f)
	}

	sort.Slice(
		all,
		func(i, j int) bool { return all[i].VocabularyFilterName < all[j].VocabularyFilterName },
	)

	return paginateList(all, nextToken)
}

// --- Medical vocabulary CRUD ---

// GetMedicalVocabulary returns a medical vocabulary by name.
func (b *InMemoryBackend) GetMedicalVocabulary(vocabularyName string) (*MedicalVocabulary, error) {
	b.mu.RLock("GetMedicalVocabulary")
	defer b.mu.RUnlock()

	v, ok := b.medicalVocabularies.Get(vocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: medical vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	cp := *v

	return &cp, nil
}

// UpdateMedicalVocabulary updates an existing medical vocabulary.
func (b *InMemoryBackend) UpdateMedicalVocabulary(
	vocabularyName, languageCode, vocabularyFileURI string,
) (*MedicalVocabulary, error) {
	if vocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	b.mu.Lock("UpdateMedicalVocabulary")
	defer b.mu.Unlock()

	v, ok := b.medicalVocabularies.Get(vocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: medical vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	if languageCode != "" {
		v.LanguageCode = languageCode
	}

	if vocabularyFileURI != "" {
		v.VocabularyFileURI = vocabularyFileURI
	}

	v.LastModifiedTime = time.Now()

	cp := *v

	return &cp, nil
}

// DeleteMedicalVocabulary removes a medical vocabulary by name.
func (b *InMemoryBackend) DeleteMedicalVocabulary(vocabularyName string) error {
	if vocabularyName == "" {
		return fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	b.mu.Lock("DeleteMedicalVocabulary")
	defer b.mu.Unlock()

	if !b.medicalVocabularies.Delete(vocabularyName) {
		return fmt.Errorf("%w: medical vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	return nil
}

// ListMedicalVocabularies returns medical vocabularies with optional state filter and pagination.
func (b *InMemoryBackend) ListMedicalVocabularies(
	stateFilter, nextToken string,
) ([]MedicalVocabulary, string) {
	b.mu.RLock("ListMedicalVocabularies")
	defer b.mu.RUnlock()

	all := make([]MedicalVocabulary, 0, b.medicalVocabularies.Len())
	for _, v := range b.medicalVocabularies.All() {
		if stateFilter == "" || v.VocabularyState == stateFilter {
			all = append(all, *v)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].VocabularyName < all[j].VocabularyName })

	return paginateList(all, nextToken)
}

// --- Medical Scribe jobs ---

// StartMedicalScribeJob creates a new Medical Scribe job.
func (b *InMemoryBackend) StartMedicalScribeJob(input *MedicalScribeJob) (*MedicalScribeJob, error) {
	if err := validateJobName(input.MedicalScribeJobName); err != nil {
		return nil, fmt.Errorf("%w: MedicalScribeJobName is required", ErrValidation)
	}

	if input.DataAccessRoleArn == "" {
		return nil, fmt.Errorf("%w: DataAccessRoleArn is required for MedicalScribeJob", ErrValidation)
	}

	if input.OutputBucketName == "" {
		return nil, fmt.Errorf("%w: OutputBucketName is required for MedicalScribeJob", ErrValidation)
	}

	b.mu.Lock("StartMedicalScribeJob")
	defer b.mu.Unlock()

	if b.medicalScribeJobs.Has(input.MedicalScribeJobName) {
		return nil, fmt.Errorf(
			"%w: medical scribe job %s already exists",
			ErrAlreadyExists,
			input.MedicalScribeJobName,
		)
	}

	now := time.Now()
	job := *input
	job.MedicalScribeJobStatus = jobStatusCompleted
	job.CreationTime = now
	job.StartTime = now
	job.CompletionTime = now
	b.medicalScribeJobs.Put(&job)

	cp := job

	return &cp, nil
}

// GetMedicalScribeJob returns a Medical Scribe job by name.
func (b *InMemoryBackend) GetMedicalScribeJob(jobName string) (*MedicalScribeJob, error) {
	b.mu.RLock("GetMedicalScribeJob")
	defer b.mu.RUnlock()

	job, ok := b.medicalScribeJobs.Get(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: medical scribe job %s not found", ErrNotFound, jobName)
	}

	cp := *job

	return &cp, nil
}

// ListMedicalScribeJobs returns Medical Scribe jobs with optional status filter and pagination.
func (b *InMemoryBackend) ListMedicalScribeJobs(
	statusFilter, nextToken string,
) ([]MedicalScribeJob, string) {
	b.mu.RLock("ListMedicalScribeJobs")
	defer b.mu.RUnlock()

	all := make([]MedicalScribeJob, 0, b.medicalScribeJobs.Len())
	for _, j := range b.medicalScribeJobs.All() {
		if statusFilter == "" || j.MedicalScribeJobStatus == statusFilter {
			all = append(all, *j)
		}
	}

	sort.Slice(
		all,
		func(i, j int) bool { return all[i].MedicalScribeJobName < all[j].MedicalScribeJobName },
	)

	return paginateList(all, nextToken)
}

// --- Medical Transcription jobs ---

// StartMedicalTranscriptionJob creates a new Medical Transcription job.
func (b *InMemoryBackend) StartMedicalTranscriptionJob(
	input *MedicalTranscriptionJob,
) (*MedicalTranscriptionJob, error) {
	if err := validateJobName(input.MedicalTranscriptionJobName); err != nil {
		return nil, fmt.Errorf("%w: MedicalTranscriptionJobName is required", ErrValidation)
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if input.LanguageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	if err := validateMedicalSpecialty(input.Specialty); err != nil {
		return nil, err
	}

	if err := validateMedicalType(input.Type); err != nil {
		return nil, err
	}

	if err := validateMediaFormat(input.MediaFormat); err != nil {
		return nil, err
	}

	if err := validateMediaSampleRateHertz(input.MediaSampleRateHertz); err != nil {
		return nil, err
	}

	if input.MedicalContentIdentificationType != "" {
		if !slices.Contains(supportedMedicalContentIDTypes(), input.MedicalContentIdentificationType) {
			return nil, fmt.Errorf("%w: MedicalContentIdentificationType %q must be PHI",
				ErrValidation, input.MedicalContentIdentificationType)
		}
	}

	b.mu.Lock("StartMedicalTranscriptionJob")
	defer b.mu.Unlock()

	if b.medicalTranscriptionJobs.Has(input.MedicalTranscriptionJobName) {
		return nil, fmt.Errorf(
			"%w: medical transcription job %s already exists",
			ErrAlreadyExists,
			input.MedicalTranscriptionJobName,
		)
	}

	now := time.Now()
	job := *input
	job.TranscriptionJobStatus = jobStatusCompleted
	job.CreationTime = now
	job.StartTime = now
	job.CompletionTime = now
	job.TranscriptJSON = synthesizeTranscriptJSON(input.MedicalTranscriptionJobName,
		"Medical transcription result for "+input.MedicalTranscriptionJobName+".")
	b.medicalTranscriptionJobs.Put(&job)

	cp := job

	return &cp, nil
}

// GetMedicalTranscriptionJob returns a Medical Transcription job by name.
func (b *InMemoryBackend) GetMedicalTranscriptionJob(
	jobName string,
) (*MedicalTranscriptionJob, error) {
	b.mu.RLock("GetMedicalTranscriptionJob")
	defer b.mu.RUnlock()

	job, ok := b.medicalTranscriptionJobs.Get(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: medical transcription job %s not found", ErrNotFound, jobName)
	}

	cp := *job

	return &cp, nil
}

// ListMedicalTranscriptionJobs returns Medical Transcription jobs with optional status filter and pagination.
func (b *InMemoryBackend) ListMedicalTranscriptionJobs(
	statusFilter, nextToken string,
) ([]MedicalTranscriptionJob, string) {
	b.mu.RLock("ListMedicalTranscriptionJobs")
	defer b.mu.RUnlock()

	all := make([]MedicalTranscriptionJob, 0, b.medicalTranscriptionJobs.Len())
	for _, j := range b.medicalTranscriptionJobs.All() {
		if statusFilter == "" || j.TranscriptionJobStatus == statusFilter {
			all = append(all, *j)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].MedicalTranscriptionJobName < all[j].MedicalTranscriptionJobName
	})

	return paginateList(all, nextToken)
}

// --- Language model (Describe/List) ---

// DescribeLanguageModel returns a language model by name.
func (b *InMemoryBackend) DescribeLanguageModel(modelName string) (*LanguageModel, error) {
	b.mu.RLock("DescribeLanguageModel")
	defer b.mu.RUnlock()

	m, ok := b.languageModels.Get(modelName)
	if !ok {
		return nil, fmt.Errorf("%w: language model %s not found", ErrNotFound, modelName)
	}

	cp := *m

	return &cp, nil
}

// ListLanguageModels returns language models with optional status filter and pagination.
func (b *InMemoryBackend) ListLanguageModels(
	statusFilter, nextToken string,
) ([]LanguageModel, string) {
	b.mu.RLock("ListLanguageModels")
	defer b.mu.RUnlock()

	all := make([]LanguageModel, 0, b.languageModels.Len())
	for _, m := range b.languageModels.All() {
		if statusFilter == "" || m.ModelStatus == statusFilter {
			all = append(all, *m)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ModelName < all[j].ModelName })

	return paginateList(all, nextToken)
}

// paginateList applies pagination to a pre-sorted slice using a token-based offset.
// It returns the page slice and the next-page token (empty string if last page).
func paginateList[T any](all []T, nextToken string) ([]T, string) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []T{}, ""
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

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[resourceArn]; !ok {
		b.resourceTags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[resourceArn], tags)

	return nil
}

// UntagResource removes specific tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.resourceTags[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.resourceTags[resourceArn]
	if !ok {
		return map[string]string{}, nil
	}

	// Return a copy so callers can't mutate the stored map.
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
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
