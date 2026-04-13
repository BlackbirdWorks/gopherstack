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

// CallAnalyticsCategory represents an Amazon Transcribe Call Analytics category.
type CallAnalyticsCategory struct {
	CreateTime     time.Time `json:"createTime"`
	LastUpdateTime time.Time `json:"lastUpdateTime"`
	CategoryName   string    `json:"categoryName"`
	InputType      string    `json:"inputType"`
}

// LanguageModel represents a custom Amazon Transcribe language model.
type LanguageModel struct {
	CreateTime       time.Time `json:"createTime"`
	LastModifiedTime time.Time `json:"lastModifiedTime"`
	ModelName        string    `json:"modelName"`
	BaseModelName    string    `json:"baseModelName"`
	LanguageCode     string    `json:"languageCode"`
	ModelStatus      string    `json:"modelStatus"`
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
	LastModifiedTime time.Time `json:"lastModifiedTime"`
	VocabularyName   string    `json:"vocabularyName"`
	LanguageCode     string    `json:"languageCode"`
	VocabularyState  string    `json:"vocabularyState"`
}

// VocabularyFilter represents an Amazon Transcribe custom vocabulary filter.
type VocabularyFilter struct {
	LastModifiedTime     time.Time `json:"lastModifiedTime"`
	VocabularyFilterName string    `json:"vocabularyFilterName"`
	LanguageCode         string    `json:"languageCode"`
}

// CallAnalyticsJob represents an Amazon Transcribe Call Analytics job.
type CallAnalyticsJob struct {
	CreationTime           time.Time `json:"creationTime"`
	CompletionTime         time.Time `json:"completionTime"`
	CallAnalyticsJobName   string    `json:"callAnalyticsJobName"`
	CallAnalyticsJobStatus string    `json:"callAnalyticsJobStatus"`
	LanguageCode           string    `json:"languageCode"`
}

// MedicalScribeJob represents an Amazon Transcribe Medical Scribe job.
type MedicalScribeJob struct {
	CreationTime           time.Time `json:"creationTime"`
	CompletionTime         time.Time `json:"completionTime"`
	MedicalScribeJobName   string    `json:"medicalScribeJobName"`
	MedicalScribeJobStatus string    `json:"medicalScribeJobStatus"`
}

// MedicalTranscriptionJob represents an Amazon Transcribe Medical transcription job.
type MedicalTranscriptionJob struct {
	CreationTime                time.Time `json:"creationTime"`
	CompletionTime              time.Time `json:"completionTime"`
	MedicalTranscriptionJobName string    `json:"medicalTranscriptionJobName"`
	TranscriptionJobStatus      string    `json:"transcriptionJobStatus"`
	LanguageCode                string    `json:"languageCode"`
}

// InMemoryBackend is the in-memory store for Transcribe jobs.
type InMemoryBackend struct {
	jobs                     map[string]*TranscriptionJob
	callAnalyticsCategories  map[string]*CallAnalyticsCategory
	languageModels           map[string]*LanguageModel
	medicalVocabularies      map[string]*MedicalVocabulary
	vocabularies             map[string]*Vocabulary
	vocabularyFilters        map[string]*VocabularyFilter
	callAnalyticsJobs        map[string]*CallAnalyticsJob
	medicalScribeJobs        map[string]*MedicalScribeJob
	medicalTranscriptionJobs map[string]*MedicalTranscriptionJob
	mu                       *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		jobs:                     make(map[string]*TranscriptionJob),
		callAnalyticsCategories:  make(map[string]*CallAnalyticsCategory),
		languageModels:           make(map[string]*LanguageModel),
		medicalVocabularies:      make(map[string]*MedicalVocabulary),
		vocabularies:             make(map[string]*Vocabulary),
		vocabularyFilters:        make(map[string]*VocabularyFilter),
		callAnalyticsJobs:        make(map[string]*CallAnalyticsJob),
		medicalScribeJobs:        make(map[string]*MedicalScribeJob),
		medicalTranscriptionJobs: make(map[string]*MedicalTranscriptionJob),
		mu:                       lockmetrics.New("transcribe"),
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

// CreateCallAnalyticsCategory creates a new Call Analytics category.
func (b *InMemoryBackend) CreateCallAnalyticsCategory(categoryName, inputType string) (*CallAnalyticsCategory, error) {
	b.mu.Lock("CreateCallAnalyticsCategory")
	defer b.mu.Unlock()

	if _, ok := b.callAnalyticsCategories[categoryName]; ok {
		return nil, fmt.Errorf("%w: category %s already exists", ErrAlreadyExists, categoryName)
	}

	now := time.Now()
	cat := &CallAnalyticsCategory{
		CategoryName:   categoryName,
		InputType:      inputType,
		CreateTime:     now,
		LastUpdateTime: now,
	}
	b.callAnalyticsCategories[categoryName] = cat

	cp := *cat

	return &cp, nil
}

// DeleteCallAnalyticsCategory removes a Call Analytics category by name.
func (b *InMemoryBackend) DeleteCallAnalyticsCategory(categoryName string) error {
	b.mu.Lock("DeleteCallAnalyticsCategory")
	defer b.mu.Unlock()

	if _, ok := b.callAnalyticsCategories[categoryName]; !ok {
		return fmt.Errorf("%w: category %s not found", ErrNotFound, categoryName)
	}

	delete(b.callAnalyticsCategories, categoryName)

	return nil
}

// CreateLanguageModel creates a new custom language model.
func (b *InMemoryBackend) CreateLanguageModel(modelName, baseModelName, languageCode string) (*LanguageModel, error) {
	b.mu.Lock("CreateLanguageModel")
	defer b.mu.Unlock()

	if _, ok := b.languageModels[modelName]; ok {
		return nil, fmt.Errorf("%w: language model %s already exists", ErrAlreadyExists, modelName)
	}

	now := time.Now()
	m := &LanguageModel{
		ModelName:        modelName,
		BaseModelName:    baseModelName,
		LanguageCode:     languageCode,
		ModelStatus:      "COMPLETED",
		CreateTime:       now,
		LastModifiedTime: now,
	}
	b.languageModels[modelName] = m

	cp := *m

	return &cp, nil
}

// DeleteLanguageModel removes a custom language model by name.
func (b *InMemoryBackend) DeleteLanguageModel(modelName string) error {
	b.mu.Lock("DeleteLanguageModel")
	defer b.mu.Unlock()

	if _, ok := b.languageModels[modelName]; !ok {
		return fmt.Errorf("%w: language model %s not found", ErrNotFound, modelName)
	}

	delete(b.languageModels, modelName)

	return nil
}

// CreateMedicalVocabulary creates a new medical custom vocabulary.
func (b *InMemoryBackend) CreateMedicalVocabulary(
	vocabularyName, languageCode, vocabularyFileURI string,
) (*MedicalVocabulary, error) {
	b.mu.Lock("CreateMedicalVocabulary")
	defer b.mu.Unlock()

	if _, ok := b.medicalVocabularies[vocabularyName]; ok {
		return nil, fmt.Errorf("%w: medical vocabulary %s already exists", ErrAlreadyExists, vocabularyName)
	}

	now := time.Now()
	v := &MedicalVocabulary{
		VocabularyName:    vocabularyName,
		LanguageCode:      languageCode,
		VocabularyState:   "READY",
		VocabularyFileURI: vocabularyFileURI,
		LastModifiedTime:  now,
	}
	b.medicalVocabularies[vocabularyName] = v

	cp := *v

	return &cp, nil
}

// CreateVocabulary creates a new custom vocabulary.
func (b *InMemoryBackend) CreateVocabulary(vocabularyName, languageCode string) (*Vocabulary, error) {
	b.mu.Lock("CreateVocabulary")
	defer b.mu.Unlock()

	if _, ok := b.vocabularies[vocabularyName]; ok {
		return nil, fmt.Errorf("%w: vocabulary %s already exists", ErrAlreadyExists, vocabularyName)
	}

	now := time.Now()
	v := &Vocabulary{
		VocabularyName:   vocabularyName,
		LanguageCode:     languageCode,
		VocabularyState:  "READY",
		LastModifiedTime: now,
	}
	b.vocabularies[vocabularyName] = v

	cp := *v

	return &cp, nil
}

// CreateVocabularyFilter creates a new custom vocabulary filter.
func (b *InMemoryBackend) CreateVocabularyFilter(vocabularyFilterName, languageCode string) (*VocabularyFilter, error) {
	b.mu.Lock("CreateVocabularyFilter")
	defer b.mu.Unlock()

	if _, ok := b.vocabularyFilters[vocabularyFilterName]; ok {
		return nil, fmt.Errorf("%w: vocabulary filter %s already exists", ErrAlreadyExists, vocabularyFilterName)
	}

	now := time.Now()
	f := &VocabularyFilter{
		VocabularyFilterName: vocabularyFilterName,
		LanguageCode:         languageCode,
		LastModifiedTime:     now,
	}
	b.vocabularyFilters[vocabularyFilterName] = f

	cp := *f

	return &cp, nil
}

// DeleteCallAnalyticsJob removes a Call Analytics job by name.
func (b *InMemoryBackend) DeleteCallAnalyticsJob(jobName string) error {
	b.mu.Lock("DeleteCallAnalyticsJob")
	defer b.mu.Unlock()

	if _, ok := b.callAnalyticsJobs[jobName]; !ok {
		return fmt.Errorf("%w: call analytics job %s not found", ErrNotFound, jobName)
	}

	delete(b.callAnalyticsJobs, jobName)

	return nil
}

// DeleteMedicalScribeJob removes a Medical Scribe job by name.
func (b *InMemoryBackend) DeleteMedicalScribeJob(jobName string) error {
	b.mu.Lock("DeleteMedicalScribeJob")
	defer b.mu.Unlock()

	if _, ok := b.medicalScribeJobs[jobName]; !ok {
		return fmt.Errorf("%w: medical scribe job %s not found", ErrNotFound, jobName)
	}

	delete(b.medicalScribeJobs, jobName)

	return nil
}

// DeleteMedicalTranscriptionJob removes a medical transcription job by name.
func (b *InMemoryBackend) DeleteMedicalTranscriptionJob(jobName string) error {
	b.mu.Lock("DeleteMedicalTranscriptionJob")
	defer b.mu.Unlock()

	if _, ok := b.medicalTranscriptionJobs[jobName]; !ok {
		return fmt.Errorf("%w: medical transcription job %s not found", ErrNotFound, jobName)
	}

	delete(b.medicalTranscriptionJobs, jobName)

	return nil
}

// AddCallAnalyticsJobInternal seeds a Call Analytics job directly (test helper).
func (b *InMemoryBackend) AddCallAnalyticsJobInternal(job *CallAnalyticsJob) {
	b.mu.Lock("AddCallAnalyticsJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.callAnalyticsJobs[job.CallAnalyticsJobName] = &cp
}

// AddMedicalScribeJobInternal seeds a Medical Scribe job directly (test helper).
func (b *InMemoryBackend) AddMedicalScribeJobInternal(job *MedicalScribeJob) {
	b.mu.Lock("AddMedicalScribeJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.medicalScribeJobs[job.MedicalScribeJobName] = &cp
}

// AddMedicalTranscriptionJobInternal seeds a Medical Transcription job directly (test helper).
func (b *InMemoryBackend) AddMedicalTranscriptionJobInternal(job *MedicalTranscriptionJob) {
	b.mu.Lock("AddMedicalTranscriptionJobInternal")
	defer b.mu.Unlock()

	cp := *job
	b.medicalTranscriptionJobs[job.MedicalTranscriptionJobName] = &cp
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
