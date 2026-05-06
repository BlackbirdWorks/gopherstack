package transcribe

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a transcription job is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the given name already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid or missing input parameters.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

const (
	transcribeDefaultPageSize = 100

	// job/scribe status constants.
	jobStatusCompleted = "COMPLETED"

	// vocabulary state constants.
	vocabStateReady = "READY"

	// language model status constant – gopherstack immediately completes models.
	modelStatusCompleted = "COMPLETED"

	// defaultAccountID is the synthetic AWS account ID used by the in-memory backend.
	defaultAccountID = "123456789012"
)

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
	b := &InMemoryBackend{mu: lockmetrics.New("transcribe")}
	b.ensureNonNilMaps()

	return b
}

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.ensureNonNilMaps()
}

// AccountID returns the synthetic AWS account ID used by this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

// ensureNonNilMaps initialises all maps. Must be called with b.mu held (or
// before the backend is shared between goroutines, as in NewInMemoryBackend).
func (b *InMemoryBackend) ensureNonNilMaps() {
	b.jobs = make(map[string]*TranscriptionJob)
	b.callAnalyticsCategories = make(map[string]*CallAnalyticsCategory)
	b.languageModels = make(map[string]*LanguageModel)
	b.medicalVocabularies = make(map[string]*MedicalVocabulary)
	b.vocabularies = make(map[string]*Vocabulary)
	b.vocabularyFilters = make(map[string]*VocabularyFilter)
	b.callAnalyticsJobs = make(map[string]*CallAnalyticsJob)
	b.medicalScribeJobs = make(map[string]*MedicalScribeJob)
	b.medicalTranscriptionJobs = make(map[string]*MedicalTranscriptionJob)
}

// StartTranscriptionJob creates a new transcription job with synthetic results.
func (b *InMemoryBackend) StartTranscriptionJob(
	jobName, languageCode, mediaFileURI string,
) (*TranscriptionJob, error) {
	if jobName == "" {
		return nil, fmt.Errorf("%w: TranscriptionJobName is required", ErrValidation)
	}

	if languageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	b.mu.Lock("StartTranscriptionJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[jobName]; ok {
		return nil, fmt.Errorf("%w: job %s already exists", ErrAlreadyExists, jobName)
	}

	now := time.Now()
	job := &TranscriptionJob{
		JobName:        jobName,
		JobStatus:      jobStatusCompleted,
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
func (b *InMemoryBackend) ListTranscriptionJobs(
	statusFilter, nextToken string,
) ([]TranscriptionJob, string) {
	b.mu.RLock("ListTranscriptionJobs")
	defer b.mu.RUnlock()

	all := make([]TranscriptionJob, 0, len(b.jobs))
	for _, j := range b.jobs {
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

	if _, ok := b.jobs[jobName]; !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	delete(b.jobs, jobName)

	return nil
}

// CreateCallAnalyticsCategory creates a new Call Analytics category.
func (b *InMemoryBackend) CreateCallAnalyticsCategory(
	categoryName, inputType string,
) (*CallAnalyticsCategory, error) {
	if categoryName == "" {
		return nil, fmt.Errorf("%w: CategoryName is required", ErrValidation)
	}

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
	if categoryName == "" {
		return fmt.Errorf("%w: CategoryName is required", ErrValidation)
	}

	b.mu.Lock("DeleteCallAnalyticsCategory")
	defer b.mu.Unlock()

	if _, ok := b.callAnalyticsCategories[categoryName]; !ok {
		return fmt.Errorf("%w: category %s not found", ErrNotFound, categoryName)
	}

	delete(b.callAnalyticsCategories, categoryName)

	return nil
}

// CreateLanguageModel creates a new custom language model.
func (b *InMemoryBackend) CreateLanguageModel(
	modelName, baseModelName, languageCode string,
) (*LanguageModel, error) {
	if modelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", ErrValidation)
	}

	if baseModelName == "" {
		return nil, fmt.Errorf("%w: BaseModelName is required", ErrValidation)
	}

	if languageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

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
		ModelStatus:      modelStatusCompleted,
		CreateTime:       now,
		LastModifiedTime: now,
	}
	b.languageModels[modelName] = m

	cp := *m

	return &cp, nil
}

// DeleteLanguageModel removes a custom language model by name.
func (b *InMemoryBackend) DeleteLanguageModel(modelName string) error {
	if modelName == "" {
		return fmt.Errorf("%w: ModelName is required", ErrValidation)
	}

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
	if vocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if languageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	if vocabularyFileURI == "" {
		return nil, fmt.Errorf("%w: VocabularyFileUri is required", ErrValidation)
	}

	b.mu.Lock("CreateMedicalVocabulary")
	defer b.mu.Unlock()

	if _, ok := b.medicalVocabularies[vocabularyName]; ok {
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
	b.medicalVocabularies[vocabularyName] = v

	cp := *v

	return &cp, nil
}

// CreateVocabulary creates a new custom vocabulary.
func (b *InMemoryBackend) CreateVocabulary(
	vocabularyName, languageCode string,
) (*Vocabulary, error) {
	if vocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if languageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	b.mu.Lock("CreateVocabulary")
	defer b.mu.Unlock()

	if _, ok := b.vocabularies[vocabularyName]; ok {
		return nil, fmt.Errorf("%w: vocabulary %s already exists", ErrAlreadyExists, vocabularyName)
	}

	now := time.Now()
	v := &Vocabulary{
		VocabularyName:   vocabularyName,
		LanguageCode:     languageCode,
		VocabularyState:  vocabStateReady,
		LastModifiedTime: now,
	}
	b.vocabularies[vocabularyName] = v

	cp := *v

	return &cp, nil
}

// CreateVocabularyFilter creates a new custom vocabulary filter.
func (b *InMemoryBackend) CreateVocabularyFilter(
	vocabularyFilterName, languageCode string,
) (*VocabularyFilter, error) {
	if vocabularyFilterName == "" {
		return nil, fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	if languageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	b.mu.Lock("CreateVocabularyFilter")
	defer b.mu.Unlock()

	if _, ok := b.vocabularyFilters[vocabularyFilterName]; ok {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s already exists",
			ErrAlreadyExists,
			vocabularyFilterName,
		)
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
	if jobName == "" {
		return fmt.Errorf("%w: CallAnalyticsJobName is required", ErrValidation)
	}

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
	if jobName == "" {
		return fmt.Errorf("%w: MedicalScribeJobName is required", ErrValidation)
	}

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
	if jobName == "" {
		return fmt.Errorf("%w: MedicalTranscriptionJobName is required", ErrValidation)
	}

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

// AddCallAnalyticsCategoryInternal seeds a Call Analytics category directly (test helper).
func (b *InMemoryBackend) AddCallAnalyticsCategoryInternal(cat *CallAnalyticsCategory) {
	b.mu.Lock("AddCallAnalyticsCategoryInternal")
	defer b.mu.Unlock()

	cp := *cat
	b.callAnalyticsCategories[cat.CategoryName] = &cp
}

// AddLanguageModelInternal seeds a language model directly (test helper).
func (b *InMemoryBackend) AddLanguageModelInternal(m *LanguageModel) {
	b.mu.Lock("AddLanguageModelInternal")
	defer b.mu.Unlock()

	cp := *m
	b.languageModels[m.ModelName] = &cp
}

// AddMedicalVocabularyInternal seeds a medical vocabulary directly (test helper).
func (b *InMemoryBackend) AddMedicalVocabularyInternal(v *MedicalVocabulary) {
	b.mu.Lock("AddMedicalVocabularyInternal")
	defer b.mu.Unlock()

	cp := *v
	b.medicalVocabularies[v.VocabularyName] = &cp
}

// AddVocabularyInternal seeds a vocabulary directly (test helper).
func (b *InMemoryBackend) AddVocabularyInternal(v *Vocabulary) {
	b.mu.Lock("AddVocabularyInternal")
	defer b.mu.Unlock()

	cp := *v
	b.vocabularies[v.VocabularyName] = &cp
}

// AddVocabularyFilterInternal seeds a vocabulary filter directly (test helper).
func (b *InMemoryBackend) AddVocabularyFilterInternal(f *VocabularyFilter) {
	b.mu.Lock("AddVocabularyFilterInternal")
	defer b.mu.Unlock()

	cp := *f
	b.vocabularyFilters[f.VocabularyFilterName] = &cp
}

// --- Call Analytics jobs ---

// StartCallAnalyticsJob creates a new Call Analytics job.
func (b *InMemoryBackend) StartCallAnalyticsJob(
	jobName, languageCode, _ string,
) (*CallAnalyticsJob, error) {
	if jobName == "" {
		return nil, fmt.Errorf("%w: CallAnalyticsJobName is required", ErrValidation)
	}

	b.mu.Lock("StartCallAnalyticsJob")
	defer b.mu.Unlock()

	if _, ok := b.callAnalyticsJobs[jobName]; ok {
		return nil, fmt.Errorf(
			"%w: call analytics job %s already exists",
			ErrAlreadyExists,
			jobName,
		)
	}

	now := time.Now()
	job := &CallAnalyticsJob{
		CallAnalyticsJobName:   jobName,
		CallAnalyticsJobStatus: jobStatusCompleted,
		LanguageCode:           languageCode,
		CreationTime:           now,
		CompletionTime:         now,
	}
	b.callAnalyticsJobs[jobName] = job

	cp := *job

	return &cp, nil
}

// GetCallAnalyticsJob returns a Call Analytics job by name.
func (b *InMemoryBackend) GetCallAnalyticsJob(jobName string) (*CallAnalyticsJob, error) {
	b.mu.RLock("GetCallAnalyticsJob")
	defer b.mu.RUnlock()

	job, ok := b.callAnalyticsJobs[jobName]
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

	all := make([]CallAnalyticsJob, 0, len(b.callAnalyticsJobs))
	for _, j := range b.callAnalyticsJobs {
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

	cat, ok := b.callAnalyticsCategories[categoryName]
	if !ok {
		return nil, fmt.Errorf("%w: category %s not found", ErrNotFound, categoryName)
	}

	cp := *cat

	return &cp, nil
}

// UpdateCallAnalyticsCategory updates an existing Call Analytics category.
func (b *InMemoryBackend) UpdateCallAnalyticsCategory(
	categoryName, inputType string,
) (*CallAnalyticsCategory, error) {
	if categoryName == "" {
		return nil, fmt.Errorf("%w: CategoryName is required", ErrValidation)
	}

	b.mu.Lock("UpdateCallAnalyticsCategory")
	defer b.mu.Unlock()

	cat, ok := b.callAnalyticsCategories[categoryName]
	if !ok {
		return nil, fmt.Errorf("%w: category %s not found", ErrNotFound, categoryName)
	}

	cat.InputType = inputType
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

	all := make([]CallAnalyticsCategory, 0, len(b.callAnalyticsCategories))
	for _, c := range b.callAnalyticsCategories {
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

	v, ok := b.vocabularies[vocabularyName]
	if !ok {
		return nil, fmt.Errorf("%w: vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	cp := *v

	return &cp, nil
}

// UpdateVocabulary updates an existing custom vocabulary.
func (b *InMemoryBackend) UpdateVocabulary(
	vocabularyName, languageCode string,
) (*Vocabulary, error) {
	if vocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	b.mu.Lock("UpdateVocabulary")
	defer b.mu.Unlock()

	v, ok := b.vocabularies[vocabularyName]
	if !ok {
		return nil, fmt.Errorf("%w: vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	if languageCode != "" {
		v.LanguageCode = languageCode
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

	if _, ok := b.vocabularies[vocabularyName]; !ok {
		return fmt.Errorf("%w: vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	delete(b.vocabularies, vocabularyName)

	return nil
}

// ListVocabularies returns custom vocabularies with optional state filter and pagination.
func (b *InMemoryBackend) ListVocabularies(stateFilter, nextToken string) ([]Vocabulary, string) {
	b.mu.RLock("ListVocabularies")
	defer b.mu.RUnlock()

	all := make([]Vocabulary, 0, len(b.vocabularies))
	for _, v := range b.vocabularies {
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

	f, ok := b.vocabularyFilters[vocabularyFilterName]
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
func (b *InMemoryBackend) UpdateVocabularyFilter(
	vocabularyFilterName, languageCode string,
) (*VocabularyFilter, error) {
	if vocabularyFilterName == "" {
		return nil, fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	b.mu.Lock("UpdateVocabularyFilter")
	defer b.mu.Unlock()

	f, ok := b.vocabularyFilters[vocabularyFilterName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s not found",
			ErrNotFound,
			vocabularyFilterName,
		)
	}

	if languageCode != "" {
		f.LanguageCode = languageCode
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

	if _, ok := b.vocabularyFilters[vocabularyFilterName]; !ok {
		return fmt.Errorf("%w: vocabulary filter %s not found", ErrNotFound, vocabularyFilterName)
	}

	delete(b.vocabularyFilters, vocabularyFilterName)

	return nil
}

// ListVocabularyFilters returns all vocabulary filters with pagination.
func (b *InMemoryBackend) ListVocabularyFilters(nextToken string) ([]VocabularyFilter, string) {
	b.mu.RLock("ListVocabularyFilters")
	defer b.mu.RUnlock()

	all := make([]VocabularyFilter, 0, len(b.vocabularyFilters))
	for _, f := range b.vocabularyFilters {
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

	v, ok := b.medicalVocabularies[vocabularyName]
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

	v, ok := b.medicalVocabularies[vocabularyName]
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

	if _, ok := b.medicalVocabularies[vocabularyName]; !ok {
		return fmt.Errorf("%w: medical vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	delete(b.medicalVocabularies, vocabularyName)

	return nil
}

// ListMedicalVocabularies returns medical vocabularies with optional state filter and pagination.
func (b *InMemoryBackend) ListMedicalVocabularies(
	stateFilter, nextToken string,
) ([]MedicalVocabulary, string) {
	b.mu.RLock("ListMedicalVocabularies")
	defer b.mu.RUnlock()

	all := make([]MedicalVocabulary, 0, len(b.medicalVocabularies))
	for _, v := range b.medicalVocabularies {
		if stateFilter == "" || v.VocabularyState == stateFilter {
			all = append(all, *v)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].VocabularyName < all[j].VocabularyName })

	return paginateList(all, nextToken)
}

// --- Medical Scribe jobs ---

// StartMedicalScribeJob creates a new Medical Scribe job.
func (b *InMemoryBackend) StartMedicalScribeJob(jobName, _ string) (*MedicalScribeJob, error) {
	if jobName == "" {
		return nil, fmt.Errorf("%w: MedicalScribeJobName is required", ErrValidation)
	}

	b.mu.Lock("StartMedicalScribeJob")
	defer b.mu.Unlock()

	if _, ok := b.medicalScribeJobs[jobName]; ok {
		return nil, fmt.Errorf(
			"%w: medical scribe job %s already exists",
			ErrAlreadyExists,
			jobName,
		)
	}

	now := time.Now()
	job := &MedicalScribeJob{
		MedicalScribeJobName:   jobName,
		MedicalScribeJobStatus: jobStatusCompleted,
		CreationTime:           now,
		CompletionTime:         now,
	}
	b.medicalScribeJobs[jobName] = job

	cp := *job

	return &cp, nil
}

// GetMedicalScribeJob returns a Medical Scribe job by name.
func (b *InMemoryBackend) GetMedicalScribeJob(jobName string) (*MedicalScribeJob, error) {
	b.mu.RLock("GetMedicalScribeJob")
	defer b.mu.RUnlock()

	job, ok := b.medicalScribeJobs[jobName]
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

	all := make([]MedicalScribeJob, 0, len(b.medicalScribeJobs))
	for _, j := range b.medicalScribeJobs {
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
	jobName, languageCode, _ string,
) (*MedicalTranscriptionJob, error) {
	if jobName == "" {
		return nil, fmt.Errorf("%w: MedicalTranscriptionJobName is required", ErrValidation)
	}

	b.mu.Lock("StartMedicalTranscriptionJob")
	defer b.mu.Unlock()

	if _, ok := b.medicalTranscriptionJobs[jobName]; ok {
		return nil, fmt.Errorf(
			"%w: medical transcription job %s already exists",
			ErrAlreadyExists,
			jobName,
		)
	}

	now := time.Now()
	job := &MedicalTranscriptionJob{
		MedicalTranscriptionJobName: jobName,
		TranscriptionJobStatus:      jobStatusCompleted,
		LanguageCode:                languageCode,
		CreationTime:                now,
		CompletionTime:              now,
	}
	b.medicalTranscriptionJobs[jobName] = job

	cp := *job

	return &cp, nil
}

// GetMedicalTranscriptionJob returns a Medical Transcription job by name.
func (b *InMemoryBackend) GetMedicalTranscriptionJob(
	jobName string,
) (*MedicalTranscriptionJob, error) {
	b.mu.RLock("GetMedicalTranscriptionJob")
	defer b.mu.RUnlock()

	job, ok := b.medicalTranscriptionJobs[jobName]
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

	all := make([]MedicalTranscriptionJob, 0, len(b.medicalTranscriptionJobs))
	for _, j := range b.medicalTranscriptionJobs {
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

	m, ok := b.languageModels[modelName]
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

	all := make([]LanguageModel, 0, len(b.languageModels))
	for _, m := range b.languageModels {
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
