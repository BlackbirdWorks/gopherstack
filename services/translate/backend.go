package translate

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

var (
	// ErrNotFound is returned when a requested resource is absent.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrConflict is returned when a resource already exists.
	ErrConflict = errors.New("ResourceInUseException")
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = errors.New("InvalidRequestException")
)

// TerminologyData holds imported terminology file bytes.
type TerminologyData struct {
	Format string
	File   []byte
}

// EncryptionKey holds optional KMS encryption key details.
type EncryptionKey struct {
	Type string
	ID   string
}

// Terminology stores a custom terminology resource.
type Terminology struct {
	TerminologyData *TerminologyData
	EncryptionKey   *EncryptionKey
	Tags            map[string]string
	CreatedAt       time.Time
	LastUpdatedAt   time.Time
	ARN             string
	Name            string
	Description     string
	SourceLanguage  string
	Directionality  string
	Format          string
	TargetLanguages []string
	SizeBytes       int
	TermCount       int
}

// ParallelDataConfig holds S3 data config for parallel data.
type ParallelDataConfig struct {
	S3URI  string
	Format string
}

// ParallelData stores a parallel data resource.
type ParallelData struct {
	ParallelDataConfig *ParallelDataConfig
	EncryptionKey      *EncryptionKey
	Tags               map[string]string
	CreatedAt          time.Time
	LastUpdatedAt      time.Time
	ARN                string
	Name               string
	Description        string
	SourceLanguage     string
	Status             string
	TargetLanguages    []string
}

// TranslationJob stores an async translation job.
type TranslationJob struct {
	InputDataConfig   map[string]any
	OutputDataConfig  map[string]any
	Settings          map[string]any
	Tags              map[string]string
	SubmittedAt       time.Time
	EndAt             time.Time
	JobID             string
	JobName           string
	JobStatus         string
	DataAccessRoleARN string
	SourceLanguage    string
	Message           string
	TargetLanguages   []string
	TerminologyNames  []string
	ParallelDataNames []string
	stopRequested     bool
}

// InMemoryBackend stores Translate state for concurrent requests.
type InMemoryBackend struct {
	terminologies map[string]*Terminology
	parallelData  map[string]*ParallelData
	jobs          map[string]*TranslationJob
	tags          map[string]map[string]string
	accountID     string
	region        string
	mu            sync.RWMutex
}

// NewInMemoryBackend returns an initialized InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:     accountID,
		region:        region,
		terminologies: make(map[string]*Terminology),
		parallelData:  make(map[string]*ParallelData),
		jobs:          make(map[string]*TranslationJob),
		tags:          make(map[string]map[string]string),
	}
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.terminologies = make(map[string]*Terminology)
	b.parallelData = make(map[string]*ParallelData)
	b.jobs = make(map[string]*TranslationJob)
	b.tags = make(map[string]map[string]string)
}

func (b *InMemoryBackend) terminologyARN(name string) string {
	return arn.Build("translate", b.region, b.accountID, "terminology/"+name)
}

func (b *InMemoryBackend) parallelDataARN(name string) string {
	return arn.Build("translate", b.region, b.accountID, "parallel-data/"+name)
}

// parseCSVLanguages extracts source/target language codes and term count from CSV bytes.
// CSV header row is: sourceLang,targetLang1[,targetLang2,...]; subsequent rows are terms.
func parseCSVLanguages(csvBytes []byte) (string, []string, int) {
	const minCols = 2

	lines := strings.Split(strings.TrimSpace(string(csvBytes)), "\n")
	if len(lines) == 0 {
		return "", nil, 0
	}

	var srcLang string
	var targets []string

	// Parse header line.
	header := strings.Split(strings.TrimSpace(lines[0]), ",")
	if len(header) >= minCols {
		srcLang = strings.TrimSpace(header[0])
		for _, col := range header[1:] {
			if t := strings.TrimSpace(col); t != "" {
				targets = append(targets, t)
			}
		}
	}

	// Count non-empty, non-comment data rows.
	termCount := 0
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			termCount++
		}
	}

	return srcLang, targets, termCount
}

// ImportTerminology creates or overwrites a custom terminology.
func (b *InMemoryBackend) ImportTerminology(
	name, description string,
	data *TerminologyData,
	encKey *EncryptionKey,
	tags map[string]string,
) (*Terminology, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if data == nil {
		return nil, fmt.Errorf("%w: TerminologyData is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC()
	resourceARN := b.terminologyARN(name)

	srcLang, targetLangs, termCount := parseCSVLanguages(data.File)
	if srcLang == "" {
		srcLang = "en"
	}

	existing, exists := b.terminologies[name]
	if exists {
		existing.Description = description
		existing.TerminologyData = data
		existing.EncryptionKey = encKey
		existing.LastUpdatedAt = now
		existing.Format = data.Format
		existing.SizeBytes = len(data.File)
		existing.SourceLanguage = srcLang
		existing.TargetLanguages = targetLangs
		existing.TermCount = termCount

		if tags != nil {
			existing.Tags = tags
			b.tags[resourceARN] = copyMap(tags)
		}

		return existing, nil
	}

	term := &Terminology{
		ARN:             resourceARN,
		Name:            name,
		Description:     description,
		TerminologyData: data,
		EncryptionKey:   encKey,
		Tags:            tags,
		CreatedAt:       now,
		LastUpdatedAt:   now,
		Format:          data.Format,
		SizeBytes:       len(data.File),
		Directionality:  "UNI",
		SourceLanguage:  srcLang,
		TargetLanguages: targetLangs,
		TermCount:       termCount,
	}
	b.terminologies[name] = term

	if tags != nil {
		b.tags[resourceARN] = copyMap(tags)
	}

	return term, nil
}

// GetTerminology retrieves a terminology by name.
func (b *InMemoryBackend) GetTerminology(name string) (*Terminology, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.terminologies[name]
	if !ok {
		return nil, fmt.Errorf("%w: terminology %q not found", ErrNotFound, name)
	}

	return t, nil
}

// LookupTerminologies returns terminology entries for the given names (missing names skipped).
func (b *InMemoryBackend) LookupTerminologies(names []string) []*Terminology {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Terminology, 0, len(names))
	for _, name := range names {
		if t, ok := b.terminologies[name]; ok {
			out = append(out, t)
		}
	}

	return out
}

// DeleteTerminology removes a terminology by name.
func (b *InMemoryBackend) DeleteTerminology(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.terminologies[name]; !ok {
		return fmt.Errorf("%w: terminology %q not found", ErrNotFound, name)
	}

	resourceARN := b.terminologyARN(name)
	delete(b.terminologies, name)
	delete(b.tags, resourceARN)

	return nil
}

// ListTerminologies returns a paginated list of terminologies.
func (b *InMemoryBackend) ListTerminologies(maxResults int, nextToken string) ([]*Terminology, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := collections.SortedKeys(b.terminologies)

	return paginate(names, func(n string) *Terminology { return b.terminologies[n] }, maxResults, nextToken)
}

// CreateParallelData creates a new parallel data resource.
func (b *InMemoryBackend) CreateParallelData(
	name, description string,
	cfg *ParallelDataConfig,
	encKey *EncryptionKey,
	tags map[string]string,
) (*ParallelData, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.parallelData[name]; exists {
		return nil, fmt.Errorf("%w: parallel data %q already exists", ErrConflict, name)
	}

	now := time.Now().UTC()
	resourceARN := b.parallelDataARN(name)

	pd := &ParallelData{
		ARN:                resourceARN,
		Name:               name,
		Description:        description,
		ParallelDataConfig: cfg,
		EncryptionKey:      encKey,
		Tags:               tags,
		CreatedAt:          now,
		LastUpdatedAt:      now,
		Status:             "ACTIVE",
		SourceLanguage:     "en",
	}
	b.parallelData[name] = pd

	if tags != nil {
		b.tags[resourceARN] = copyMap(tags)
	}

	return pd, nil
}

// GetParallelData retrieves a parallel data resource by name.
func (b *InMemoryBackend) GetParallelData(name string) (*ParallelData, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pd, ok := b.parallelData[name]
	if !ok {
		return nil, fmt.Errorf("%w: parallel data %q not found", ErrNotFound, name)
	}

	return pd, nil
}

// UpdateParallelData updates an existing parallel data resource.
func (b *InMemoryBackend) UpdateParallelData(
	name, description string,
	cfg *ParallelDataConfig,
) (*ParallelData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pd, ok := b.parallelData[name]
	if !ok {
		return nil, fmt.Errorf("%w: parallel data %q not found", ErrNotFound, name)
	}

	pd.Description = description
	if cfg != nil {
		pd.ParallelDataConfig = cfg
	}

	pd.LastUpdatedAt = time.Now().UTC()

	return pd, nil
}

// DeleteParallelData removes a parallel data resource by name.
func (b *InMemoryBackend) DeleteParallelData(name string) (*ParallelData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pd, ok := b.parallelData[name]
	if !ok {
		return nil, fmt.Errorf("%w: parallel data %q not found", ErrNotFound, name)
	}

	resourceARN := b.parallelDataARN(name)
	delete(b.parallelData, name)
	delete(b.tags, resourceARN)

	return pd, nil
}

// ListParallelData returns a paginated list of parallel data resources.
func (b *InMemoryBackend) ListParallelData(maxResults int, nextToken string) ([]*ParallelData, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := collections.SortedKeys(b.parallelData)

	return paginate(names, func(n string) *ParallelData { return b.parallelData[n] }, maxResults, nextToken)
}

// StartTextTranslationJob creates a new async translation job.
func (b *InMemoryBackend) StartTextTranslationJob(
	jobName, dataAccessRoleARN, sourceLang string,
	targetLangs, terminologyNames, parallelDataNames []string,
	inputCfg, outputCfg, settings map[string]any,
	tags map[string]string,
) (*TranslationJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	jobID := uuid.New().String()

	job := &TranslationJob{
		JobID:             jobID,
		JobName:           jobName,
		JobStatus:         "IN_PROGRESS",
		DataAccessRoleARN: dataAccessRoleARN,
		SourceLanguage:    sourceLang,
		TargetLanguages:   targetLangs,
		TerminologyNames:  terminologyNames,
		ParallelDataNames: parallelDataNames,
		InputDataConfig:   inputCfg,
		OutputDataConfig:  outputCfg,
		Settings:          settings,
		Tags:              tags,
		SubmittedAt:       time.Now().UTC(),
	}
	b.jobs[jobID] = job

	return job, nil
}

// StopTextTranslationJob requests stop of a translation job.
func (b *InMemoryBackend) StopTextTranslationJob(jobID string) (*TranslationJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobID)
	}

	if job.JobStatus != "IN_PROGRESS" && job.JobStatus != "SUBMITTED" {
		return nil, fmt.Errorf("%w: job %q is not stoppable (status: %s)", ErrValidation, jobID, job.JobStatus)
	}

	job.stopRequested = true
	job.JobStatus = "STOP_REQUESTED"
	job.EndAt = time.Now().UTC()

	return job, nil
}

// DescribeTextTranslationJob retrieves a translation job.
func (b *InMemoryBackend) DescribeTextTranslationJob(jobID string) (*TranslationJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobID)
	}

	return job, nil
}

// ListTextTranslationJobs returns a paginated list of translation jobs.
func (b *InMemoryBackend) ListTextTranslationJobs(
	statusFilter string,
	maxResults int,
	nextToken string,
) ([]*TranslationJob, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.jobs))

	for id, job := range b.jobs {
		if statusFilter == "" || strings.EqualFold(job.JobStatus, statusFilter) {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return paginate(ids, func(id string) *TranslationJob { return b.jobs[id] }, maxResults, nextToken)
}

// TagResource adds or replaces tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, newTags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], newTags)

	return nil
}

// UntagResource removes specific tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	for _, k := range keys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.arnExists(resourceARN) {
		return nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	return copyMap(b.tags[resourceARN]), nil
}

// arnExists checks whether the ARN corresponds to any stored resource.
func (b *InMemoryBackend) arnExists(resourceARN string) bool {
	for _, t := range b.terminologies {
		if t.ARN == resourceARN {
			return true
		}
	}

	for _, pd := range b.parallelData {
		if pd.ARN == resourceARN {
			return true
		}
	}

	return false
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func paginate[T any](keys []string, get func(string) T, maxResults int, nextToken string) ([]T, string) {
	const defaultMaxResults = 100

	if maxResults <= 0 {
		maxResults = defaultMaxResults
	}

	start := 0

	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := start + maxResults

	var outToken string

	if end < len(keys) {
		outToken = keys[end]
	} else {
		end = len(keys)
	}

	items := make([]T, 0, end-start)

	for _, k := range keys[start:end] {
		items = append(items, get(k))
	}

	return items, outToken
}
