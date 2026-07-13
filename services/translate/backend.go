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
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	// ErrNotFound is returned when a requested resource is absent.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrConflict is returned when a resource already exists.
	ErrConflict = errors.New("ResourceInUseException")
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = errors.New("InvalidRequestException")
)

// Job status values, matching aws-sdk-go-v2/service/translate/types.JobStatus.
// A freshly started job begins at jobStatusSubmitted and is advanced one step
// per DescribeTextTranslationJob call (see advanceJob), mirroring the
// SUBMITTED -> IN_PROGRESS -> COMPLETED/FAILED lifecycle real Translate jobs
// go through and the pattern established by services/comprehend.
const (
	jobStatusSubmitted     = "SUBMITTED"
	jobStatusInProgress    = "IN_PROGRESS"
	jobStatusCompleted     = "COMPLETED"
	jobStatusFailed        = "FAILED"
	jobStatusStopRequested = "STOP_REQUESTED"
	jobStatusStopped       = "STOPPED"

	// failedJobNameMarker lets tests deterministically drive a job to FAILED
	// by including this marker in JobName, matching services/comprehend's
	// failedMarker convention.
	failedJobNameMarker = "[fail]"
)

// Directionality values, matching aws-sdk-go-v2/service/translate/types.Directionality.
const (
	directionalityUni   = "UNI"
	directionalityMulti = "MULTI"
)

// TerminologyData holds imported terminology file bytes.
type TerminologyData struct {
	Format         string
	Directionality string
	File           []byte
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
	shouldFail        bool
}

// InMemoryBackend stores Translate state for concurrent requests.
//
// terminologies, parallelData, and jobs are *store.Table[T]-backed (see
// store_setup.go and pkgs/store's package doc); tags remains a plain map
// since its values are map[string]string, not *T, so there is nothing for
// store.Table to key on.
type InMemoryBackend struct {
	terminologies *store.Table[Terminology]
	parallelData  *store.Table[ParallelData]
	jobs          *store.Table[TranslationJob]
	tags          map[string]map[string]string
	registry      *store.Registry
	accountID     string
	region        string
	mu            sync.RWMutex
}

// NewInMemoryBackend returns an initialized InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
	}
	registerAllTables(b)

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
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

	directionality := data.Directionality
	switch directionality {
	case "":
		directionality = directionalityUni
	case directionalityUni, directionalityMulti:
		// valid, keep as specified
	default:
		return nil, fmt.Errorf("%w: Directionality must be UNI or MULTI", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC()
	resourceARN := b.terminologyARN(name)

	srcLang, targetLangs, termCount := parseCSVLanguages(data.File)
	if srcLang == "" {
		srcLang = "en"
	}

	existing, exists := b.terminologies.Get(name)
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
		existing.Directionality = directionality

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
		Directionality:  directionality,
		SourceLanguage:  srcLang,
		TargetLanguages: targetLangs,
		TermCount:       termCount,
	}
	b.terminologies.Put(term)

	if tags != nil {
		b.tags[resourceARN] = copyMap(tags)
	}

	return term, nil
}

// GetTerminology retrieves a terminology by name.
func (b *InMemoryBackend) GetTerminology(name string) (*Terminology, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.terminologies.Get(name)
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
		if t, ok := b.terminologies.Get(name); ok {
			out = append(out, t)
		}
	}

	return out
}

// DeleteTerminology removes a terminology by name.
func (b *InMemoryBackend) DeleteTerminology(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.terminologies.Has(name) {
		return fmt.Errorf("%w: terminology %q not found", ErrNotFound, name)
	}

	resourceARN := b.terminologyARN(name)
	b.terminologies.Delete(name)
	delete(b.tags, resourceARN)

	return nil
}

// ListTerminologies returns a paginated list of terminologies.
func (b *InMemoryBackend) ListTerminologies(maxResults int, nextToken string) ([]*Terminology, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := sortedNames(b.terminologies.All(), func(t *Terminology) string { return t.Name })

	return paginate(names, func(n string) *Terminology { return tableGet(b.terminologies, n) }, maxResults, nextToken)
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

	if b.parallelData.Has(name) {
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
	b.parallelData.Put(pd)

	if tags != nil {
		b.tags[resourceARN] = copyMap(tags)
	}

	return pd, nil
}

// GetParallelData retrieves a parallel data resource by name.
func (b *InMemoryBackend) GetParallelData(name string) (*ParallelData, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pd, ok := b.parallelData.Get(name)
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

	pd, ok := b.parallelData.Get(name)
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

	pd, ok := b.parallelData.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: parallel data %q not found", ErrNotFound, name)
	}

	resourceARN := b.parallelDataARN(name)
	b.parallelData.Delete(name)
	delete(b.tags, resourceARN)

	return pd, nil
}

// ListParallelData returns a paginated list of parallel data resources.
func (b *InMemoryBackend) ListParallelData(maxResults int, nextToken string) ([]*ParallelData, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := sortedNames(b.parallelData.All(), func(pd *ParallelData) string { return pd.Name })

	return paginate(names, func(n string) *ParallelData { return tableGet(b.parallelData, n) }, maxResults, nextToken)
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
		JobStatus:         jobStatusSubmitted,
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
		shouldFail:        strings.Contains(strings.ToLower(jobName), failedJobNameMarker),
	}
	b.jobs.Put(job)

	return job, nil
}

// StopTextTranslationJob requests stop of a translation job.
func (b *InMemoryBackend) StopTextTranslationJob(jobID string) (*TranslationJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobID)
	}

	if job.JobStatus != jobStatusInProgress && job.JobStatus != jobStatusSubmitted {
		return nil, fmt.Errorf("%w: job %q is not stoppable (status: %s)", ErrValidation, jobID, job.JobStatus)
	}

	job.stopRequested = true
	job.JobStatus = jobStatusStopRequested
	job.EndAt = time.Now().UTC()

	return job, nil
}

// DescribeTextTranslationJob retrieves a translation job and advances it one
// step through its lifecycle, matching services/comprehend's DescribeJob
// pattern: real batch translation jobs move from SUBMITTED to IN_PROGRESS to
// a terminal state (COMPLETED/FAILED, or STOPPED once stop was requested)
// asynchronously. Without this, a job started via StartTextTranslationJob
// would sit at its initial status forever and SDK callers polling
// DescribeTextTranslationJob (the documented way to track job progress) would
// never observe completion.
func (b *InMemoryBackend) DescribeTextTranslationJob(jobID string) (*TranslationJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobID)
	}

	advanceJob(job)

	return job, nil
}

// advanceJob moves job one step through its lifecycle. Called from
// DescribeTextTranslationJob so that each poll makes progress, the same
// convention services/comprehend uses for its analysis jobs.
func advanceJob(job *TranslationJob) {
	switch job.JobStatus {
	case jobStatusSubmitted:
		job.JobStatus = jobStatusInProgress
	case jobStatusInProgress:
		if job.shouldFail {
			job.JobStatus = jobStatusFailed
			job.Message = "simulated translation failure"
		} else {
			job.JobStatus = jobStatusCompleted
		}

		job.EndAt = time.Now().UTC()
	case jobStatusStopRequested:
		job.JobStatus = jobStatusStopped
		job.EndAt = time.Now().UTC()
	}
}

// ListTextTranslationJobs returns a paginated list of translation jobs.
func (b *InMemoryBackend) ListTextTranslationJobs(
	statusFilter string,
	maxResults int,
	nextToken string,
) ([]*TranslationJob, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := make([]string, 0, b.jobs.Len())

	for _, job := range b.jobs.All() {
		if statusFilter == "" || strings.EqualFold(job.JobStatus, statusFilter) {
			ids = append(ids, job.JobID)
		}
	}

	sort.Strings(ids)

	return paginate(ids, func(id string) *TranslationJob { return tableGet(b.jobs, id) }, maxResults, nextToken)
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
	found := false

	b.terminologies.Range(func(t *Terminology) bool {
		if t.ARN == resourceARN {
			found = true

			return false
		}

		return true
	})

	if found {
		return true
	}

	b.parallelData.Range(func(pd *ParallelData) bool {
		if pd.ARN == resourceARN {
			found = true

			return false
		}

		return true
	})

	return found
}

// sortedNames extracts a key from every item via keyFn and returns the
// resulting names in ascending sorted order, matching the deterministic
// ordering collections.SortedKeys previously gave callers iterating the raw
// map[string]*V this table replaced.
func sortedNames[V any](items []*V, keyFn func(*V) string) []string {
	names := make([]string, 0, len(items))
	for _, v := range items {
		names = append(names, keyFn(v))
	}

	sort.Strings(names)

	return names
}

// tableGet looks up id in t, returning nil if absent. It adapts
// [store.Table.Get]'s (value, ok) result to the single-value getter shape
// [paginate] expects.
func tableGet[V any](t *store.Table[V], id string) *V {
	v, _ := t.Get(id)

	return v
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
