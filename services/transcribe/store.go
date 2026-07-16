package transcribe

import (
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

	// ARN resource-type segments, matching the patterns documented in the
	// TagResource/ListTagsForResource/UntagResource SDK doc comments
	// (e.g. "arn:aws:transcribe:us-west-2:111122223333:transcription-job/name").
	resourceTypeTranscriptionJob        = "transcription-job"
	resourceTypeCallAnalyticsJob        = "call-analytics-job"
	resourceTypeCallAnalyticsCategory   = "call-analytics-category"
	resourceTypeMedicalScribeJob        = "medical-scribe-job"
	resourceTypeMedicalTranscriptionJob = "medical-transcription-job"
	resourceTypeVocabulary              = "vocabulary"
	resourceTypeVocabularyFilter        = "vocabulary-filter"
	resourceTypeMedicalVocabulary       = "medical-vocabulary"
	resourceTypeLanguageModel           = "language-model"
)

// resourceARN builds the ARN for a Transcribe resource of the given type and name,
// matching the format real AWS clients compute for TagResource/ListTagsForResource calls.
func resourceARN(resourceType, name string) string {
	return arn.Build("transcribe", config.DefaultRegion, defaultAccountID, resourceType+"/"+name)
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

// applyListOrURI resolves an update's "either list or URI" pair against the currently
// stored values, mirroring the mutual exclusivity AWS enforces between a resource's
// phrase/word list and its file-URI alternative (Vocabulary.Phrases/VocabularyFileURI,
// VocabularyFilter.Words/VocabularyFilterFileURI): a non-empty newList wins and clears
// the URI, a non-empty newURI wins and clears the list, and if neither is supplied the
// existing values pass through unchanged. Shared by UpdateVocabulary and
// UpdateVocabularyFilter so their otherwise-identical update shape doesn't need its own
// per-type duplicate block.
func applyListOrURI[T any](newList []T, newURI string, curList []T, curURI string) ([]T, string) {
	switch {
	case len(newList) > 0:
		return newList, ""
	case newURI != "":
		return nil, newURI
	default:
		return curList, curURI
	}
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
