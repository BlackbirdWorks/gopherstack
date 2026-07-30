package transcribe

import (
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	// transcribeDefaultPageSize is the page size used when a caller does not supply
	// MaxResults. Amazon Transcribe's List* operations document a default of 5 when
	// MaxResults is omitted, but gopherstack intentionally returns a larger default
	// page (the documented maximum) since real clients always page via NextToken
	// regardless of page size, so a larger unrequested default is non-breaking.
	transcribeDefaultPageSize = 100

	// transcribeMaxResultsUpperBound is the documented maximum value for MaxResults
	// across every Transcribe List* operation (ListTranscriptionJobs,
	// ListVocabularies, ListVocabularyFilters, ListMedicalVocabularies,
	// ListMedicalScribeJobs, ListCallAnalyticsCategories,
	// ListMedicalTranscriptionJobs, ListCallAnalyticsJobs, ListLanguageModels):
	// "Valid Range: Minimum value of 1. Maximum value of 100." The documented
	// minimum is 1; gopherstack has no notion of a 0-or-negative caller value other
	// than "not supplied", handled as the default case below.
	transcribeMaxResultsUpperBound = 100

	// job/scribe status constants.
	jobStatusQueued     = "QUEUED"
	jobStatusInProgress = "IN_PROGRESS"
	jobStatusCompleted  = "COMPLETED"
	jobStatusFailed     = "FAILED"

	// vocabulary state constants.
	vocabStateReady = "READY"

	// language model status constant – gopherstack immediately completes models.
	modelStatusCompleted = "COMPLETED"

	// output location type constants, mirroring OutputLocationType.
	outputLocationCustomerBucket = "CUSTOMER_BUCKET"
	outputLocationServiceBucket  = "SERVICE_BUCKET"

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
// maxResults honors a caller-supplied MaxResults, clamped to the real API's documented
// bounds (1-100); a value of 0 (not supplied) falls back to transcribeDefaultPageSize.
// It returns the page slice and the next-page token (empty string if last page).
func paginateList[T any](all []T, nextToken string, maxResults int32) ([]T, string) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []T{}, ""
	}

	end := startIdx + int(clampMaxResults(maxResults))

	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

// clampMaxResults honors a caller-supplied MaxResults value, clamping it to the
// documented [1, 100] range shared by every Transcribe List* operation. A value <= 0
// (i.e. not supplied) falls back to transcribeDefaultPageSize rather than the real
// API's undocumented-in-the-wire default of 5, since gopherstack clients always page
// via NextToken and a larger unrequested default page is non-breaking.
func clampMaxResults(maxResults int32) int32 {
	switch {
	case maxResults <= 0:
		return transcribeDefaultPageSize
	case maxResults > transcribeMaxResultsUpperBound:
		return transcribeMaxResultsUpperBound
	default:
		return maxResults
	}
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

// outputLocationType reports whether a job's output lands in a customer-specified
// S3 bucket or the service-managed bucket, matching the real OutputLocationType
// values Amazon Transcribe returns alongside CUSTOMER_BUCKET/SERVICE_BUCKET.
func outputLocationType(outputBucketName string) string {
	if outputBucketName != "" {
		return outputLocationCustomerBucket
	}

	return outputLocationServiceBucket
}

// matchesNameContains reports whether name contains the (case-insensitive)
// substring filter, matching the NameContains/JobNameContains behavior AWS
// documents for its List* operations ("the search is not case sensitive").
// An empty filter matches everything.
func matchesNameContains(name, filter string) bool {
	if filter == "" {
		return true
	}

	return strings.Contains(strings.ToLower(name), strings.ToLower(filter))
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
