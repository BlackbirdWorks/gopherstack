package transcribe_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Transcribe
// operation, extracted from transcribe@v1.58.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Transcribe.<Op>") and
// always POSTs to "/" -- Transcribe is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header.
//
// ExtractOperation (TrimPrefix on "Transcribe.", falling back to "Unknown"
// only when the header is empty or lacks the prefix -- never the case here
// since every case sets a well-formed header) and Handler() (via
// pkgs/service.HandleTarget splitting on "." and taking parts[1], then
// dispatch()'s h.ops flat map lookup) both resolve to the identical action
// string, so the class of bug this table catches is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case), not a
// route-template or splitting mismatch.
//
// This table covers all 43 real Transcribe ops (transcribe@v1.58.4) --
// confirmed by diffing both allSupportedOps() (returned by
// GetSupportedOperations(), a hand-written literal) and buildOps()'s h.ops
// map keys against this exact list: zero mismatches in either direction,
// no dead or excluded keys. The two diffs are genuinely independent -- both
// are separately-typed string literals, not built by ranging over each
// other.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Transcribe.` and pulling the suffix
// after the dot.
func sdkRouteCases() []string {
	return []string{
		"CreateCallAnalyticsCategory",
		"CreateLanguageModel",
		"CreateMedicalVocabulary",
		"CreateVocabulary",
		"CreateVocabularyFilter",
		"DeleteCallAnalyticsCategory",
		"DeleteCallAnalyticsJob",
		"DeleteLanguageModel",
		"DeleteMedicalScribeJob",
		"DeleteMedicalTranscriptionJob",
		"DeleteMedicalVocabulary",
		"DeleteTranscriptionJob",
		"DeleteVocabulary",
		"DeleteVocabularyFilter",
		"DescribeLanguageModel",
		"GetCallAnalyticsCategory",
		"GetCallAnalyticsJob",
		"GetMedicalScribeJob",
		"GetMedicalTranscriptionJob",
		"GetMedicalVocabulary",
		"GetTranscriptionJob",
		"GetVocabulary",
		"GetVocabularyFilter",
		"ListCallAnalyticsCategories",
		"ListCallAnalyticsJobs",
		"ListLanguageModels",
		"ListMedicalScribeJobs",
		"ListMedicalTranscriptionJobs",
		"ListMedicalVocabularies",
		"ListTagsForResource",
		"ListTranscriptionJobs",
		"ListVocabularies",
		"ListVocabularyFilters",
		"StartCallAnalyticsJob",
		"StartMedicalScribeJob",
		"StartMedicalTranscriptionJob",
		"StartTranscriptionJob",
		"TagResource",
		"UntagResource",
		"UpdateCallAnalyticsCategory",
		"UpdateMedicalVocabulary",
		"UpdateVocabulary",
		"UpdateVocabularyFilter",
	}
}

// TestExtractOperation_SDKRouteTable drives every real Transcribe
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss branch (dispatch()'s
// h.ops lookup miss, returning errUnknownAction, mapped by handleError's
// dedicated errors.Is(err, errUnknownAction) case to wire code
// "UnknownOperationException"). Grepped handler.go: "UnknownOperationException"
// is written in exactly that one handleError case -- the adjacent
// BadRequestException case covers a disjoint sentinel set (errInvalidRequest,
// awserr.ErrInvalidParameter, JSON syntax/type errors), so the dispatch-miss
// wire type is not reused by any legitimate validation path, and asserting
// on it directly is safe here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := transcribe.NewHandler(transcribe.NewInMemoryBackend())

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "Transcribe."+op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
