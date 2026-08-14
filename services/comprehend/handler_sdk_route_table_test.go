package comprehend_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Amazon
// Comprehend operation, extracted from comprehend@v1.43.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "Comprehend_20171127.<Op>") and always POSTs to "/" -- Comprehend is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so dispatch is entirely by this
// one header, not a path template.
//
// This table covers all 85 real Comprehend ops (comprehend@v1.43.4).
//
// SELF-REFERENTIALLY COLLAPSED, not genuinely independent: unlike most
// services in this campaign, GetSupportedOperations() here is built by
// ranging over h.ops (buildOperations()'s own map) --
//
//	operations := make([]string, 0, len(h.ops))
//	for name := range h.ops { operations = append(operations, name) }
//
// -- so it is not a second, independently hand-maintained source; it IS
// the dispatch map's key set, just re-derived. Diffing this SDK list
// against GetSupportedOperations() is therefore only ONE real check (the
// dispatch map vs. the SDK), not two independent ones -- confirmed by
// dumping GetSupportedOperations() at runtime and diffing byte-for-byte
// against this exact list: zero mismatches. buildOperations() itself also
// assembles most of its 85 keys programmatically from two family-spec
// maps (asyncJobSpecs(), resourceSpecs()) using Go string concatenation
// ("Start"+prefix, "Describe"+prefix, ...) rather than literal op-name
// strings, with a further ~20 entries added as individual literals
// afterward -- the buildOperations() doc comments (noDelete/noStop)
// record several real AWS asymmetries (e.g. Dataset has no DeleteDataset,
// DocumentClassificationJob/TopicsDetectionJob have no Stop*Job) that this
// table's exhaustive real-op-name list independently confirms are handled
// correctly: no ops corresponding to those excluded combinations exist in
// the real SDK's target list either.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Comprehend_20171127.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchDetectDominantLanguage", "Comprehend_20171127.BatchDetectDominantLanguage"},
		{"BatchDetectEntities", "Comprehend_20171127.BatchDetectEntities"},
		{"BatchDetectKeyPhrases", "Comprehend_20171127.BatchDetectKeyPhrases"},
		{"BatchDetectSentiment", "Comprehend_20171127.BatchDetectSentiment"},
		{"BatchDetectSyntax", "Comprehend_20171127.BatchDetectSyntax"},
		{"BatchDetectTargetedSentiment", "Comprehend_20171127.BatchDetectTargetedSentiment"},
		{"ClassifyDocument", "Comprehend_20171127.ClassifyDocument"},
		{"ContainsPiiEntities", "Comprehend_20171127.ContainsPiiEntities"},
		{"CreateDataset", "Comprehend_20171127.CreateDataset"},
		{"CreateDocumentClassifier", "Comprehend_20171127.CreateDocumentClassifier"},
		{"CreateEndpoint", "Comprehend_20171127.CreateEndpoint"},
		{"CreateEntityRecognizer", "Comprehend_20171127.CreateEntityRecognizer"},
		{"CreateFlywheel", "Comprehend_20171127.CreateFlywheel"},
		{"DeleteDocumentClassifier", "Comprehend_20171127.DeleteDocumentClassifier"},
		{"DeleteEndpoint", "Comprehend_20171127.DeleteEndpoint"},
		{"DeleteEntityRecognizer", "Comprehend_20171127.DeleteEntityRecognizer"},
		{"DeleteFlywheel", "Comprehend_20171127.DeleteFlywheel"},
		{"DeleteResourcePolicy", "Comprehend_20171127.DeleteResourcePolicy"},
		{"DescribeDataset", "Comprehend_20171127.DescribeDataset"},
		{"DescribeDocumentClassificationJob", "Comprehend_20171127.DescribeDocumentClassificationJob"},
		{"DescribeDocumentClassifier", "Comprehend_20171127.DescribeDocumentClassifier"},
		{"DescribeDominantLanguageDetectionJob", "Comprehend_20171127.DescribeDominantLanguageDetectionJob"},
		{"DescribeEndpoint", "Comprehend_20171127.DescribeEndpoint"},
		{"DescribeEntitiesDetectionJob", "Comprehend_20171127.DescribeEntitiesDetectionJob"},
		{"DescribeEntityRecognizer", "Comprehend_20171127.DescribeEntityRecognizer"},
		{"DescribeEventsDetectionJob", "Comprehend_20171127.DescribeEventsDetectionJob"},
		{"DescribeFlywheel", "Comprehend_20171127.DescribeFlywheel"},
		{"DescribeFlywheelIteration", "Comprehend_20171127.DescribeFlywheelIteration"},
		{"DescribeKeyPhrasesDetectionJob", "Comprehend_20171127.DescribeKeyPhrasesDetectionJob"},
		{"DescribePiiEntitiesDetectionJob", "Comprehend_20171127.DescribePiiEntitiesDetectionJob"},
		{"DescribeResourcePolicy", "Comprehend_20171127.DescribeResourcePolicy"},
		{"DescribeSentimentDetectionJob", "Comprehend_20171127.DescribeSentimentDetectionJob"},
		{"DescribeTargetedSentimentDetectionJob", "Comprehend_20171127.DescribeTargetedSentimentDetectionJob"},
		{"DescribeTopicsDetectionJob", "Comprehend_20171127.DescribeTopicsDetectionJob"},
		{"DetectDominantLanguage", "Comprehend_20171127.DetectDominantLanguage"},
		{"DetectEntities", "Comprehend_20171127.DetectEntities"},
		{"DetectKeyPhrases", "Comprehend_20171127.DetectKeyPhrases"},
		{"DetectPiiEntities", "Comprehend_20171127.DetectPiiEntities"},
		{"DetectSentiment", "Comprehend_20171127.DetectSentiment"},
		{"DetectSyntax", "Comprehend_20171127.DetectSyntax"},
		{"DetectTargetedSentiment", "Comprehend_20171127.DetectTargetedSentiment"},
		{"DetectToxicContent", "Comprehend_20171127.DetectToxicContent"},
		{"ImportModel", "Comprehend_20171127.ImportModel"},
		{"ListDatasets", "Comprehend_20171127.ListDatasets"},
		{"ListDocumentClassificationJobs", "Comprehend_20171127.ListDocumentClassificationJobs"},
		{"ListDocumentClassifiers", "Comprehend_20171127.ListDocumentClassifiers"},
		{"ListDocumentClassifierSummaries", "Comprehend_20171127.ListDocumentClassifierSummaries"},
		{"ListDominantLanguageDetectionJobs", "Comprehend_20171127.ListDominantLanguageDetectionJobs"},
		{"ListEndpoints", "Comprehend_20171127.ListEndpoints"},
		{"ListEntitiesDetectionJobs", "Comprehend_20171127.ListEntitiesDetectionJobs"},
		{"ListEntityRecognizers", "Comprehend_20171127.ListEntityRecognizers"},
		{"ListEntityRecognizerSummaries", "Comprehend_20171127.ListEntityRecognizerSummaries"},
		{"ListEventsDetectionJobs", "Comprehend_20171127.ListEventsDetectionJobs"},
		{"ListFlywheelIterationHistory", "Comprehend_20171127.ListFlywheelIterationHistory"},
		{"ListFlywheels", "Comprehend_20171127.ListFlywheels"},
		{"ListKeyPhrasesDetectionJobs", "Comprehend_20171127.ListKeyPhrasesDetectionJobs"},
		{"ListPiiEntitiesDetectionJobs", "Comprehend_20171127.ListPiiEntitiesDetectionJobs"},
		{"ListSentimentDetectionJobs", "Comprehend_20171127.ListSentimentDetectionJobs"},
		{"ListTagsForResource", "Comprehend_20171127.ListTagsForResource"},
		{"ListTargetedSentimentDetectionJobs", "Comprehend_20171127.ListTargetedSentimentDetectionJobs"},
		{"ListTopicsDetectionJobs", "Comprehend_20171127.ListTopicsDetectionJobs"},
		{"PutResourcePolicy", "Comprehend_20171127.PutResourcePolicy"},
		{"StartDocumentClassificationJob", "Comprehend_20171127.StartDocumentClassificationJob"},
		{"StartDominantLanguageDetectionJob", "Comprehend_20171127.StartDominantLanguageDetectionJob"},
		{"StartEntitiesDetectionJob", "Comprehend_20171127.StartEntitiesDetectionJob"},
		{"StartEventsDetectionJob", "Comprehend_20171127.StartEventsDetectionJob"},
		{"StartFlywheelIteration", "Comprehend_20171127.StartFlywheelIteration"},
		{"StartKeyPhrasesDetectionJob", "Comprehend_20171127.StartKeyPhrasesDetectionJob"},
		{"StartPiiEntitiesDetectionJob", "Comprehend_20171127.StartPiiEntitiesDetectionJob"},
		{"StartSentimentDetectionJob", "Comprehend_20171127.StartSentimentDetectionJob"},
		{"StartTargetedSentimentDetectionJob", "Comprehend_20171127.StartTargetedSentimentDetectionJob"},
		{"StartTopicsDetectionJob", "Comprehend_20171127.StartTopicsDetectionJob"},
		{"StopDominantLanguageDetectionJob", "Comprehend_20171127.StopDominantLanguageDetectionJob"},
		{"StopEntitiesDetectionJob", "Comprehend_20171127.StopEntitiesDetectionJob"},
		{"StopEventsDetectionJob", "Comprehend_20171127.StopEventsDetectionJob"},
		{"StopKeyPhrasesDetectionJob", "Comprehend_20171127.StopKeyPhrasesDetectionJob"},
		{"StopPiiEntitiesDetectionJob", "Comprehend_20171127.StopPiiEntitiesDetectionJob"},
		{"StopSentimentDetectionJob", "Comprehend_20171127.StopSentimentDetectionJob"},
		{"StopTargetedSentimentDetectionJob", "Comprehend_20171127.StopTargetedSentimentDetectionJob"},
		{"StopTrainingDocumentClassifier", "Comprehend_20171127.StopTrainingDocumentClassifier"},
		{"StopTrainingEntityRecognizer", "Comprehend_20171127.StopTrainingEntityRecognizer"},
		{"TagResource", "Comprehend_20171127.TagResource"},
		{"UntagResource", "Comprehend_20171127.UntagResource"},
		{"UpdateEndpoint", "Comprehend_20171127.UpdateEndpoint"},
		{"UpdateFlywheel", "Comprehend_20171127.UpdateFlywheel"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Comprehend
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to dispatch's unmatched-route branch
// (fmt.Errorf("%w: operation %q", ErrValidation, action), handler.go's
// single production call site for this exact phrasing).
//
// This asserts on MESSAGE TEXT (`operation "<op>"`, JSON-escaped to
// `operation \"<op>\"` on the wire), not wire type: ErrValidation resolves
// to the shared InvalidRequestException, the same type ordinary validation
// failures elsewhere in this service produce (missing Text, missing
// LanguageCode, malformed JSON), so a type assertion here would not
// distinguish a dispatch miss from a routine validation failure. The
// `operation %q` phrasing is unique to this one call site (grepped across
// the package) -- every other ErrValidation use in this service has
// different message text. A first version of this assertion checked for
// bare quotes and could never fail (json.Marshal always escapes them) --
// confirmed by deliberately mis-wiring a dispatch key and observing the
// bare-quote assertion silently pass; fixed to match the escaped form.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := comprehend.NewHandler(comprehend.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			// The message is JSON-encoded, so a literal `"` in the source
			// message becomes `\"` on the wire -- match the escaped form,
			// not bare quotes (a bare-quote assertion here would never
			// match any JSON body and could never fail).
			assert.NotContains(t, rec.Body.String(), `operation \"`+tc.op+`\"`,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
