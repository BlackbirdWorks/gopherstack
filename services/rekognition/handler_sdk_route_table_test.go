package rekognition_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Rekognition
// operation, extracted from rekognition@v1.54.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("RekognitionService.<Op>")
// and always POSTs to "/" -- Rekognition is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header. The
// target prefix ("RekognitionService", not "Rekognition" or
// "AmazonRekognition") is not guessable from the service or directory name
// -- read directly from serializers.go, as instructed. ExtractOperation and
// Handler() (via buildOps()'s merged map, dispatched through h.dispatch)
// both derive the action the same way (TrimPrefix on
// "RekognitionService."), so the class of bug this table catches is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case), not a route-template mismatch.
//
// GetSupportedOperations() here is `for name := range h.ops`, i.e. it is
// h.ops's key set by construction rather than an independently maintained
// list -- so it structurally cannot diverge from the dispatch map, and
// diffing it separately from buildOps() (the fifteen per-family *Ops()
// builders merged via maps.Copy) is not a second independent check the way
// it is for every other service in this campaign. The static-diff coverage
// this table provides is instead: the dispatch map's 75 keys (extracted
// directly from the fifteen family functions) match the real SDK's 75 ops
// exactly in both directions -- zero mismatches, no dead or excluded keys --
// which rules out a key that is wrong (typo/case) though not a
// GetSupportedOperations()/dispatch-map divergence, since none is possible
// here.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("RekognitionService.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AssociateFaces", "RekognitionService.AssociateFaces"},
		{"CompareFaces", "RekognitionService.CompareFaces"},
		{"CopyProjectVersion", "RekognitionService.CopyProjectVersion"},
		{"CreateCollection", "RekognitionService.CreateCollection"},
		{"CreateDataset", "RekognitionService.CreateDataset"},
		{"CreateFaceLivenessSession", "RekognitionService.CreateFaceLivenessSession"},
		{"CreateProject", "RekognitionService.CreateProject"},
		{"CreateProjectVersion", "RekognitionService.CreateProjectVersion"},
		{"CreateStreamProcessor", "RekognitionService.CreateStreamProcessor"},
		{"CreateUser", "RekognitionService.CreateUser"},
		{"DeleteCollection", "RekognitionService.DeleteCollection"},
		{"DeleteDataset", "RekognitionService.DeleteDataset"},
		{"DeleteFaces", "RekognitionService.DeleteFaces"},
		{"DeleteProject", "RekognitionService.DeleteProject"},
		{"DeleteProjectPolicy", "RekognitionService.DeleteProjectPolicy"},
		{"DeleteProjectVersion", "RekognitionService.DeleteProjectVersion"},
		{"DeleteStreamProcessor", "RekognitionService.DeleteStreamProcessor"},
		{"DeleteUser", "RekognitionService.DeleteUser"},
		{"DescribeCollection", "RekognitionService.DescribeCollection"},
		{"DescribeDataset", "RekognitionService.DescribeDataset"},
		{"DescribeProjects", "RekognitionService.DescribeProjects"},
		{"DescribeProjectVersions", "RekognitionService.DescribeProjectVersions"},
		{"DescribeStreamProcessor", "RekognitionService.DescribeStreamProcessor"},
		{"DetectCustomLabels", "RekognitionService.DetectCustomLabels"},
		{"DetectFaces", "RekognitionService.DetectFaces"},
		{"DetectLabels", "RekognitionService.DetectLabels"},
		{"DetectModerationLabels", "RekognitionService.DetectModerationLabels"},
		{"DetectProtectiveEquipment", "RekognitionService.DetectProtectiveEquipment"},
		{"DetectText", "RekognitionService.DetectText"},
		{"DisassociateFaces", "RekognitionService.DisassociateFaces"},
		{"DistributeDatasetEntries", "RekognitionService.DistributeDatasetEntries"},
		{"GetCelebrityInfo", "RekognitionService.GetCelebrityInfo"},
		{"GetCelebrityRecognition", "RekognitionService.GetCelebrityRecognition"},
		{"GetContentModeration", "RekognitionService.GetContentModeration"},
		{"GetFaceDetection", "RekognitionService.GetFaceDetection"},
		{"GetFaceLivenessSessionResults", "RekognitionService.GetFaceLivenessSessionResults"},
		{"GetFaceSearch", "RekognitionService.GetFaceSearch"},
		{"GetLabelDetection", "RekognitionService.GetLabelDetection"},
		{"GetMediaAnalysisJob", "RekognitionService.GetMediaAnalysisJob"},
		{"GetPersonTracking", "RekognitionService.GetPersonTracking"},
		{"GetSegmentDetection", "RekognitionService.GetSegmentDetection"},
		{"GetTextDetection", "RekognitionService.GetTextDetection"},
		{"IndexFaces", "RekognitionService.IndexFaces"},
		{"ListCollections", "RekognitionService.ListCollections"},
		{"ListDatasetEntries", "RekognitionService.ListDatasetEntries"},
		{"ListDatasetLabels", "RekognitionService.ListDatasetLabels"},
		{"ListFaces", "RekognitionService.ListFaces"},
		{"ListMediaAnalysisJobs", "RekognitionService.ListMediaAnalysisJobs"},
		{"ListProjectPolicies", "RekognitionService.ListProjectPolicies"},
		{"ListStreamProcessors", "RekognitionService.ListStreamProcessors"},
		{"ListTagsForResource", "RekognitionService.ListTagsForResource"},
		{"ListUsers", "RekognitionService.ListUsers"},
		{"PutProjectPolicy", "RekognitionService.PutProjectPolicy"},
		{"RecognizeCelebrities", "RekognitionService.RecognizeCelebrities"},
		{"SearchFaces", "RekognitionService.SearchFaces"},
		{"SearchFacesByImage", "RekognitionService.SearchFacesByImage"},
		{"SearchUsers", "RekognitionService.SearchUsers"},
		{"SearchUsersByImage", "RekognitionService.SearchUsersByImage"},
		{"StartCelebrityRecognition", "RekognitionService.StartCelebrityRecognition"},
		{"StartContentModeration", "RekognitionService.StartContentModeration"},
		{"StartFaceDetection", "RekognitionService.StartFaceDetection"},
		{"StartFaceSearch", "RekognitionService.StartFaceSearch"},
		{"StartLabelDetection", "RekognitionService.StartLabelDetection"},
		{"StartMediaAnalysisJob", "RekognitionService.StartMediaAnalysisJob"},
		{"StartPersonTracking", "RekognitionService.StartPersonTracking"},
		{"StartProjectVersion", "RekognitionService.StartProjectVersion"},
		{"StartSegmentDetection", "RekognitionService.StartSegmentDetection"},
		{"StartStreamProcessor", "RekognitionService.StartStreamProcessor"},
		{"StartTextDetection", "RekognitionService.StartTextDetection"},
		{"StopProjectVersion", "RekognitionService.StopProjectVersion"},
		{"StopStreamProcessor", "RekognitionService.StopStreamProcessor"},
		{"TagResource", "RekognitionService.TagResource"},
		{"UntagResource", "RekognitionService.UntagResource"},
		{"UpdateDatasetEntries", "RekognitionService.UpdateDatasetEntries"},
		{"UpdateStreamProcessor", "RekognitionService.UpdateStreamProcessor"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Rekognition
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel
// (ErrUnknownOperation, handler.go's dispatch() single production call
// site) that a dispatch-table key mismatch would produce.
//
// handleError's default case renders ErrUnknownOperation as
// "UnknownOperationException" -- but that default is the catch-all for
// *any* error not matched by the five typed cases above it (awserr.ErrNotFound,
// ErrNameInUse, ErrUserConflict, awserr.ErrAlreadyExists,
// awserr.ErrInvalidParameter), not a type unique to the dispatch miss, so
// asserting on that wire type alone would risk a false negative if some
// legitimate handler ever returned an error outside those five families
// (this service's own validation errors are all wrapped through ErrValidation,
// which chains to awserr.ErrInvalidParameter, so today none do -- but the
// type is still shared structurally, not proven safe by a single production
// call site the way workmail/transfer's genuinely-unique sentinels are).
// This test instead asserts on the dispatch-miss message text, which is
// unique per op: dispatch's fmt.Errorf("%w: operation %q not implemented",
// ErrUnknownOperation, action) always renders as `operation "<op>" not
// implemented`, mirroring athena's approach to the same shared-type risk.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := rekognition.NewInMemoryBackend("111122223333", "us-east-1")
			h := rekognition.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			// The op name is JSON-escaped in the response body, so the
			// expected substring uses literal backslash-quotes, not %q's
			// unescaped ones -- an unescaped %q silently never matches and
			// was caught by the destructive-op failure proof below.
			assert.NotContains(t, rec.Body.String(), fmt.Sprintf(`operation \"%s\" not implemented`, tc.op),
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
