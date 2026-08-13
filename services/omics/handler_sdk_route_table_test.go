package omics_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real HealthOmics
// operation, extracted from omics@v1.49.5 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AbortMultipartReadSetUpload", "DELETE", "/sequencestore/PLACEHOLDER/upload/PLACEHOLDER/abort"},
		{"AcceptShare", "POST", "/share/PLACEHOLDER"},
		{"BatchDeleteReadSet", "POST", "/sequencestore/PLACEHOLDER/readset/batch/delete"},
		{"CancelAnnotationImportJob", "DELETE", "/import/annotation/PLACEHOLDER"},
		{"CancelRun", "POST", "/run/PLACEHOLDER/cancel"},
		{"CancelRunBatch", "POST", "/runBatch/cancel"},
		{"CancelVariantImportJob", "DELETE", "/import/variant/PLACEHOLDER"},
		{"CompleteMultipartReadSetUpload", "POST", "/sequencestore/PLACEHOLDER/upload/PLACEHOLDER/complete"},
		{"CreateAnnotationStore", "POST", "/annotationStore"},
		{"CreateAnnotationStoreVersion", "POST", "/annotationStore/PLACEHOLDER/version"},
		{"CreateConfiguration", "POST", "/configuration"},
		{"CreateMultipartReadSetUpload", "POST", "/sequencestore/PLACEHOLDER/upload"},
		{"CreateReferenceStore", "POST", "/referencestore"},
		{"CreateRunCache", "POST", "/runCache"},
		{"CreateRunGroup", "POST", "/runGroup"},
		{"CreateSequenceStore", "POST", "/sequencestore"},
		{"CreateShare", "POST", "/share"},
		{"CreateVariantStore", "POST", "/variantStore"},
		{"CreateWorkflow", "POST", "/workflow"},
		{"CreateWorkflowVersion", "POST", "/workflow/PLACEHOLDER/version"},
		{"DeleteAnnotationStore", "DELETE", "/annotationStore/PLACEHOLDER"},
		{"DeleteAnnotationStoreVersions", "POST", "/annotationStore/PLACEHOLDER/versions/delete"},
		{"DeleteBatch", "DELETE", "/runBatch/PLACEHOLDER"},
		{"DeleteConfiguration", "DELETE", "/configuration/PLACEHOLDER"},
		{"DeleteReference", "DELETE", "/referencestore/PLACEHOLDER/reference/PLACEHOLDER"},
		{"DeleteReferenceStore", "DELETE", "/referencestore/PLACEHOLDER"},
		{"DeleteRun", "DELETE", "/run/PLACEHOLDER"},
		{"DeleteRunBatch", "POST", "/runBatch/delete"},
		{"DeleteRunCache", "DELETE", "/runCache/PLACEHOLDER"},
		{"DeleteRunGroup", "DELETE", "/runGroup/PLACEHOLDER"},
		{"DeleteS3AccessPolicy", "DELETE", "/s3accesspolicy/PLACEHOLDER"},
		{"DeleteSequenceStore", "DELETE", "/sequencestore/PLACEHOLDER"},
		{"DeleteShare", "DELETE", "/share/PLACEHOLDER"},
		{"DeleteVariantStore", "DELETE", "/variantStore/PLACEHOLDER"},
		{"DeleteWorkflow", "DELETE", "/workflow/PLACEHOLDER"},
		{"DeleteWorkflowVersion", "DELETE", "/workflow/PLACEHOLDER/version/PLACEHOLDER"},
		{"GetAnnotationImportJob", "GET", "/import/annotation/PLACEHOLDER"},
		{"GetAnnotationStore", "GET", "/annotationStore/PLACEHOLDER"},
		{"GetAnnotationStoreVersion", "GET", "/annotationStore/PLACEHOLDER/version/PLACEHOLDER"},
		{"GetBatch", "GET", "/runBatch/PLACEHOLDER"},
		{"GetConfiguration", "GET", "/configuration/PLACEHOLDER"},
		{"GetReadSet", "GET", "/sequencestore/PLACEHOLDER/readset/PLACEHOLDER"},
		{"GetReadSetActivationJob", "GET", "/sequencestore/PLACEHOLDER/activationjob/PLACEHOLDER"},
		{"GetReadSetExportJob", "GET", "/sequencestore/PLACEHOLDER/exportjob/PLACEHOLDER"},
		{"GetReadSetImportJob", "GET", "/sequencestore/PLACEHOLDER/importjob/PLACEHOLDER"},
		{"GetReadSetMetadata", "GET", "/sequencestore/PLACEHOLDER/readset/PLACEHOLDER/metadata"},
		{"GetReference", "GET", "/referencestore/PLACEHOLDER/reference/PLACEHOLDER"},
		{"GetReferenceImportJob", "GET", "/referencestore/PLACEHOLDER/importjob/PLACEHOLDER"},
		{"GetReferenceMetadata", "GET", "/referencestore/PLACEHOLDER/reference/PLACEHOLDER/metadata"},
		{"GetReferenceStore", "GET", "/referencestore/PLACEHOLDER"},
		{"GetRun", "GET", "/run/PLACEHOLDER"},
		{"GetRunCache", "GET", "/runCache/PLACEHOLDER"},
		{"GetRunGroup", "GET", "/runGroup/PLACEHOLDER"},
		{"GetRunTask", "GET", "/run/PLACEHOLDER/task/PLACEHOLDER"},
		{"GetS3AccessPolicy", "GET", "/s3accesspolicy/PLACEHOLDER"},
		{"GetSequenceStore", "GET", "/sequencestore/PLACEHOLDER"},
		{"GetShare", "GET", "/share/PLACEHOLDER"},
		{"GetVariantImportJob", "GET", "/import/variant/PLACEHOLDER"},
		{"GetVariantStore", "GET", "/variantStore/PLACEHOLDER"},
		{"GetWorkflow", "GET", "/workflow/PLACEHOLDER"},
		{"GetWorkflowVersion", "GET", "/workflow/PLACEHOLDER/version/PLACEHOLDER"},
		{"ListAnnotationImportJobs", "POST", "/import/annotations"},
		{"ListAnnotationStoreVersions", "POST", "/annotationStore/PLACEHOLDER/versions"},
		{"ListAnnotationStores", "POST", "/annotationStores"},
		{"ListBatch", "GET", "/runBatch"},
		{"ListConfigurations", "GET", "/configuration"},
		{"ListMultipartReadSetUploads", "POST", "/sequencestore/PLACEHOLDER/uploads"},
		{"ListReadSetActivationJobs", "POST", "/sequencestore/PLACEHOLDER/activationjobs"},
		{"ListReadSetExportJobs", "POST", "/sequencestore/PLACEHOLDER/exportjobs"},
		{"ListReadSetImportJobs", "POST", "/sequencestore/PLACEHOLDER/importjobs"},
		{"ListReadSetUploadParts", "POST", "/sequencestore/PLACEHOLDER/upload/PLACEHOLDER/parts"},
		{"ListReadSets", "POST", "/sequencestore/PLACEHOLDER/readsets"},
		{"ListReferenceImportJobs", "POST", "/referencestore/PLACEHOLDER/importjobs"},
		{"ListReferenceStores", "POST", "/referencestores"},
		{"ListReferences", "POST", "/referencestore/PLACEHOLDER/references"},
		{"ListRunCaches", "GET", "/runCache"},
		{"ListRunGroups", "GET", "/runGroup"},
		{"ListRunTasks", "GET", "/run/PLACEHOLDER/task"},
		{"ListRuns", "GET", "/run"},
		{"ListRunsInBatch", "GET", "/runBatch/PLACEHOLDER/run"},
		{"ListSequenceStores", "POST", "/sequencestores"},
		{"ListShares", "POST", "/shares"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListVariantImportJobs", "POST", "/import/variants"},
		{"ListVariantStores", "POST", "/variantStores"},
		{"ListWorkflowVersions", "GET", "/workflow/PLACEHOLDER/version"},
		{"ListWorkflows", "GET", "/workflow"},
		{"PutS3AccessPolicy", "PUT", "/s3accesspolicy/PLACEHOLDER"},
		{"StartAnnotationImportJob", "POST", "/import/annotation"},
		{"StartReadSetActivationJob", "POST", "/sequencestore/PLACEHOLDER/activationjob"},
		{"StartReadSetExportJob", "POST", "/sequencestore/PLACEHOLDER/exportjob"},
		{"StartReadSetImportJob", "POST", "/sequencestore/PLACEHOLDER/importjob"},
		{"StartReferenceImportJob", "POST", "/referencestore/PLACEHOLDER/importjob"},
		{"StartRun", "POST", "/run"},
		{"StartRunBatch", "POST", "/runBatch"},
		{"StartVariantImportJob", "POST", "/import/variant"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAnnotationStore", "POST", "/annotationStore/PLACEHOLDER"},
		{"UpdateAnnotationStoreVersion", "POST", "/annotationStore/PLACEHOLDER/version/PLACEHOLDER"},
		{"UpdateRunCache", "POST", "/runCache/PLACEHOLDER"},
		{"UpdateRunGroup", "POST", "/runGroup/PLACEHOLDER"},
		{"UpdateSequenceStore", "PATCH", "/sequencestore/PLACEHOLDER"},
		{"UpdateVariantStore", "POST", "/variantStore/PLACEHOLDER"},
		{"UpdateWorkflow", "POST", "/workflow/PLACEHOLDER"},
		{"UpdateWorkflowVersion", "POST", "/workflow/PLACEHOLDER/version/PLACEHOLDER"},
		{"UploadReadSetPart", "PUT", "/sequencestore/PLACEHOLDER/upload/PLACEHOLDER/part"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real HealthOmics op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 2: re-derived all 107 omics ops from the pinned SDK and found the existing
// classifyPath table already correct -- no bugs, unlike this audit's earlier
// opensearch/lambda/route53/backup findings.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "operation not implemented" NotImplementedException
// that handleREST's map-lookup miss emits (handler.go:704-711) -- guarding
// against an operation name that resolves correctly but has no entry in
// opDispatch (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "operation not implemented",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
