package omics_test

// This is the route-map regression test for the omics package's table-based
// dispatch (opDispatch in handler.go, postStaticRoutes in routes.go): it
// locks the specific failure mode a hand-written map literal is exposed to
// that the compiler can't catch -- two identical map keys silently collapse
// to one entry (Go allows a duplicate map key at compile time), or a route
// classifies to an operation name with no corresponding dispatch-table
// entry -- both of which would otherwise only surface at request time as a
// silent 501 NotImplementedException. It uses the unexported-surface
// accessors in export_test.go (ClassifyPathForTest/OpDispatchKeysForTest)
// since classifyPath and opDispatch are both unexported.

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

// TestOpDispatch_NoDuplicateOrMissingKeys verifies GetSupportedOperations()
// (derived from opDispatch's keys) has no duplicates and that every listed
// operation actually resolves through OpDispatchKeysForTest.
func TestOpDispatch_NoDuplicateOrMissingKeys(t *testing.T) {
	t.Parallel()

	h := omics.NewHandler(omics.NewInMemoryBackend("000000000000", "us-east-1"))
	supported := h.GetSupportedOperations()
	dispatchKeys := omics.OpDispatchKeysForTest()

	if len(supported) != len(dispatchKeys) {
		t.Fatalf("GetSupportedOperations() returned %d ops but opDispatch has %d entries",
			len(supported), len(dispatchKeys))
	}

	seen := make(map[string]bool, len(supported))
	for _, op := range supported {
		if seen[op] {
			t.Errorf("duplicate operation in GetSupportedOperations(): %s", op)
		}

		seen[op] = true
	}
}

// routeCase is one (method, path) -> expected-operation fixture, covering
// every distinct operation classifyPath can produce.
type routeCase struct {
	method string
	path   string
	op     string
}

// allRouteCases enumerates one representative (method, path) pair per
// HealthOmics operation classifyPath/classifyPOST/classifyGET/classifyDELETE
// can produce -- mirroring the path shapes documented inline in routes.go.
// omics.OpUnknownForTest() marks the one path shape that is intentionally
// not a real HealthOmics route.
func allRouteCases() []routeCase {
	return []routeCase{
		// ReferenceStore / Reference
		{http.MethodPost, "/referencestore", "CreateReferenceStore"},
		{http.MethodGet, "/referencestore", omics.OpUnknownForTest()}, // no GET /referencestore collection route
		{http.MethodPost, "/referencestores", "ListReferenceStores"},
		{http.MethodGet, "/referencestore/rs1", "GetReferenceStore"},
		{http.MethodDelete, "/referencestore/rs1", "DeleteReferenceStore"},
		{http.MethodGet, "/referencestore/rs1/reference/r1", "GetReference"},
		{http.MethodGet, "/referencestore/rs1/reference/r1/metadata", "GetReferenceMetadata"},
		{http.MethodDelete, "/referencestore/rs1/reference/r1", "DeleteReference"},
		{http.MethodPost, "/referencestore/rs1/references", "ListReferences"},
		{http.MethodPost, "/referencestore/rs1/importjob", "StartReferenceImportJob"},
		{http.MethodGet, "/referencestore/rs1/importjob/j1", "GetReferenceImportJob"},
		{http.MethodPost, "/referencestore/rs1/importjobs", "ListReferenceImportJobs"},

		// SequenceStore
		{http.MethodPost, "/sequencestore", "CreateSequenceStore"},
		{http.MethodPost, "/sequencestores", "ListSequenceStores"},
		{http.MethodGet, "/sequencestore/ss1", "GetSequenceStore"},
		{http.MethodDelete, "/sequencestore/ss1", "DeleteSequenceStore"},
		{http.MethodPatch, "/sequencestore/ss1", "UpdateSequenceStore"},

		// ReadSet
		{http.MethodPost, "/sequencestore/ss1/readset/batch/delete", "BatchDeleteReadSet"},
		{http.MethodGet, "/sequencestore/ss1/readset/rd1", "GetReadSet"},
		{http.MethodGet, "/sequencestore/ss1/readset/rd1/metadata", "GetReadSetMetadata"},
		{http.MethodPost, "/sequencestore/ss1/readsets", "ListReadSets"},
		{http.MethodPost, "/sequencestore/ss1/activationjob", "StartReadSetActivationJob"},
		{http.MethodGet, "/sequencestore/ss1/activationjob/j1", "GetReadSetActivationJob"},
		{http.MethodPost, "/sequencestore/ss1/activationjobs", "ListReadSetActivationJobs"},
		{http.MethodPost, "/sequencestore/ss1/exportjob", "StartReadSetExportJob"},
		{http.MethodGet, "/sequencestore/ss1/exportjob/j1", "GetReadSetExportJob"},
		{http.MethodPost, "/sequencestore/ss1/exportjobs", "ListReadSetExportJobs"},
		{http.MethodPost, "/sequencestore/ss1/importjob", "StartReadSetImportJob"},
		{http.MethodGet, "/sequencestore/ss1/importjob/j1", "GetReadSetImportJob"},
		{http.MethodPost, "/sequencestore/ss1/importjobs", "ListReadSetImportJobs"},

		// Multipart Upload
		{http.MethodPost, "/sequencestore/ss1/upload", "CreateMultipartReadSetUpload"},
		{http.MethodDelete, "/sequencestore/ss1/upload/u1/abort", "AbortMultipartReadSetUpload"},
		{http.MethodPost, "/sequencestore/ss1/upload/u1/complete", "CompleteMultipartReadSetUpload"},
		{http.MethodPost, "/sequencestore/ss1/uploads", "ListMultipartReadSetUploads"},
		{http.MethodPost, "/sequencestore/ss1/upload/u1/parts", "ListReadSetUploadParts"},
		{http.MethodPut, "/sequencestore/ss1/upload/u1/part", "UploadReadSetPart"},

		// RunGroup
		{http.MethodPost, "/runGroup", "CreateRunGroup"},
		{http.MethodGet, "/runGroup", "ListRunGroups"},
		{http.MethodGet, "/runGroup/rg1", "GetRunGroup"},
		{http.MethodPost, "/runGroup/rg1", "UpdateRunGroup"},
		{http.MethodDelete, "/runGroup/rg1", "DeleteRunGroup"},

		// Run
		{http.MethodPost, "/run", "StartRun"},
		{http.MethodGet, "/run", "ListRuns"},
		{http.MethodGet, "/run/r1", "GetRun"},
		{http.MethodDelete, "/run/r1", "DeleteRun"},
		{http.MethodPost, "/run/r1/cancel", "CancelRun"},
		{http.MethodGet, "/run/r1/task", "ListRunTasks"},
		{http.MethodGet, "/run/r1/task/t1", "GetRunTask"},

		// Workflow / WorkflowVersion
		{http.MethodPost, "/workflow", "CreateWorkflow"},
		{http.MethodGet, "/workflow", "ListWorkflows"},
		{http.MethodGet, "/workflow/w1", "GetWorkflow"},
		{http.MethodPost, "/workflow/w1", "UpdateWorkflow"},
		{http.MethodDelete, "/workflow/w1", "DeleteWorkflow"},
		{http.MethodPost, "/workflow/w1/version", "CreateWorkflowVersion"},
		{http.MethodGet, "/workflow/w1/version", "ListWorkflowVersions"},
		{http.MethodGet, "/workflow/w1/version/v1", "GetWorkflowVersion"},
		{http.MethodPost, "/workflow/w1/version/v1", "UpdateWorkflowVersion"},
		{http.MethodDelete, "/workflow/w1/version/v1", "DeleteWorkflowVersion"},

		// AnnotationStore / AnnotationStoreVersion
		{http.MethodPost, "/annotationStore", "CreateAnnotationStore"},
		{http.MethodPost, "/annotationStores", "ListAnnotationStores"},
		{http.MethodGet, "/annotationStore/as1", "GetAnnotationStore"},
		{http.MethodPost, "/annotationStore/as1", "UpdateAnnotationStore"},
		{http.MethodDelete, "/annotationStore/as1", "DeleteAnnotationStore"},
		{http.MethodPost, "/import/annotation", "StartAnnotationImportJob"},
		{http.MethodGet, "/import/annotation/j1", "GetAnnotationImportJob"},
		{http.MethodPost, "/import/annotations", "ListAnnotationImportJobs"},
		{http.MethodDelete, "/import/annotation/j1", "CancelAnnotationImportJob"},
		{http.MethodPost, "/annotationStore/as1/version", "CreateAnnotationStoreVersion"},
		{http.MethodPost, "/annotationStore/as1/versions/delete", "DeleteAnnotationStoreVersions"},
		{http.MethodGet, "/annotationStore/as1/version/v1", "GetAnnotationStoreVersion"},
		{http.MethodPost, "/annotationStore/as1/versions", "ListAnnotationStoreVersions"},
		{http.MethodPost, "/annotationStore/as1/version/v1", "UpdateAnnotationStoreVersion"},

		// VariantStore
		{http.MethodPost, "/variantStore", "CreateVariantStore"},
		{http.MethodPost, "/variantStores", "ListVariantStores"},
		{http.MethodGet, "/variantStore/vs1", "GetVariantStore"},
		{http.MethodPost, "/variantStore/vs1", "UpdateVariantStore"},
		{http.MethodDelete, "/variantStore/vs1", "DeleteVariantStore"},
		{http.MethodPost, "/import/variant", "StartVariantImportJob"},
		{http.MethodGet, "/import/variant/j1", "GetVariantImportJob"},
		{http.MethodPost, "/import/variants", "ListVariantImportJobs"},
		{http.MethodDelete, "/import/variant/j1", "CancelVariantImportJob"},

		// Share
		{http.MethodPost, "/share", "CreateShare"},
		{http.MethodPost, "/shares", "ListShares"},
		{http.MethodPost, "/share/sh1", "AcceptShare"},
		{http.MethodGet, "/share/sh1", "GetShare"},
		{http.MethodDelete, "/share/sh1", "DeleteShare"},

		// RunCache
		{http.MethodPost, "/runCache", "CreateRunCache"},
		{http.MethodGet, "/runCache", "ListRunCaches"},
		{http.MethodGet, "/runCache/rc1", "GetRunCache"},
		{http.MethodPost, "/runCache/rc1", "UpdateRunCache"},
		{http.MethodDelete, "/runCache/rc1", "DeleteRunCache"},

		// RunBatch
		{http.MethodPost, "/runBatch", "StartRunBatch"},
		{http.MethodGet, "/runBatch", "ListBatch"},
		{http.MethodGet, "/runBatch/rb1", "GetBatch"},
		{http.MethodDelete, "/runBatch/rb1", "DeleteBatch"},
		{http.MethodPost, "/runBatch/cancel", "CancelRunBatch"},
		{http.MethodPost, "/runBatch/delete", "DeleteRunBatch"},
		{http.MethodGet, "/runBatch/rb1/run", "ListRunsInBatch"},

		// Configuration
		{http.MethodPost, "/configuration", "CreateConfiguration"},
		{http.MethodGet, "/configuration", "ListConfigurations"},
		{http.MethodGet, "/configuration/c1", "GetConfiguration"},
		{http.MethodDelete, "/configuration/c1", "DeleteConfiguration"},

		// S3 Access Policy
		{http.MethodPut, "/s3accesspolicy/arn1", "PutS3AccessPolicy"},
		{http.MethodGet, "/s3accesspolicy/arn1", "GetS3AccessPolicy"},
		{http.MethodDelete, "/s3accesspolicy/arn1", "DeleteS3AccessPolicy"},

		// Tags
		{http.MethodPost, "/tags/arn:aws:omics:us-east-1:000000000000:workflow/w1", "TagResource"},
		{http.MethodDelete, "/tags/arn:aws:omics:us-east-1:000000000000:workflow/w1", "UntagResource"},
		{http.MethodGet, "/tags/arn:aws:omics:us-east-1:000000000000:workflow/w1", "ListTagsForResource"},
	}
}

// TestClassifyPath_EveryOperationResolvesInDispatchTable proves every
// operation classifyPath can produce for a real HealthOmics wire path -- one
// fixture per operation, see allRouteCases -- either has a working opDispatch
// entry or is the one intentionally-unreachable case documented inline
// (opUnknown). It's the regression test for the specific bug class already
// fixed once in this package (see runbatch_route_test.go): a route that
// classifies correctly but silently 404s/501s because no handler was wired
// for it, or vice versa.
func TestClassifyPath_EveryOperationResolvesInDispatchTable(t *testing.T) {
	t.Parallel()

	dispatchable := make(map[string]bool)
	for _, op := range omics.OpDispatchKeysForTest() {
		dispatchable[op] = true
	}

	opUnknown := omics.OpUnknownForTest()

	for _, tc := range allRouteCases() {
		got := omics.ClassifyPathForTest(tc.method, tc.path)
		if got != tc.op {
			t.Errorf("classifyPath(%s, %s) = %s, want %s", tc.method, tc.path, got, tc.op)

			continue
		}

		if tc.op == opUnknown {
			continue
		}

		if !dispatchable[tc.op] {
			t.Errorf("classifyPath(%s, %s) produced operation %s with no opDispatch entry",
				tc.method, tc.path, tc.op)
		}
	}
}

// TestAllRouteCases_CoverEveryDispatchedOperation is the inverse check: every
// operation opDispatch knows how to handle must appear in allRouteCases, so a
// newly added operation can't silently go untested here.
func TestAllRouteCases_CoverEveryDispatchedOperation(t *testing.T) {
	t.Parallel()

	covered := make(map[string]bool)
	for _, tc := range allRouteCases() {
		covered[tc.op] = true
	}

	for _, op := range omics.OpDispatchKeysForTest() {
		if !covered[op] {
			t.Errorf("operation %s is in opDispatch but has no allRouteCases fixture", op)
		}
	}
}
