package omics

import (
	"net/http"
	"strings"
)

// ────────────────────────────────────────────────────────────────────────────
// classifyPath maps (method, path) → operation name
// ────────────────────────────────────────────────────────────────────────────

func classifyPath(method, path string) string { //nolint:cyclop // large routing table
	// Tags
	if strings.HasPrefix(path, "/tags/") {
		switch method {
		case http.MethodPost:
			return opTagResource
		case http.MethodDelete:
			return opUntagResource
		case http.MethodGet:
			return opListTagsForResource
		}

		return opUnknown
	}

	// S3 access policy
	if strings.HasPrefix(path, "/s3accesspolicy/") {
		switch method {
		case http.MethodPut:
			return opPutS3AccessPolicy
		case http.MethodGet:
			return opGetS3AccessPolicy
		case http.MethodDelete:
			return opDeleteS3AccessPolicy
		}

		return opUnknown
	}

	switch method {
	case http.MethodPost:
		return classifyPOST(path)
	case http.MethodGet:
		return classifyGET(path)
	case http.MethodDelete:
		return classifyDELETE(path)
	case http.MethodPatch:
		if matchPattern(path, "/sequencestore/", "") {
			return opUpdateSequenceStore
		}

		return opUnknown
	case http.MethodPut:
		if strings.HasPrefix(path, "/sequencestore/") && strings.Contains(path, "/upload/") &&
			strings.HasSuffix(path, "/part") {
			return opUploadReadSetPart
		}

		return opUnknown
	}

	return opUnknown
}

func classifyPOST(path string) string { //nolint:cyclop,funlen,gocognit,gocyclo // large routing table
	// Static paths first
	switch path {
	case "/referencestore":
		return opCreateReferenceStore
	case "/referencestores":
		return opListReferenceStores
	case "/sequencestore":
		return opCreateSequenceStore
	case "/sequencestores":
		return opListSequenceStores
	case pathRunGroup:
		return opCreateRunGroup
	case pathRun:
		return opStartRun
	case pathRunCache:
		return opCreateRunCache
	case pathRunBatch:
		return opStartRunBatch
	case pathRunBatch + "/cancel":
		return opCancelRunBatch
	case pathRunBatch + "/delete":
		// Real AWS: POST /runBatch/delete is DeleteRunBatch (deletes the runs
		// within a batch, single batchId in body); DELETE /runBatch/{id} is
		// DeleteBatch (deletes the batch resource itself, id in the URI).
		return opDeleteRunBatch
	case pathWorkflow:
		return opCreateWorkflow
	case "/annotationStore":
		return opCreateAnnotationStore
	case "/annotationStores":
		return opListAnnotationStores
	case "/variantStore":
		return opCreateVariantStore
	case "/variantStores":
		return opListVariantStores
	case "/share":
		return opCreateShare
	case "/shares":
		return opListShares
	case "/import/annotation":
		return opStartAnnotationImportJob
	case "/import/annotations":
		return opListAnnotationImportJobs
	case "/import/variant":
		return opStartVariantImportJob
	case "/import/variants":
		return opListVariantImportJobs
	case pathConfiguration:
		return opCreateConfiguration
	}

	// /runGroup/{id} → UpdateRunGroup
	if matchPattern(path, pathRunGroup+"/", "") {
		return opUpdateRunGroup
	}

	// /run/{id}/cancel → CancelRun
	if strings.HasSuffix(path, "/cancel") && strings.HasPrefix(path, pathRun+"/") {
		return opCancelRun
	}

	// /runCache/{id} → UpdateRunCache
	if matchPattern(path, pathRunCache+"/", "") {
		return opUpdateRunCache
	}

	// /workflow/{id} → UpdateWorkflow (POST with id but no subpath)
	if matchPattern(path, pathWorkflow+"/", "") &&
		!strings.Contains(path[len(pathWorkflow+"/"):], "/") {
		return opUpdateWorkflow
	}

	// /workflow/{workflowId}/version → CreateWorkflowVersion
	if strings.HasPrefix(path, pathWorkflow+"/") && strings.HasSuffix(path, "/version") {
		return opCreateWorkflowVersion
	}

	// /workflow/{workflowId}/version/{versionName} → UpdateWorkflowVersion
	if strings.HasPrefix(path, pathWorkflow+"/") && strings.Contains(path, "/version/") {
		return opUpdateWorkflowVersion
	}

	// /annotationStore/{name} → UpdateAnnotationStore (POST with name, no subpath)
	if strings.HasPrefix(path, "/annotationStore/") {
		rest := path[len("/annotationStore/"):]

		switch {
		case !strings.Contains(rest, "/"):
			return opUpdateAnnotationStore
		case strings.HasSuffix(rest, "/versions/delete"):
			return opDeleteAnnotationStoreVersions
		case strings.HasSuffix(rest, "/versions"):
			return opListAnnotationStoreVersions
		case strings.HasSuffix(rest, "/version"):
			return opCreateAnnotationStoreVersion
		case strings.Contains(rest, "/version/"):
			return opUpdateAnnotationStoreVersion
		}
	}

	// /variantStore/{name} → UpdateVariantStore
	if matchPattern(path, "/variantStore/", "") {
		return opUpdateVariantStore
	}

	// /share/{shareId} → AcceptShare
	if matchPattern(path, "/share/", "") {
		return opAcceptShare
	}

	// /import/annotation/{jobId} — DELETE is CancelAnnotationImportJob handled elsewhere.
	// /import/variant/{jobId}

	// /referencestore/{id}/importjob → StartReferenceImportJob
	if strings.HasPrefix(path, "/referencestore/") {
		rest := path[len("/referencestore/"):]

		switch {
		case strings.HasSuffix(rest, "/importjob"):
			return opStartReferenceImportJob
		case strings.HasSuffix(rest, "/importjobs"):
			return opListReferenceImportJobs
		case strings.HasSuffix(rest, "/references"):
			return opListReferences
		}
	}

	// /sequencestore/{id}/... paths
	if strings.HasPrefix(path, "/sequencestore/") {
		rest := path[len("/sequencestore/"):]

		switch {
		case strings.HasSuffix(rest, "/readset/batch/delete"):
			return opBatchDeleteReadSet
		case strings.HasSuffix(rest, "/readsets"):
			return opListReadSets
		case strings.HasSuffix(rest, "/activationjob"):
			return opStartReadSetActivationJob
		case strings.HasSuffix(rest, "/activationjobs"):
			return opListReadSetActivationJobs
		case strings.HasSuffix(rest, "/exportjob"):
			return opStartReadSetExportJob
		case strings.HasSuffix(rest, "/exportjobs"):
			return opListReadSetExportJobs
		case strings.HasSuffix(rest, "/importjob"):
			return opStartReadSetImportJob
		case strings.HasSuffix(rest, "/importjobs"):
			return opListReadSetImportJobs
		case strings.HasSuffix(rest, "/upload"):
			return opCreateMultipartReadSetUpload
		case strings.HasSuffix(rest, "/uploads"):
			return opListMultipartReadSetUploads
		case strings.Contains(rest, "/upload/") && strings.HasSuffix(rest, "/complete"):
			return opCompleteMultipartReadSetUpload
		case strings.Contains(rest, "/upload/") && strings.HasSuffix(rest, "/parts"):
			return opListReadSetUploadParts
		}
	}

	return opUnknown
}

func classifyGET(path string) string { //nolint:cyclop,funlen,gocognit,gocyclo // large routing table
	switch path {
	case pathRunGroup:
		return opListRunGroups
	case pathRun:
		return opListRuns
	case pathRunCache:
		return opListRunCaches
	case pathRunBatch:
		return opListRunBatches
	case pathWorkflow:
		return opListWorkflows
	case pathConfiguration:
		return opListConfigurations
	}

	// /runBatch/{batchId}/run — real AWS ListRunsInBatch is GET, not POST.
	if strings.HasPrefix(path, pathRunBatch+"/") && strings.HasSuffix(path, "/run") {
		return opListRunsInBatch
	}

	// /runBatch/{batchId}
	if strings.HasPrefix(path, pathRunBatch+"/") {
		rest := path[len(pathRunBatch+"/"):]

		if !strings.Contains(rest, "/") {
			return opGetRunBatch
		}
	}

	// /runGroup/{id}
	if matchPattern(path, pathRunGroup+"/", "") {
		return opGetRunGroup
	}

	// /runCache/{id}
	if matchPattern(path, pathRunCache+"/", "") {
		return opGetRunCache
	}

	// /run/{id}/task/{taskId}
	if strings.HasPrefix(path, pathRun+"/") && strings.Contains(path, "/task/") {
		return opGetRunTask
	}

	// /run/{id}/task
	if strings.HasPrefix(path, pathRun+"/") && strings.HasSuffix(path, "/task") {
		return opListRunTasks
	}

	// /run/{id}
	if matchPattern(path, pathRun+"/", "") {
		return opGetRun
	}

	// /workflow/{workflowId}/version/{versionName}
	if strings.HasPrefix(path, pathWorkflow+"/") && strings.Contains(path, "/version/") {
		return opGetWorkflowVersion
	}

	// /workflow/{workflowId}/version
	if strings.HasPrefix(path, pathWorkflow+"/") && strings.HasSuffix(path, "/version") {
		return opListWorkflowVersions
	}

	// /workflow/{id}
	if matchPattern(path, pathWorkflow+"/", "") {
		return opGetWorkflow
	}

	// /import/annotation/{jobId}
	if strings.HasPrefix(path, "/import/annotation/") {
		return opGetAnnotationImportJob
	}

	// /annotationStore/{name}/version/{versionName}
	if strings.HasPrefix(path, "/annotationStore/") && strings.Contains(path, "/version/") {
		return opGetAnnotationStoreVersion
	}

	// /annotationStore/{name}
	if matchPattern(path, "/annotationStore/", "") {
		return opGetAnnotationStore
	}

	// /import/variant/{jobId}
	if strings.HasPrefix(path, "/import/variant/") {
		return opGetVariantImportJob
	}

	// /variantStore/{name}
	if matchPattern(path, "/variantStore/", "") {
		return opGetVariantStore
	}

	// /share/{shareId}
	if matchPattern(path, "/share/", "") {
		return opGetShare
	}

	// /referencestore/{referenceStoreId}/reference/{id}/metadata
	if strings.HasPrefix(path, "/referencestore/") && strings.Contains(path, "/reference/") &&
		strings.HasSuffix(path, "/metadata") {
		return opGetReferenceMetadata
	}

	// /referencestore/{referenceStoreId}/reference/{id}
	if strings.HasPrefix(path, "/referencestore/") && strings.Contains(path, "/reference/") {
		return opGetReference
	}

	// /referencestore/{referenceStoreId}/importjob/{id}
	if strings.HasPrefix(path, "/referencestore/") && strings.Contains(path, "/importjob/") {
		return opGetReferenceImportJob
	}

	// /referencestore/{id}
	if matchPattern(path, "/referencestore/", "") {
		return opGetReferenceStore
	}

	// /sequencestore/{sequenceStoreId}/readset/{id}/metadata
	if strings.HasPrefix(path, "/sequencestore/") && strings.Contains(path, "/readset/") &&
		strings.HasSuffix(path, "/metadata") {
		return opGetReadSetMetadata
	}

	// /sequencestore/{sequenceStoreId}/readset/{id}
	if strings.HasPrefix(path, "/sequencestore/") && strings.Contains(path, "/readset/") {
		return opGetReadSet
	}

	// /sequencestore/{sequenceStoreId}/activationjob/{id}
	if strings.HasPrefix(path, "/sequencestore/") && strings.Contains(path, "/activationjob/") {
		return opGetReadSetActivationJob
	}

	// /sequencestore/{sequenceStoreId}/exportjob/{id}
	if strings.HasPrefix(path, "/sequencestore/") && strings.Contains(path, "/exportjob/") {
		return opGetReadSetExportJob
	}

	// /sequencestore/{sequenceStoreId}/importjob/{id}
	if strings.HasPrefix(path, "/sequencestore/") && strings.Contains(path, "/importjob/") {
		return opGetReadSetImportJob
	}

	// /sequencestore/{id}
	if matchPattern(path, "/sequencestore/", "") {
		return opGetSequenceStore
	}

	// /configuration/{name}
	if matchPattern(path, "/configuration/", "") {
		return opGetConfiguration
	}

	return opUnknown
}

func classifyDELETE(path string) string { //nolint:cyclop // large routing table
	switch {
	// /referencestore/{referenceStoreId}/reference/{id}
	case strings.HasPrefix(path, "/referencestore/") && strings.Contains(path, "/reference/"):
		return opDeleteReference
	// /referencestore/{id}
	case matchPattern(path, "/referencestore/", ""):
		return opDeleteReferenceStore
	// /sequencestore/{sequenceStoreId}/upload/{uploadId}/abort
	case strings.HasPrefix(path, "/sequencestore/") &&
		strings.Contains(path, "/upload/") &&
		strings.HasSuffix(path, "/abort"):
		return opAbortMultipartReadSetUpload
	// /sequencestore/{id}
	case matchPattern(path, "/sequencestore/", ""):
		return opDeleteSequenceStore
	// /runGroup/{id}
	case matchPattern(path, pathRunGroup+"/", ""):
		return opDeleteRunGroup
	// /run/{id}
	case matchPattern(path, pathRun+"/", ""):
		return opDeleteRun
	// /runCache/{id}
	case matchPattern(path, pathRunCache+"/", ""):
		return opDeleteRunCache
	// /runBatch/{batchId} — real AWS DeleteBatch (see classifyPOST for the
	// DeleteRunBatch/DeleteBatch wire-shape note).
	case matchPattern(path, pathRunBatch+"/", ""):
		return opDeleteBatch
	// /workflow/{workflowId}/version/{versionName}
	case strings.HasPrefix(path, pathWorkflow+"/") && strings.Contains(path, "/version/"):
		return opDeleteWorkflowVersion
	// /workflow/{id}
	case matchPattern(path, pathWorkflow+"/", ""):
		return opDeleteWorkflow
	// /annotationStore/{name}
	case matchPattern(path, "/annotationStore/", ""):
		return opDeleteAnnotationStore
	// /variantStore/{name}
	case matchPattern(path, "/variantStore/", ""):
		return opDeleteVariantStore
	// /share/{shareId}
	case matchPattern(path, "/share/", ""):
		return opDeleteShare
	// /import/annotation/{jobId}
	case strings.HasPrefix(path, "/import/annotation/"):
		return opCancelAnnotationImportJob
	// /import/variant/{jobId}
	case strings.HasPrefix(path, "/import/variant/"):
		return opCancelVariantImportJob
	// /configuration/{name}
	case matchPattern(path, pathConfiguration+"/", ""):
		return opDeleteConfiguration
	}

	return opUnknown
}

// matchPattern returns true when path starts with prefix and has no further "/" after the ID.
func matchPattern(path, prefix, _ string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}

	rest := path[len(prefix):]

	return rest != "" && !strings.Contains(rest, "/")
}

// extractID returns the segment after the prefix (up to next slash or end).
func extractID(path, prefix string) string {
	rest, found := strings.CutPrefix(path, prefix)

	if !found {
		return ""
	}

	if before, _, hasSep := strings.Cut(rest, "/"); hasSep {
		return before
	}

	return rest
}

// extractTwoIDs returns the two IDs from a path of the form /prefix/{id1}/segment/{id2}.
func extractTwoIDs(path, prefix, segment string) (string, string) {
	rest, found := strings.CutPrefix(path, prefix)

	if !found {
		return "", ""
	}

	id1, after, found := strings.Cut(rest, segment)

	if !found {
		return rest, ""
	}

	id2, _, _ := strings.Cut(after, "/")

	return id1, id2
}

// extractRefMetadataIDs parses /referencestore/{storeId}/reference/{refId}/metadata.
func extractRefMetadataIDs(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, "/referencestore/")
	storeID, after, _ := strings.Cut(rest, "/reference/")
	refID, _, _ := strings.Cut(after, "/metadata")

	return storeID, refID
}

// extractReadSetMetadataIDs parses /sequencestore/{storeId}/readset/{rsId}/metadata.
func extractReadSetMetadataIDs(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, "/sequencestore/")
	storeID, after, _ := strings.Cut(rest, "/readset/")
	rsID, _, _ := strings.Cut(after, "/metadata")

	return storeID, rsID
}

// extractUploadIDs parses /sequencestore/{storeId}/upload/{uploadId}/....
func extractUploadIDs(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, "/sequencestore/")
	storeID, after, _ := strings.Cut(rest, "/upload/")
	uploadID, _, _ := strings.Cut(after, "/")

	return storeID, uploadID
}
