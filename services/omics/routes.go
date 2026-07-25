package omics

import (
	"net/http"
	"strings"
	"sync"
)

// ────────────────────────────────────────────────────────────────────────────
// classifyPath maps (method, path) → operation name
// ────────────────────────────────────────────────────────────────────────────

// classifyPath is the entry point for routing. It first checks the two
// prefixes shared across HTTP methods (tags, S3 access policy), then
// dispatches by method to classifyPOST/classifyGET/classifyDELETE or the two
// single-pattern PATCH/PUT operations.
func classifyPath(method, path string) string {
	if op, ok := classifyFixedPrefixOp(method, path); ok {
		return op
	}

	switch method {
	case http.MethodPost:
		return classifyPOST(path)
	case http.MethodGet:
		return classifyGET(path)
	case http.MethodDelete:
		return classifyDELETE(path)
	case http.MethodPatch:
		return classifyPATCH(path)
	case http.MethodPut:
		return classifyPUT(path)
	}

	return opUnknown
}

// classifyFixedPrefixOp handles the two path families whose operation
// depends only on (method, prefix) and never on any further path shape:
// /tags/{arn} and /s3accesspolicy/{arn}. Returns ok=false if path matches
// neither prefix, so the caller falls through to the per-method classifiers.
func classifyFixedPrefixOp(method, path string) (string, bool) {
	if strings.HasPrefix(path, "/tags/") {
		switch method {
		case http.MethodPost:
			return opTagResource, true
		case http.MethodDelete:
			return opUntagResource, true
		case http.MethodGet:
			return opListTagsForResource, true
		}

		return opUnknown, true
	}

	if strings.HasPrefix(path, "/s3accesspolicy/") {
		switch method {
		case http.MethodPut:
			return opPutS3AccessPolicy, true
		case http.MethodGet:
			return opGetS3AccessPolicy, true
		case http.MethodDelete:
			return opDeleteS3AccessPolicy, true
		}

		return opUnknown, true
	}

	return "", false
}

// classifyPATCH handles the single PATCH route: UpdateSequenceStore.
func classifyPATCH(path string) string {
	if matchPattern(path, "/sequencestore/", "") {
		return opUpdateSequenceStore
	}

	return opUnknown
}

// classifyPUT handles the single PUT route: UploadReadSetPart.
func classifyPUT(path string) string {
	if strings.HasPrefix(path, "/sequencestore/") && strings.Contains(path, "/upload/") &&
		strings.HasSuffix(path, "/part") {
		return opUploadReadSetPart
	}

	return opUnknown
}

// ────────────────────────────────────────────────────────────────────────────
// POST routing
// ────────────────────────────────────────────────────────────────────────────

// pathClassifier checks one path-family and reports whether it matched.
type pathClassifier func(path string) (string, bool)

// classifyPOST dispatches through classifyPOSTStatic followed by each
// per-family classifier below, in order, returning the first match. The
// families each own a disjoint URL prefix (runGroup/run/runCache/workflow/
// annotationStore/variantStore/share/referencestore/sequencestore), so the
// order between families is immaterial -- only the checks *within* a family
// (handled inside that family's own function) are order-sensitive.
func classifyPOST(path string) string {
	if op, ok := classifyPOSTStatic(path); ok {
		return op
	}

	for _, classify := range []pathClassifier{
		classifyPOSTRunFamily,
		classifyPOSTWorkflowFamily,
		classifyPOSTAnnotationVariantShare,
		classifyPOSTReferenceStoreFamily,
		classifyPOSTSequenceStoreFamily,
	} {
		if op, ok := classify(path); ok {
			return op
		}
	}

	return opUnknown
}

// postStaticRoutes is the exact-match static POST path table (collection
// create/list endpoints and the fixed RunBatch action paths). A map lookup
// keeps this a flat, zero-branch table rather than a long switch.
//
//nolint:gochecknoglobals // read-only package-level lookup table (apigatewayv2 pattern)
var postStaticRoutes = sync.OnceValue(func() map[string]string {
	return map[string]string{
		pathReferenceStore:       opCreateReferenceStore,
		pathReferenceStores:      opListReferenceStores,
		pathSequenceStore:        opCreateSequenceStore,
		pathSequenceStores:       opListSequenceStores,
		pathRunGroup:             opCreateRunGroup,
		pathRun:                  opStartRun,
		pathRunCache:             opCreateRunCache,
		pathRunBatch:             opStartRunBatch,
		pathRunBatch + "/cancel": opCancelRunBatch,
		// Real AWS: POST /runBatch/delete is DeleteRunBatch (deletes the runs
		// within a batch, single batchId in body); DELETE /runBatch/{id} is
		// DeleteBatch (deletes the batch resource itself, id in the URI).
		pathRunBatch + "/delete": opDeleteRunBatch,
		pathWorkflow:             opCreateWorkflow,
		pathAnnotationStore:      opCreateAnnotationStore,
		pathAnnotationStores:     opListAnnotationStores,
		pathVariantStore:         opCreateVariantStore,
		pathVariantStores:        opListVariantStores,
		pathShare:                opCreateShare,
		pathShares:               opListShares,
		pathImportAnnotation:     opStartAnnotationImportJob,
		pathImportAnnotations:    opListAnnotationImportJobs,
		pathImportVariant:        opStartVariantImportJob,
		pathImportVariants:       opListVariantImportJobs,
		pathConfiguration:        opCreateConfiguration,
	}
})

// classifyPOSTStatic handles the exact-match static POST paths via
// postStaticRoutes.
func classifyPOSTStatic(path string) (string, bool) {
	op, ok := postStaticRoutes()[path]

	return op, ok
}

// classifyPOSTRunFamily handles /runGroup/{id}, /run/{id}/cancel, and
// /runCache/{id}.
func classifyPOSTRunFamily(path string) (string, bool) {
	if matchPattern(path, pathRunGroup+"/", "") {
		return opUpdateRunGroup, true
	}

	if strings.HasSuffix(path, "/cancel") && strings.HasPrefix(path, pathRun+"/") {
		return opCancelRun, true
	}

	if matchPattern(path, pathRunCache+"/", "") {
		return opUpdateRunCache, true
	}

	return "", false
}

// classifyPOSTWorkflowFamily handles /workflow/{id} (update),
// /workflow/{id}/version (create), and /workflow/{id}/version/{name} (update).
func classifyPOSTWorkflowFamily(path string) (string, bool) {
	if !strings.HasPrefix(path, pathWorkflow+"/") {
		return "", false
	}

	rest := path[len(pathWorkflow+"/"):]

	switch {
	case !strings.Contains(rest, "/"):
		return opUpdateWorkflow, true
	case strings.HasSuffix(rest, "/version"):
		return opCreateWorkflowVersion, true
	case strings.Contains(rest, "/version/"):
		return opUpdateWorkflowVersion, true
	}

	return "", false
}

// classifyPOSTAnnotationVariantShare handles /annotationStore/{name} (update
// and version sub-resources), /variantStore/{name} (update), and
// /share/{id} (accept).
func classifyPOSTAnnotationVariantShare(path string) (string, bool) {
	if strings.HasPrefix(path, "/annotationStore/") {
		rest := path[len("/annotationStore/"):]

		switch {
		case !strings.Contains(rest, "/"):
			return opUpdateAnnotationStore, true
		case strings.HasSuffix(rest, "/versions/delete"):
			return opDeleteAnnotationStoreVersions, true
		case strings.HasSuffix(rest, "/versions"):
			return opListAnnotationStoreVersions, true
		case strings.HasSuffix(rest, "/version"):
			return opCreateAnnotationStoreVersion, true
		case strings.Contains(rest, "/version/"):
			return opUpdateAnnotationStoreVersion, true
		}

		return "", false
	}

	if matchPattern(path, "/variantStore/", "") {
		return opUpdateVariantStore, true
	}

	if matchPattern(path, "/share/", "") {
		return opAcceptShare, true
	}

	return "", false
}

// classifyPOSTReferenceStoreFamily handles /referencestore/{id}/importjob(s)
// and /referencestore/{id}/references (ListReferences, POST-with-body).
func classifyPOSTReferenceStoreFamily(path string) (string, bool) {
	if !strings.HasPrefix(path, "/referencestore/") {
		return "", false
	}

	rest := path[len("/referencestore/"):]

	switch {
	case strings.HasSuffix(rest, "/importjob"):
		return opStartReferenceImportJob, true
	case strings.HasSuffix(rest, "/importjobs"):
		return opListReferenceImportJobs, true
	case strings.HasSuffix(rest, "/references"):
		return opListReferences, true
	}

	return "", false
}

// classifyPOSTSequenceStoreFamily handles the /sequencestore/{id}/... action
// and list sub-resources (read sets, activation/export/import jobs, and
// multipart upload lifecycle).
func classifyPOSTSequenceStoreFamily(path string) (string, bool) {
	if !strings.HasPrefix(path, "/sequencestore/") {
		return "", false
	}

	rest := path[len("/sequencestore/"):]

	if op, ok := classifyPOSTReadSetJobs(rest); ok {
		return op, true
	}

	return classifyPOSTMultipartUpload(rest)
}

// classifyPOSTReadSetJobs handles the batch-delete/activation/export/import
// job sub-paths under /sequencestore/{id}/... (rest is the path with the
// "/sequencestore/" prefix already stripped).
func classifyPOSTReadSetJobs(rest string) (string, bool) {
	switch {
	case strings.HasSuffix(rest, "/readset/batch/delete"):
		return opBatchDeleteReadSet, true
	case strings.HasSuffix(rest, "/readsets"):
		return opListReadSets, true
	case strings.HasSuffix(rest, "/activationjob"):
		return opStartReadSetActivationJob, true
	case strings.HasSuffix(rest, "/activationjobs"):
		return opListReadSetActivationJobs, true
	case strings.HasSuffix(rest, "/exportjob"):
		return opStartReadSetExportJob, true
	case strings.HasSuffix(rest, "/exportjobs"):
		return opListReadSetExportJobs, true
	case strings.HasSuffix(rest, "/importjob"):
		return opStartReadSetImportJob, true
	case strings.HasSuffix(rest, "/importjobs"):
		return opListReadSetImportJobs, true
	}

	return "", false
}

// classifyPOSTMultipartUpload handles the multipart-upload lifecycle
// sub-paths under /sequencestore/{id}/... (rest is the path with the
// "/sequencestore/" prefix already stripped).
func classifyPOSTMultipartUpload(rest string) (string, bool) {
	switch {
	case strings.HasSuffix(rest, "/upload"):
		return opCreateMultipartReadSetUpload, true
	case strings.HasSuffix(rest, "/uploads"):
		return opListMultipartReadSetUploads, true
	case strings.Contains(rest, "/upload/") && strings.HasSuffix(rest, "/complete"):
		return opCompleteMultipartReadSetUpload, true
	case strings.Contains(rest, "/upload/") && strings.HasSuffix(rest, "/parts"):
		return opListReadSetUploadParts, true
	}

	return "", false
}

// ────────────────────────────────────────────────────────────────────────────
// GET routing
// ────────────────────────────────────────────────────────────────────────────

// classifyGET dispatches through classifyGETStatic followed by each
// per-family classifier below, in order, returning the first match. As with
// classifyPOST, each family owns a disjoint URL prefix, so only the checks
// *within* a family are order-sensitive.
func classifyGET(path string) string {
	if op, ok := classifyGETStatic(path); ok {
		return op
	}

	for _, classify := range []pathClassifier{
		classifyGETRunBatch,
		classifyGETRunGroupOrCache,
		classifyGETRun,
		classifyGETWorkflow,
		classifyGETAnnotationOrVariantOrShare,
		classifyGETReferenceStoreFamily,
		classifyGETSequenceStoreFamily,
	} {
		if op, ok := classify(path); ok {
			return op
		}
	}

	if matchPattern(path, "/configuration/", "") {
		return opGetConfiguration
	}

	return opUnknown
}

// classifyGETStatic handles the exact-match collection List* paths.
func classifyGETStatic(path string) (string, bool) {
	switch path {
	case pathRunGroup:
		return opListRunGroups, true
	case pathRun:
		return opListRuns, true
	case pathRunCache:
		return opListRunCaches, true
	case pathRunBatch:
		return opListRunBatches, true
	case pathWorkflow:
		return opListWorkflows, true
	case pathConfiguration:
		return opListConfigurations, true
	}

	return "", false
}

// classifyGETRunBatch handles /runBatch/{id}/run (ListRunsInBatch, real AWS
// wire shape is GET not POST) and /runBatch/{id} (GetBatch).
func classifyGETRunBatch(path string) (string, bool) {
	if !strings.HasPrefix(path, pathRunBatch+"/") {
		return "", false
	}

	if strings.HasSuffix(path, "/run") {
		return opListRunsInBatch, true
	}

	rest := path[len(pathRunBatch+"/"):]
	if !strings.Contains(rest, "/") {
		return opGetRunBatch, true
	}

	return "", false
}

// classifyGETRunGroupOrCache handles /runGroup/{id} and /runCache/{id}.
func classifyGETRunGroupOrCache(path string) (string, bool) {
	if matchPattern(path, pathRunGroup+"/", "") {
		return opGetRunGroup, true
	}

	if matchPattern(path, pathRunCache+"/", "") {
		return opGetRunCache, true
	}

	return "", false
}

// classifyGETRun handles /run/{id}/task/{taskId}, /run/{id}/task, and
// /run/{id}.
func classifyGETRun(path string) (string, bool) {
	if !strings.HasPrefix(path, pathRun+"/") {
		return "", false
	}

	switch {
	case strings.Contains(path, "/task/"):
		return opGetRunTask, true
	case strings.HasSuffix(path, "/task"):
		return opListRunTasks, true
	}

	if matchPattern(path, pathRun+"/", "") {
		return opGetRun, true
	}

	return "", false
}

// classifyGETWorkflow handles /workflow/{id}/version/{name},
// /workflow/{id}/version, and /workflow/{id}.
func classifyGETWorkflow(path string) (string, bool) {
	if !strings.HasPrefix(path, pathWorkflow+"/") {
		return "", false
	}

	switch {
	case strings.Contains(path, "/version/"):
		return opGetWorkflowVersion, true
	case strings.HasSuffix(path, "/version"):
		return opListWorkflowVersions, true
	}

	if matchPattern(path, pathWorkflow+"/", "") {
		return opGetWorkflow, true
	}

	return "", false
}

// classifyGETAnnotationOrVariantOrShare handles /import/annotation/{id},
// /annotationStore/{name}(/version/{name}), /import/variant/{id},
// /variantStore/{name}, and /share/{id}.
func classifyGETAnnotationOrVariantOrShare(path string) (string, bool) {
	switch {
	case strings.HasPrefix(path, "/import/annotation/"):
		return opGetAnnotationImportJob, true
	case strings.HasPrefix(path, "/annotationStore/") && strings.Contains(path, "/version/"):
		return opGetAnnotationStoreVersion, true
	case matchPattern(path, "/annotationStore/", ""):
		return opGetAnnotationStore, true
	case strings.HasPrefix(path, "/import/variant/"):
		return opGetVariantImportJob, true
	case matchPattern(path, "/variantStore/", ""):
		return opGetVariantStore, true
	case matchPattern(path, "/share/", ""):
		return opGetShare, true
	}

	return "", false
}

// classifyGETReferenceStoreFamily handles /referencestore/{id}/reference/{id}
// (with an optional /metadata suffix), /referencestore/{id}/importjob/{id},
// and /referencestore/{id}.
func classifyGETReferenceStoreFamily(path string) (string, bool) {
	if !strings.HasPrefix(path, "/referencestore/") {
		return "", false
	}

	switch {
	case strings.Contains(path, "/reference/") && strings.HasSuffix(path, "/metadata"):
		return opGetReferenceMetadata, true
	case strings.Contains(path, "/reference/"):
		return opGetReference, true
	case strings.Contains(path, "/importjob/"):
		return opGetReferenceImportJob, true
	}

	if matchPattern(path, "/referencestore/", "") {
		return opGetReferenceStore, true
	}

	return "", false
}

// classifyGETSequenceStoreFamily handles /sequencestore/{id}/readset/{id}
// (with an optional /metadata suffix), the activation/export/import job
// sub-resources, and /sequencestore/{id}.
func classifyGETSequenceStoreFamily(path string) (string, bool) {
	if !strings.HasPrefix(path, "/sequencestore/") {
		return "", false
	}

	switch {
	case strings.Contains(path, "/readset/") && strings.HasSuffix(path, "/metadata"):
		return opGetReadSetMetadata, true
	case strings.Contains(path, "/readset/"):
		return opGetReadSet, true
	case strings.Contains(path, "/activationjob/"):
		return opGetReadSetActivationJob, true
	case strings.Contains(path, "/exportjob/"):
		return opGetReadSetExportJob, true
	case strings.Contains(path, "/importjob/"):
		return opGetReadSetImportJob, true
	}

	if matchPattern(path, "/sequencestore/", "") {
		return opGetSequenceStore, true
	}

	return "", false
}

// ────────────────────────────────────────────────────────────────────────────
// DELETE routing
// ────────────────────────────────────────────────────────────────────────────

// classifyDELETE dispatches through each per-family classifier below, in
// order, returning the first match. As with classifyPOST/classifyGET, each
// family owns a disjoint URL prefix, so only the checks *within* a family are
// order-sensitive.
func classifyDELETE(path string) string {
	for _, classify := range []pathClassifier{
		classifyDELETEReferenceOrSequenceStore,
		classifyDELETERunFamily,
		classifyDELETEWorkflowFamily,
		classifyDELETEStoreFamily,
		classifyDELETEImportJobFamily,
	} {
		if op, ok := classify(path); ok {
			return op
		}
	}

	if matchPattern(path, pathConfiguration+"/", "") {
		return opDeleteConfiguration
	}

	return opUnknown
}

// classifyDELETEReferenceOrSequenceStore handles
// /referencestore/{id}/reference/{id}, /referencestore/{id},
// /sequencestore/{id}/upload/{id}/abort, and /sequencestore/{id}.
func classifyDELETEReferenceOrSequenceStore(path string) (string, bool) {
	switch {
	case strings.HasPrefix(path, "/referencestore/") && strings.Contains(path, "/reference/"):
		return opDeleteReference, true
	case matchPattern(path, "/referencestore/", ""):
		return opDeleteReferenceStore, true
	case strings.HasPrefix(path, "/sequencestore/") &&
		strings.Contains(path, "/upload/") &&
		strings.HasSuffix(path, "/abort"):
		return opAbortMultipartReadSetUpload, true
	case matchPattern(path, "/sequencestore/", ""):
		return opDeleteSequenceStore, true
	}

	return "", false
}

// classifyDELETERunFamily handles /runGroup/{id}, /run/{id}, /runCache/{id},
// and /runBatch/{id} (real AWS DeleteBatch; see classifyPOSTStatic for the
// DeleteRunBatch/DeleteBatch wire-shape note).
func classifyDELETERunFamily(path string) (string, bool) {
	switch {
	case matchPattern(path, pathRunGroup+"/", ""):
		return opDeleteRunGroup, true
	case matchPattern(path, pathRun+"/", ""):
		return opDeleteRun, true
	case matchPattern(path, pathRunCache+"/", ""):
		return opDeleteRunCache, true
	case matchPattern(path, pathRunBatch+"/", ""):
		return opDeleteBatch, true
	}

	return "", false
}

// classifyDELETEWorkflowFamily handles /workflow/{id}/version/{name} and
// /workflow/{id}.
func classifyDELETEWorkflowFamily(path string) (string, bool) {
	switch {
	case strings.HasPrefix(path, pathWorkflow+"/") && strings.Contains(path, "/version/"):
		return opDeleteWorkflowVersion, true
	case matchPattern(path, pathWorkflow+"/", ""):
		return opDeleteWorkflow, true
	}

	return "", false
}

// classifyDELETEStoreFamily handles /annotationStore/{name},
// /variantStore/{name}, and /share/{id}.
func classifyDELETEStoreFamily(path string) (string, bool) {
	switch {
	case matchPattern(path, "/annotationStore/", ""):
		return opDeleteAnnotationStore, true
	case matchPattern(path, "/variantStore/", ""):
		return opDeleteVariantStore, true
	case matchPattern(path, "/share/", ""):
		return opDeleteShare, true
	}

	return "", false
}

// classifyDELETEImportJobFamily handles /import/annotation/{id} and
// /import/variant/{id}.
func classifyDELETEImportJobFamily(path string) (string, bool) {
	switch {
	case strings.HasPrefix(path, "/import/annotation/"):
		return opCancelAnnotationImportJob, true
	case strings.HasPrefix(path, "/import/variant/"):
		return opCancelVariantImportJob, true
	}

	return "", false
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
