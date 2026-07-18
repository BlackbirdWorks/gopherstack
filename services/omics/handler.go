package omics

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityPathVersioned

	// Operation name constants.
	opCreateReferenceStore    = "CreateReferenceStore"
	opDeleteReferenceStore    = "DeleteReferenceStore"
	opGetReferenceStore       = "GetReferenceStore"
	opListReferenceStores     = "ListReferenceStores"
	opDeleteReference         = "DeleteReference"
	opGetReference            = "GetReference"
	opGetReferenceMetadata    = "GetReferenceMetadata"
	opListReferences          = "ListReferences"
	opStartReferenceImportJob = "StartReferenceImportJob"
	opGetReferenceImportJob   = "GetReferenceImportJob"
	opListReferenceImportJobs = "ListReferenceImportJobs"

	opCreateSequenceStore = "CreateSequenceStore"
	opDeleteSequenceStore = "DeleteSequenceStore"
	opGetSequenceStore    = "GetSequenceStore"
	opListSequenceStores  = "ListSequenceStores"
	opUpdateSequenceStore = "UpdateSequenceStore"

	opBatchDeleteReadSet        = "BatchDeleteReadSet"
	opGetReadSet                = "GetReadSet"
	opGetReadSetMetadata        = "GetReadSetMetadata"
	opListReadSets              = "ListReadSets"
	opStartReadSetActivationJob = "StartReadSetActivationJob"
	opGetReadSetActivationJob   = "GetReadSetActivationJob"
	opListReadSetActivationJobs = "ListReadSetActivationJobs"
	opStartReadSetExportJob     = "StartReadSetExportJob"
	opGetReadSetExportJob       = "GetReadSetExportJob"
	opListReadSetExportJobs     = "ListReadSetExportJobs"
	opStartReadSetImportJob     = "StartReadSetImportJob"
	opGetReadSetImportJob       = "GetReadSetImportJob"
	opListReadSetImportJobs     = "ListReadSetImportJobs"

	opCreateMultipartReadSetUpload   = "CreateMultipartReadSetUpload"
	opAbortMultipartReadSetUpload    = "AbortMultipartReadSetUpload"
	opCompleteMultipartReadSetUpload = "CompleteMultipartReadSetUpload"
	opListMultipartReadSetUploads    = "ListMultipartReadSetUploads"
	opListReadSetUploadParts         = "ListReadSetUploadParts"
	opUploadReadSetPart              = "UploadReadSetPart"

	opCreateRunGroup = "CreateRunGroup"
	opDeleteRunGroup = "DeleteRunGroup"
	opGetRunGroup    = "GetRunGroup"
	opListRunGroups  = "ListRunGroups"
	opUpdateRunGroup = "UpdateRunGroup"

	opStartRun     = "StartRun"
	opCancelRun    = "CancelRun"
	opDeleteRun    = "DeleteRun"
	opGetRun       = "GetRun"
	opListRuns     = "ListRuns"
	opGetRunTask   = "GetRunTask"
	opListRunTasks = "ListRunTasks"

	opCreateWorkflow = "CreateWorkflow"
	opDeleteWorkflow = "DeleteWorkflow"
	opGetWorkflow    = "GetWorkflow"
	opListWorkflows  = "ListWorkflows"
	opUpdateWorkflow = "UpdateWorkflow"

	opCreateWorkflowVersion = "CreateWorkflowVersion"
	opDeleteWorkflowVersion = "DeleteWorkflowVersion"
	opGetWorkflowVersion    = "GetWorkflowVersion"
	opListWorkflowVersions  = "ListWorkflowVersions"
	opUpdateWorkflowVersion = "UpdateWorkflowVersion"

	opCreateAnnotationStore         = "CreateAnnotationStore"
	opDeleteAnnotationStore         = "DeleteAnnotationStore"
	opGetAnnotationStore            = "GetAnnotationStore"
	opListAnnotationStores          = "ListAnnotationStores"
	opUpdateAnnotationStore         = "UpdateAnnotationStore"
	opStartAnnotationImportJob      = "StartAnnotationImportJob"
	opGetAnnotationImportJob        = "GetAnnotationImportJob"
	opListAnnotationImportJobs      = "ListAnnotationImportJobs"
	opCancelAnnotationImportJob     = "CancelAnnotationImportJob"
	opCreateAnnotationStoreVersion  = "CreateAnnotationStoreVersion"
	opDeleteAnnotationStoreVersions = "DeleteAnnotationStoreVersions"
	opGetAnnotationStoreVersion     = "GetAnnotationStoreVersion"
	opListAnnotationStoreVersions   = "ListAnnotationStoreVersions"
	opUpdateAnnotationStoreVersion  = "UpdateAnnotationStoreVersion"

	opCreateVariantStore     = "CreateVariantStore"
	opDeleteVariantStore     = "DeleteVariantStore"
	opGetVariantStore        = "GetVariantStore"
	opListVariantStores      = "ListVariantStores"
	opUpdateVariantStore     = "UpdateVariantStore"
	opStartVariantImportJob  = "StartVariantImportJob"
	opGetVariantImportJob    = "GetVariantImportJob"
	opListVariantImportJobs  = "ListVariantImportJobs"
	opCancelVariantImportJob = "CancelVariantImportJob"

	opCreateShare = "CreateShare"
	opAcceptShare = "AcceptShare"
	opDeleteShare = "DeleteShare"
	opGetShare    = "GetShare"
	opListShares  = "ListShares"

	opCreateRunCache = "CreateRunCache"
	opDeleteRunCache = "DeleteRunCache"
	opGetRunCache    = "GetRunCache"
	opListRunCaches  = "ListRunCaches"
	opUpdateRunCache = "UpdateRunCache"

	opStartRunBatch   = "StartRunBatch"
	opCancelRunBatch  = "CancelRunBatch"
	opDeleteRunBatch  = "DeleteRunBatch"
	opGetRunBatch     = "GetBatch"
	opListRunBatches  = "ListBatch"
	opDeleteBatch     = "DeleteBatch"
	opListRunsInBatch = "ListRunsInBatch"

	opCreateConfiguration = "CreateConfiguration"
	opDeleteConfiguration = "DeleteConfiguration"
	opGetConfiguration    = "GetConfiguration"
	opListConfigurations  = "ListConfigurations"

	opPutS3AccessPolicy    = "PutS3AccessPolicy"
	opGetS3AccessPolicy    = "GetS3AccessPolicy"
	opDeleteS3AccessPolicy = "DeleteS3AccessPolicy"

	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"

	opUnknown = "Unknown"

	// path prefix constants used in routing.
	pathRunGroup      = "/runGroup"
	pathRun           = "/run"
	pathRunCache      = "/runCache"
	pathRunBatch      = "/runBatch"
	pathWorkflow      = "/workflow"
	pathConfiguration = "/configuration"

	// response key constants.
	keyNextToken  = "nextToken"
	keyImportJobs = "importJobs"
	keyErrors     = "errors"
	keyTags       = "tags"
)

// Handler handles HealthOmics HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Omics" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operation names.
func (h *Handler) GetSupportedOperations() []string { //nolint:funlen // long but complete list
	return []string{
		opCreateReferenceStore,
		opDeleteReferenceStore,
		opGetReferenceStore,
		opListReferenceStores,
		opDeleteReference,
		opGetReference,
		opGetReferenceMetadata,
		opListReferences,
		opStartReferenceImportJob,
		opGetReferenceImportJob,
		opListReferenceImportJobs,
		opCreateSequenceStore,
		opDeleteSequenceStore,
		opGetSequenceStore,
		opListSequenceStores,
		opUpdateSequenceStore,
		opBatchDeleteReadSet,
		opGetReadSet,
		opGetReadSetMetadata,
		opListReadSets,
		opStartReadSetActivationJob,
		opGetReadSetActivationJob,
		opListReadSetActivationJobs,
		opStartReadSetExportJob,
		opGetReadSetExportJob,
		opListReadSetExportJobs,
		opStartReadSetImportJob,
		opGetReadSetImportJob,
		opListReadSetImportJobs,
		opCreateMultipartReadSetUpload,
		opAbortMultipartReadSetUpload,
		opCompleteMultipartReadSetUpload,
		opListMultipartReadSetUploads,
		opListReadSetUploadParts,
		opUploadReadSetPart,
		opCreateRunGroup,
		opDeleteRunGroup,
		opGetRunGroup,
		opListRunGroups,
		opUpdateRunGroup,
		opStartRun,
		opCancelRun,
		opDeleteRun,
		opGetRun,
		opListRuns,
		opGetRunTask,
		opListRunTasks,
		opCreateWorkflow,
		opDeleteWorkflow,
		opGetWorkflow,
		opListWorkflows,
		opUpdateWorkflow,
		opCreateWorkflowVersion,
		opDeleteWorkflowVersion,
		opGetWorkflowVersion,
		opListWorkflowVersions,
		opUpdateWorkflowVersion,
		opCreateAnnotationStore,
		opDeleteAnnotationStore,
		opGetAnnotationStore,
		opListAnnotationStores,
		opUpdateAnnotationStore,
		opStartAnnotationImportJob,
		opGetAnnotationImportJob,
		opListAnnotationImportJobs,
		opCancelAnnotationImportJob,
		opCreateAnnotationStoreVersion,
		opDeleteAnnotationStoreVersions,
		opGetAnnotationStoreVersion,
		opListAnnotationStoreVersions,
		opUpdateAnnotationStoreVersion,
		opCreateVariantStore,
		opDeleteVariantStore,
		opGetVariantStore,
		opListVariantStores,
		opUpdateVariantStore,
		opStartVariantImportJob,
		opGetVariantImportJob,
		opListVariantImportJobs,
		opCancelVariantImportJob,
		opCreateShare,
		opAcceptShare,
		opDeleteShare,
		opGetShare,
		opListShares,
		opCreateRunCache,
		opDeleteRunCache,
		opGetRunCache,
		opListRunCaches,
		opUpdateRunCache,
		opStartRunBatch,
		opCancelRunBatch,
		opDeleteRunBatch,
		opGetRunBatch,
		opListRunBatches,
		opDeleteBatch,
		opListRunsInBatch,
		opCreateConfiguration,
		opDeleteConfiguration,
		opGetConfiguration,
		opListConfigurations,
		opPutS3AccessPolicy,
		opGetS3AccessPolicy,
		opDeleteS3AccessPolicy,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// RouteMatcher returns a matcher for HealthOmics REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return isOmicsPath(path)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return classifyPath(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource extracts the resource identifier.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return c.Request().URL.Path
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func isOmicsPath(path string) bool {
	prefixes := []string{
		"/referencestore",
		"/referencestores",
		"/sequencestore",
		"/sequencestores",
		pathRunGroup,
		pathRun,
		pathRunCache,
		pathRunBatch,
		pathWorkflow,
		"/annotationStore",
		"/annotationStores",
		"/variantStore",
		"/variantStores",
		"/share",
		"/shares",
		"/import/annotation",
		"/import/annotations",
		"/import/variant",
		"/import/variants",
		pathConfiguration,
		"/s3accesspolicy/",
	}

	for _, p := range prefixes {
		if path == p || strings.HasPrefix(path, p+"/") || strings.HasPrefix(path, p+"?") {
			return true
		}
	}

	// Match /tags/{arn} only for Omics-owned resources (arn:aws:omics:...).
	// Other services (e.g. FIS) also expose /tags/{arn}; we must not steal their requests.
	if rest, ok := strings.CutPrefix(path, "/tags/"); ok {
		return strings.Contains(rest, ":omics:")
	}

	return false
}

// handleREST is the top-level dispatch switch: it maps the classified
// operation name to its handler_<family>.go implementation. This mirrors the
// restjson1 path routing in routes.go (classifyPath/classifyPOST/classifyGET/
// classifyDELETE) op-for-op, so its size and cyclomatic complexity are
// mechanical (one case per HealthOmics operation), not incidental -- kept
// with the funlen/gocyclo/cyclop exemption per the parity-principles nolint
// policy rather than split further, since any split would just relocate the
// same flat mapping into another equally-long function.
//
//nolint:cyclop,funlen,gocyclo // large dispatch table
func (h *Handler) handleREST(
	c *echo.Context,
) error {
	method := c.Request().Method
	path := c.Request().URL.Path

	switch classifyPath(method, path) {
	// ReferenceStore
	case opCreateReferenceStore:
		return h.handleCreateReferenceStore(c)
	case opDeleteReferenceStore:
		return h.handleDeleteReferenceStore(c, extractID(path, "/referencestore/"))
	case opGetReferenceStore:
		return h.handleGetReferenceStore(c, extractID(path, "/referencestore/"))
	case opListReferenceStores:
		return h.handleListReferenceStores(c)

	// Reference
	case opDeleteReference:
		storeID, refID := extractTwoIDs(path, "/referencestore/", "/reference/")

		return h.handleDeleteReference(c, storeID, refID)
	case opGetReference:
		storeID, refID := extractTwoIDs(path, "/referencestore/", "/reference/")

		return h.handleGetReference(c, storeID, refID)
	case opGetReferenceMetadata:
		storeID, refID := extractRefMetadataIDs(path)

		return h.handleGetReferenceMetadata(c, storeID, refID)
	case opListReferences:
		return h.handleListReferences(c, extractID(path, "/referencestore/"))
	case opStartReferenceImportJob:
		return h.handleStartReferenceImportJob(c, extractID(path, "/referencestore/"))
	case opGetReferenceImportJob:
		storeID, jobID := extractTwoIDs(path, "/referencestore/", "/importjob/")

		return h.handleGetReferenceImportJob(c, storeID, jobID)
	case opListReferenceImportJobs:
		return h.handleListReferenceImportJobs(c, extractID(path, "/referencestore/"))

	// SequenceStore
	case opCreateSequenceStore:
		return h.handleCreateSequenceStore(c)
	case opDeleteSequenceStore:
		return h.handleDeleteSequenceStore(c, extractID(path, "/sequencestore/"))
	case opGetSequenceStore:
		return h.handleGetSequenceStore(c, extractID(path, "/sequencestore/"))
	case opListSequenceStores:
		return h.handleListSequenceStores(c)
	case opUpdateSequenceStore:
		return h.handleUpdateSequenceStore(c, extractID(path, "/sequencestore/"))

	// ReadSet
	case opBatchDeleteReadSet:
		return h.handleBatchDeleteReadSet(c, extractID(path, "/sequencestore/"))
	case opGetReadSet:
		storeID, rsID := extractTwoIDs(path, "/sequencestore/", "/readset/")

		return h.handleGetReadSet(c, storeID, rsID)
	case opGetReadSetMetadata:
		storeID, rsID := extractReadSetMetadataIDs(path)

		return h.handleGetReadSetMetadata(c, storeID, rsID)
	case opListReadSets:
		return h.handleListReadSets(c, extractID(path, "/sequencestore/"))
	case opStartReadSetActivationJob:
		return h.handleStartReadSetActivationJob(c, extractID(path, "/sequencestore/"))
	case opGetReadSetActivationJob:
		storeID, jobID := extractTwoIDs(path, "/sequencestore/", "/activationjob/")

		return h.handleGetReadSetActivationJob(c, storeID, jobID)
	case opListReadSetActivationJobs:
		return h.handleListReadSetActivationJobs(c, extractID(path, "/sequencestore/"))
	case opStartReadSetExportJob:
		return h.handleStartReadSetExportJob(c, extractID(path, "/sequencestore/"))
	case opGetReadSetExportJob:
		storeID, jobID := extractTwoIDs(path, "/sequencestore/", "/exportjob/")

		return h.handleGetReadSetExportJob(c, storeID, jobID)
	case opListReadSetExportJobs:
		return h.handleListReadSetExportJobs(c, extractID(path, "/sequencestore/"))
	case opStartReadSetImportJob:
		return h.handleStartReadSetImportJob(c, extractID(path, "/sequencestore/"))
	case opGetReadSetImportJob:
		storeID, jobID := extractTwoIDs(path, "/sequencestore/", "/importjob/")

		return h.handleGetReadSetImportJob(c, storeID, jobID)
	case opListReadSetImportJobs:
		return h.handleListReadSetImportJobs(c, extractID(path, "/sequencestore/"))

	// Multipart Upload
	case opCreateMultipartReadSetUpload:
		return h.handleCreateMultipartReadSetUpload(c, extractID(path, "/sequencestore/"))
	case opAbortMultipartReadSetUpload:
		storeID, uploadID := extractUploadIDs(path)

		return h.handleAbortMultipartReadSetUpload(c, storeID, uploadID)
	case opCompleteMultipartReadSetUpload:
		storeID, uploadID := extractUploadIDs(path)

		return h.handleCompleteMultipartReadSetUpload(c, storeID, uploadID)
	case opListMultipartReadSetUploads:
		return h.handleListMultipartReadSetUploads(c, extractID(path, "/sequencestore/"))
	case opListReadSetUploadParts:
		storeID, uploadID := extractUploadIDs(path)

		return h.handleListReadSetUploadParts(c, storeID, uploadID)
	case opUploadReadSetPart:
		storeID, uploadID := extractUploadIDs(path)

		return h.handleUploadReadSetPart(c, storeID, uploadID)

	// RunGroup
	case opCreateRunGroup:
		return h.handleCreateRunGroup(c)
	case opDeleteRunGroup:
		return h.handleDeleteRunGroup(c, extractID(path, "/runGroup/"))
	case opGetRunGroup:
		return h.handleGetRunGroup(c, extractID(path, "/runGroup/"))
	case opListRunGroups:
		return h.handleListRunGroups(c)
	case opUpdateRunGroup:
		return h.handleUpdateRunGroup(c, extractID(path, "/runGroup/"))

	// Run
	case opStartRun:
		return h.handleStartRun(c)
	case opCancelRun:
		return h.handleCancelRun(c, extractID(path, "/run/"))
	case opDeleteRun:
		return h.handleDeleteRun(c, extractID(path, "/run/"))
	case opGetRun:
		return h.handleGetRun(c, extractID(path, "/run/"))
	case opListRuns:
		return h.handleListRuns(c)
	case opGetRunTask:
		runID, taskID := extractTwoIDs(path, "/run/", "/task/")

		return h.handleGetRunTask(c, runID, taskID)
	case opListRunTasks:
		return h.handleListRunTasks(c, extractID(path, "/run/"))

	// Workflow
	case opCreateWorkflow:
		return h.handleCreateWorkflow(c)
	case opDeleteWorkflow:
		return h.handleDeleteWorkflow(c, extractID(path, "/workflow/"))
	case opGetWorkflow:
		return h.handleGetWorkflow(c, extractID(path, "/workflow/"))
	case opListWorkflows:
		return h.handleListWorkflows(c)
	case opUpdateWorkflow:
		return h.handleUpdateWorkflow(c, extractID(path, "/workflow/"))

	// WorkflowVersion
	case opCreateWorkflowVersion:
		return h.handleCreateWorkflowVersion(c, extractID(path, "/workflow/"))
	case opDeleteWorkflowVersion:
		wfID, verName := extractTwoIDs(path, "/workflow/", "/version/")

		return h.handleDeleteWorkflowVersion(c, wfID, verName)
	case opGetWorkflowVersion:
		wfID, verName := extractTwoIDs(path, "/workflow/", "/version/")

		return h.handleGetWorkflowVersion(c, wfID, verName)
	case opListWorkflowVersions:
		return h.handleListWorkflowVersions(c, extractID(path, "/workflow/"))
	case opUpdateWorkflowVersion:
		wfID, verName := extractTwoIDs(path, "/workflow/", "/version/")

		return h.handleUpdateWorkflowVersion(c, wfID, verName)

	// AnnotationStore
	case opCreateAnnotationStore:
		return h.handleCreateAnnotationStore(c)
	case opDeleteAnnotationStore:
		return h.handleDeleteAnnotationStore(c, extractID(path, "/annotationStore/"))
	case opGetAnnotationStore:
		return h.handleGetAnnotationStore(c, extractID(path, "/annotationStore/"))
	case opListAnnotationStores:
		return h.handleListAnnotationStores(c)
	case opUpdateAnnotationStore:
		return h.handleUpdateAnnotationStore(c, extractID(path, "/annotationStore/"))
	case opStartAnnotationImportJob:
		return h.handleStartAnnotationImportJob(c)
	case opGetAnnotationImportJob:
		return h.handleGetAnnotationImportJob(c, extractID(path, "/import/annotation/"))
	case opListAnnotationImportJobs:
		return h.handleListAnnotationImportJobs(c)
	case opCancelAnnotationImportJob:
		return h.handleCancelAnnotationImportJob(c, extractID(path, "/import/annotation/"))
	case opCreateAnnotationStoreVersion:
		return h.handleCreateAnnotationStoreVersion(c, extractID(path, "/annotationStore/"))
	case opDeleteAnnotationStoreVersions:
		return h.handleDeleteAnnotationStoreVersions(c, extractID(path, "/annotationStore/"))
	case opGetAnnotationStoreVersion:
		name, verName := extractTwoIDs(path, "/annotationStore/", "/version/")

		return h.handleGetAnnotationStoreVersion(c, name, verName)
	case opListAnnotationStoreVersions:
		return h.handleListAnnotationStoreVersions(c, extractID(path, "/annotationStore/"))
	case opUpdateAnnotationStoreVersion:
		name, verName := extractTwoIDs(path, "/annotationStore/", "/version/")

		return h.handleUpdateAnnotationStoreVersion(c, name, verName)

	// VariantStore
	case opCreateVariantStore:
		return h.handleCreateVariantStore(c)
	case opDeleteVariantStore:
		return h.handleDeleteVariantStore(c, extractID(path, "/variantStore/"))
	case opGetVariantStore:
		return h.handleGetVariantStore(c, extractID(path, "/variantStore/"))
	case opListVariantStores:
		return h.handleListVariantStores(c)
	case opUpdateVariantStore:
		return h.handleUpdateVariantStore(c, extractID(path, "/variantStore/"))
	case opStartVariantImportJob:
		return h.handleStartVariantImportJob(c)
	case opGetVariantImportJob:
		return h.handleGetVariantImportJob(c, extractID(path, "/import/variant/"))
	case opListVariantImportJobs:
		return h.handleListVariantImportJobs(c)
	case opCancelVariantImportJob:
		return h.handleCancelVariantImportJob(c, extractID(path, "/import/variant/"))

	// Share
	case opCreateShare:
		return h.handleCreateShare(c)
	case opAcceptShare:
		return h.handleAcceptShare(c, extractID(path, "/share/"))
	case opDeleteShare:
		return h.handleDeleteShare(c, extractID(path, "/share/"))
	case opGetShare:
		return h.handleGetShare(c, extractID(path, "/share/"))
	case opListShares:
		return h.handleListShares(c)

	// RunCache
	case opCreateRunCache:
		return h.handleCreateRunCache(c)
	case opDeleteRunCache:
		return h.handleDeleteRunCache(c, extractID(path, "/runCache/"))
	case opGetRunCache:
		return h.handleGetRunCache(c, extractID(path, "/runCache/"))
	case opListRunCaches:
		return h.handleListRunCaches(c)
	case opUpdateRunCache:
		return h.handleUpdateRunCache(c, extractID(path, "/runCache/"))

	// RunBatch
	case opStartRunBatch:
		return h.handleStartRunBatch(c)
	case opCancelRunBatch:
		return h.handleCancelRunBatch(c)
	case opDeleteBatch:
		return h.handleDeleteBatch(c, extractID(path, "/runBatch/"))
	case opGetRunBatch:
		return h.handleGetRunBatch(c, extractID(path, "/runBatch/"))
	case opListRunBatches:
		return h.handleListRunBatches(c)
	case opDeleteRunBatch:
		return h.handleDeleteRunBatch(c)
	case opListRunsInBatch:
		return h.handleListRunsInBatch(c, extractID(path, "/runBatch/"))

	// Configuration
	case opCreateConfiguration:
		return h.handleCreateConfiguration(c)
	case opDeleteConfiguration:
		return h.handleDeleteConfiguration(c, extractID(path, "/configuration/"))
	case opGetConfiguration:
		return h.handleGetConfiguration(c, extractID(path, "/configuration/"))
	case opListConfigurations:
		return h.handleListConfigurations(c)

	// S3 Access Policy
	case opPutS3AccessPolicy:
		return h.handlePutS3AccessPolicy(c, strings.TrimPrefix(path, "/s3accesspolicy/"))
	case opGetS3AccessPolicy:
		return h.handleGetS3AccessPolicy(c, strings.TrimPrefix(path, "/s3accesspolicy/"))
	case opDeleteS3AccessPolicy:
		return h.handleDeleteS3AccessPolicy(c, strings.TrimPrefix(path, "/s3accesspolicy/"))

	// Tags
	case opTagResource:
		return h.handleTagResource(c, strings.TrimPrefix(path, "/tags/"))
	case opUntagResource:
		return h.handleUntagResource(c, strings.TrimPrefix(path, "/tags/"))
	case opListTagsForResource:
		return h.handleListTagsForResource(c, strings.TrimPrefix(path, "/tags/"))

	default:
		return c.JSON(
			http.StatusNotImplemented,
			errResp("NotImplementedException", "operation not implemented"),
		)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) mapError(c *echo.Context, err error) error {
	logger.Load(c.Request().Context()).Error("omics error", "error", err)

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp(errResourceNotFound, err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp(errConflict, err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errResp(errValidation, err.Error()))
	default:
		return c.JSON(
			http.StatusInternalServerError,
			errResp("InternalServerException", err.Error()),
		)
	}
}

func errResp(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}

func readJSON(c *echo.Context, v any) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp(errValidation, "invalid body"))
	}

	if len(body) == 0 {
		return nil
	}

	if jsonErr := json.Unmarshal(body, v); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp(errValidation, "invalid JSON"))
	}

	return nil
}

func paginationQueryParams(c *echo.Context) (int, string) {
	q := c.Request().URL.Query()
	nextToken := q.Get("startingToken")

	var maxResults int

	if s := q.Get("maxResults"); s != "" {
		_, _ = fmt.Sscanf(s, "%d", &maxResults)
	}

	return maxResults, nextToken
}

// listQueryParams extracts maxResults/nextToken pagination params from the
// query string for List* operations whose real AWS wire shape places these
// in the query string (with only a JSON body "filter", e.g. ListReferenceStores,
// ListReadSets, ListAnnotationStores, ListVariantStores, ListShares, ...) rather
// than the request body.
func listQueryParams(c *echo.Context) (int, string) {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	var maxResults int

	if s := q.Get("maxResults"); s != "" {
		_, _ = fmt.Sscanf(s, "%d", &maxResults)
	}

	return maxResults, nextToken
}

// batchQueryParams extracts maxItems/startingToken pagination params from the
// query string for the RunBatch family (ListBatch, ListRunsInBatch), whose
// real AWS wire shape uses "maxItems" rather than "maxResults".
func batchQueryParams(c *echo.Context) (int, string) {
	q := c.Request().URL.Query()
	nextToken := q.Get("startingToken")

	var maxResults int

	if s := q.Get("maxItems"); s != "" {
		_, _ = fmt.Sscanf(s, "%d", &maxResults)
	}

	return maxResults, nextToken
}
