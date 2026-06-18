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
	case opDeleteRunBatch:
		return h.handleDeleteRunBatch(c, extractID(path, "/runBatch/"))
	case opGetRunBatch:
		return h.handleGetRunBatch(c, extractID(path, "/runBatch/"))
	case opListRunBatches:
		return h.handleListRunBatches(c)
	case opDeleteBatch:
		return h.handleDeleteBatch(c)
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
		return opDeleteBatch
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

	// /runBatch/{batchId}/run
	if strings.HasPrefix(path, pathRunBatch+"/") && strings.HasSuffix(path, "/run") {
		return opListRunsInBatch
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
	// /runBatch/{batchId}
	case matchPattern(path, pathRunBatch+"/", ""):
		return opDeleteRunBatch
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

// ────────────────────────────────────────────────────────────────────────────
// Operation handlers
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) handleCreateReferenceStore(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rs, err := h.Backend.CreateReferenceStore(req.Name, req.Description, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rs)
}

func (h *Handler) handleDeleteReferenceStore(c *echo.Context, id string) error {
	if err := h.Backend.DeleteReferenceStore(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetReferenceStore(c *echo.Context, id string) error {
	rs, err := h.Backend.GetReferenceStore(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rs)
}

func (h *Handler) handleListReferenceStores(c *echo.Context) error {
	var req struct {
		Filter     *ReferenceStoreFilter `json:"filter"`
		NextToken  string                `json:"nextToken"`
		MaxResults int                   `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	stores, next, err := h.Backend.ListReferenceStores(req.Filter, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"referenceStores": stores,
		keyNextToken:      next,
	})
}

func (h *Handler) handleDeleteReference(c *echo.Context, storeID, id string) error {
	if err := h.Backend.DeleteReference(storeID, id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetReference(c *echo.Context, storeID, id string) error {
	// GetReference returns binary data; respond with 200 and empty body as stub
	if _, err := h.Backend.GetReferenceMetadata(storeID, id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetReferenceMetadata(c *echo.Context, storeID, id string) error {
	ref, err := h.Backend.GetReferenceMetadata(storeID, id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, ref)
}

func (h *Handler) handleListReferences(c *echo.Context, storeID string) error {
	var req struct {
		Filter     *ReferenceFilter `json:"filter"`
		NextToken  string           `json:"nextToken"`
		MaxResults int              `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	refs, next, err := h.Backend.ListReferences(storeID, req.Filter, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"references": refs,
		keyNextToken: next,
	})
}

func (h *Handler) handleStartReferenceImportJob(c *echo.Context, storeID string) error {
	var req struct {
		RoleArn string                     `json:"roleArn"`
		Sources []ReferenceImportJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReferenceImportJob(storeID, req.RoleArn, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReferenceImportJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReferenceImportJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReferenceImportJobs(c *echo.Context, storeID string) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	jobs, next, err := h.Backend.ListReferenceImportJobs(storeID, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportJobs: jobs,
		keyNextToken:  next,
	})
}

func (h *Handler) handleCreateSequenceStore(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	ss, err := h.Backend.CreateSequenceStore(req.Name, req.Description, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, ss)
}

func (h *Handler) handleDeleteSequenceStore(c *echo.Context, id string) error {
	if err := h.Backend.DeleteSequenceStore(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetSequenceStore(c *echo.Context, id string) error {
	ss, err := h.Backend.GetSequenceStore(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, ss)
}

func (h *Handler) handleListSequenceStores(c *echo.Context) error {
	var req struct {
		Filter     *SequenceStoreFilter `json:"filter"`
		NextToken  string               `json:"nextToken"`
		MaxResults int                  `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	stores, next, err := h.Backend.ListSequenceStores(req.Filter, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"sequenceStores": stores,
		keyNextToken:     next,
	})
}

func (h *Handler) handleUpdateSequenceStore(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	ss, err := h.Backend.UpdateSequenceStore(id, req.Name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, ss)
}

func (h *Handler) handleBatchDeleteReadSet(c *echo.Context, storeID string) error {
	var req struct {
		IDs []string `json:"ids"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	errs, err := h.Backend.BatchDeleteReadSet(storeID, req.IDs)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyErrors: errs})
}

func (h *Handler) handleGetReadSet(c *echo.Context, storeID, id string) error {
	// GetReadSet returns binary streaming data; return 200 empty as stub
	if _, err := h.Backend.GetReadSetMetadata(storeID, id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetReadSetMetadata(c *echo.Context, storeID, id string) error {
	rs, err := h.Backend.GetReadSetMetadata(storeID, id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rs)
}

func (h *Handler) handleListReadSets(c *echo.Context, storeID string) error {
	var req struct {
		Filter     *ReadSetFilter `json:"filter"`
		NextToken  string         `json:"nextToken"`
		MaxResults int            `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	readSets, next, err := h.Backend.ListReadSets(
		storeID,
		req.Filter,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"readSets":   readSets,
		keyNextToken: next,
	})
}

func (h *Handler) handleStartReadSetActivationJob(c *echo.Context, storeID string) error {
	var req struct {
		Sources []ReadSetActivationJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReadSetActivationJob(storeID, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReadSetActivationJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReadSetActivationJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReadSetActivationJobs(c *echo.Context, storeID string) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	jobs, next, err := h.Backend.ListReadSetActivationJobs(storeID, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"activationJobs": jobs, keyNextToken: next})
}

func (h *Handler) handleStartReadSetExportJob(c *echo.Context, storeID string) error {
	var req struct {
		Destination string                   `json:"destination"`
		Sources     []ReadSetExportJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReadSetExportJob(storeID, req.Destination, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReadSetExportJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReadSetExportJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReadSetExportJobs(c *echo.Context, storeID string) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	jobs, next, err := h.Backend.ListReadSetExportJobs(storeID, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"exportJobs": jobs, keyNextToken: next})
}

func (h *Handler) handleStartReadSetImportJob(c *echo.Context, storeID string) error {
	var req struct {
		RoleArn string                   `json:"roleArn"`
		Sources []ReadSetImportJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReadSetImportJob(storeID, req.RoleArn, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReadSetImportJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReadSetImportJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReadSetImportJobs(c *echo.Context, storeID string) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	jobs, next, err := h.Backend.ListReadSetImportJobs(storeID, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyImportJobs: jobs, keyNextToken: next})
}

func (h *Handler) handleCreateMultipartReadSetUpload(c *echo.Context, storeID string) error {
	var req struct {
		Tags         map[string]string `json:"tags"`
		Name         string            `json:"name"`
		SequenceType string            `json:"sequenceType"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	upload, err := h.Backend.CreateMultipartReadSetUpload(
		storeID,
		req.Name,
		req.SequenceType,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, upload)
}

func (h *Handler) handleAbortMultipartReadSetUpload(
	c *echo.Context,
	storeID, uploadID string,
) error {
	if err := h.Backend.AbortMultipartReadSetUpload(storeID, uploadID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCompleteMultipartReadSetUpload(
	c *echo.Context,
	storeID, uploadID string,
) error {
	rs, err := h.Backend.CompleteMultipartReadSetUpload(storeID, uploadID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rs)
}

func (h *Handler) handleListMultipartReadSetUploads(c *echo.Context, storeID string) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	uploads, next, err := h.Backend.ListMultipartReadSetUploads(
		storeID,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"uploads": uploads, keyNextToken: next})
}

func (h *Handler) handleListReadSetUploadParts(c *echo.Context, storeID, uploadID string) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	parts, next, err := h.Backend.ListReadSetUploadParts(
		storeID,
		uploadID,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"parts": parts, keyNextToken: next})
}

func (h *Handler) handleUploadReadSetPart(c *echo.Context, storeID, uploadID string) error {
	// Binary upload stub - validate the store/upload exist then return 200
	if _, _, err := h.Backend.ListMultipartReadSetUploads(storeID, 1, ""); err != nil {
		return h.mapError(c, err)
	}

	_ = uploadID

	return c.JSON(http.StatusOK, map[string]any{"checksum": "stub"})
}

func (h *Handler) handleCreateRunGroup(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		MaxCpus     int               `json:"maxCpus"`
		MaxRuns     int               `json:"maxRuns"`
		MaxDuration int               `json:"maxDuration"`
		MaxGpus     int               `json:"maxGpus"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rg, err := h.Backend.CreateRunGroup(
		req.Name,
		req.MaxCpus,
		req.MaxRuns,
		req.MaxDuration,
		req.MaxGpus,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rg)
}

func (h *Handler) handleDeleteRunGroup(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRunGroup(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRunGroup(c *echo.Context, id string) error {
	rg, err := h.Backend.GetRunGroup(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rg)
}

func (h *Handler) handleListRunGroups(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	groups, next, err := h.Backend.ListRunGroups(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runGroups": groups, keyNextToken: next})
}

func (h *Handler) handleUpdateRunGroup(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		MaxCpus     int    `json:"maxCpus"`
		MaxRuns     int    `json:"maxRuns"`
		MaxDuration int    `json:"maxDuration"`
		MaxGpus     int    `json:"maxGpus"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rg, err := h.Backend.UpdateRunGroup(
		id,
		req.Name,
		req.MaxCpus,
		req.MaxRuns,
		req.MaxDuration,
		req.MaxGpus,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rg)
}

func (h *Handler) handleStartRun(c *echo.Context) error {
	var req struct {
		Parameters map[string]any    `json:"parameters"`
		Tags       map[string]string `json:"tags"`
		WorkflowID string            `json:"workflowId"`
		RoleArn    string            `json:"roleArn"`
		Name       string            `json:"name"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	run, err := h.Backend.StartRun(req.WorkflowID, req.RoleArn, req.Name, req.Parameters, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, run)
}

func (h *Handler) handleCancelRun(c *echo.Context, id string) error {
	if err := h.Backend.CancelRun(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteRun(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRun(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRun(c *echo.Context, id string) error {
	run, err := h.Backend.GetRun(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, run)
}

func (h *Handler) handleListRuns(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	runs, next, err := h.Backend.ListRuns(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runs": runs, keyNextToken: next})
}

func (h *Handler) handleGetRunTask(c *echo.Context, runID, taskID string) error {
	task, err := h.Backend.GetRunTask(runID, taskID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, task)
}

func (h *Handler) handleListRunTasks(c *echo.Context, runID string) error {
	maxResults, nextToken := paginationQueryParams(c)
	tasks, next, err := h.Backend.ListRunTasks(runID, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tasks": tasks, keyNextToken: next})
}

func (h *Handler) handleCreateWorkflow(c *echo.Context) error {
	var req struct {
		Tags          map[string]string `json:"tags"`
		Name          string            `json:"name"`
		Description   string            `json:"description"`
		Engine        string            `json:"engine"`
		DefinitionZip []byte            `json:"definitionZip"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	wf, err := h.Backend.CreateWorkflow(
		req.Name,
		req.Description,
		string(req.DefinitionZip),
		req.Engine,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, wf)
}

func (h *Handler) handleDeleteWorkflow(c *echo.Context, id string) error {
	if err := h.Backend.DeleteWorkflow(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetWorkflow(c *echo.Context, id string) error {
	wf, err := h.Backend.GetWorkflow(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, wf)
}

func (h *Handler) handleListWorkflows(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	workflows, next, err := h.Backend.ListWorkflows(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"workflows": workflows, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflow(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateWorkflow(id, req.Name, req.Description); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateWorkflowVersion(c *echo.Context, workflowID string) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		VersionName string            `json:"versionName"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	wv, err := h.Backend.CreateWorkflowVersion(
		workflowID,
		req.VersionName,
		req.Description,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, wv)
}

func (h *Handler) handleDeleteWorkflowVersion(
	c *echo.Context,
	workflowID, versionName string,
) error {
	if err := h.Backend.DeleteWorkflowVersion(workflowID, versionName); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetWorkflowVersion(c *echo.Context, workflowID, versionName string) error {
	wv, err := h.Backend.GetWorkflowVersion(workflowID, versionName)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, wv)
}

func (h *Handler) handleListWorkflowVersions(c *echo.Context, workflowID string) error {
	maxResults, nextToken := paginationQueryParams(c)
	versions, next, err := h.Backend.ListWorkflowVersions(workflowID, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"workflowVersions": versions, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflowVersion(
	c *echo.Context,
	workflowID, versionName string,
) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateWorkflowVersion(workflowID, versionName, req.Description); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateAnnotationStore(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		StoreFormat string            `json:"storeFormat"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	as, err := h.Backend.CreateAnnotationStore(req.Name, req.StoreFormat, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, as)
}

func (h *Handler) handleDeleteAnnotationStore(c *echo.Context, name string) error {
	as, err := h.Backend.DeleteAnnotationStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, as)
}

func (h *Handler) handleGetAnnotationStore(c *echo.Context, name string) error {
	as, err := h.Backend.GetAnnotationStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, as)
}

func (h *Handler) handleListAnnotationStores(c *echo.Context) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	stores, next, err := h.Backend.ListAnnotationStores(req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"annotationStores": stores, keyNextToken: next})
}

func (h *Handler) handleUpdateAnnotationStore(c *echo.Context, name string) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	as, err := h.Backend.UpdateAnnotationStore(name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, as)
}

func (h *Handler) handleStartAnnotationImportJob(c *echo.Context) error {
	var req struct {
		DestinationName string                 `json:"destinationName"`
		RoleArn         string                 `json:"roleArn"`
		Items           []AnnotationImportItem `json:"items"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartAnnotationImportJob(req.DestinationName, req.RoleArn, req.Items)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetAnnotationImportJob(c *echo.Context, jobID string) error {
	job, err := h.Backend.GetAnnotationImportJob(jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListAnnotationImportJobs(c *echo.Context) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	jobs, next, err := h.Backend.ListAnnotationImportJobs(req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyImportJobs: jobs, keyNextToken: next})
}

func (h *Handler) handleCancelAnnotationImportJob(c *echo.Context, jobID string) error {
	if err := h.Backend.CancelAnnotationImportJob(jobID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateAnnotationStoreVersion(c *echo.Context, name string) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		VersionName string            `json:"versionName"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	v, err := h.Backend.CreateAnnotationStoreVersion(
		name,
		req.VersionName,
		req.Description,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, v)
}

func (h *Handler) handleDeleteAnnotationStoreVersions(c *echo.Context, name string) error {
	var req struct {
		Versions []string `json:"versions"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	errs, err := h.Backend.DeleteAnnotationStoreVersions(name, req.Versions)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyErrors: errs})
}

func (h *Handler) handleGetAnnotationStoreVersion(c *echo.Context, name, versionName string) error {
	v, err := h.Backend.GetAnnotationStoreVersion(name, versionName)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleListAnnotationStoreVersions(c *echo.Context, name string) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	versions, next, err := h.Backend.ListAnnotationStoreVersions(
		name,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{"annotationStoreVersions": versions, keyNextToken: next},
	)
}

func (h *Handler) handleUpdateAnnotationStoreVersion(
	c *echo.Context,
	name, versionName string,
) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	v, err := h.Backend.UpdateAnnotationStoreVersion(name, versionName, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleCreateVariantStore(c *echo.Context) error {
	var req struct {
		Tags map[string]string `json:"tags"`
		Name string            `json:"name"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	vs, err := h.Backend.CreateVariantStore(req.Name, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, vs)
}

func (h *Handler) handleDeleteVariantStore(c *echo.Context, name string) error {
	vs, err := h.Backend.DeleteVariantStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, vs)
}

func (h *Handler) handleGetVariantStore(c *echo.Context, name string) error {
	vs, err := h.Backend.GetVariantStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, vs)
}

func (h *Handler) handleListVariantStores(c *echo.Context) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	stores, next, err := h.Backend.ListVariantStores(req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"variantStores": stores, keyNextToken: next})
}

func (h *Handler) handleUpdateVariantStore(c *echo.Context, name string) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	vs, err := h.Backend.UpdateVariantStore(name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, vs)
}

func (h *Handler) handleStartVariantImportJob(c *echo.Context) error {
	var req struct {
		DestinationName string              `json:"destinationName"`
		RoleArn         string              `json:"roleArn"`
		Items           []VariantImportItem `json:"items"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartVariantImportJob(req.DestinationName, req.RoleArn, req.Items)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetVariantImportJob(c *echo.Context, jobID string) error {
	job, err := h.Backend.GetVariantImportJob(jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListVariantImportJobs(c *echo.Context) error {
	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	jobs, next, err := h.Backend.ListVariantImportJobs(req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyImportJobs: jobs, keyNextToken: next})
}

func (h *Handler) handleCancelVariantImportJob(c *echo.Context, jobID string) error {
	if err := h.Backend.CancelVariantImportJob(jobID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateShare(c *echo.Context) error {
	var req struct {
		ResourceArn         string `json:"resourceArn"`
		PrincipalSubscriber string `json:"principalSubscriber"`
		Name                string `json:"shareName"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	share, err := h.Backend.CreateShare(req.ResourceArn, req.PrincipalSubscriber, req.Name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, share)
}

func (h *Handler) handleAcceptShare(c *echo.Context, shareID string) error {
	share, err := h.Backend.AcceptShare(shareID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, share)
}

func (h *Handler) handleDeleteShare(c *echo.Context, shareID string) error {
	share, err := h.Backend.DeleteShare(shareID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, share)
}

func (h *Handler) handleGetShare(c *echo.Context, shareID string) error {
	share, err := h.Backend.GetShare(shareID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"share": share})
}

func (h *Handler) handleListShares(c *echo.Context) error {
	var req struct {
		ResourceOwner string `json:"resourceOwner"`
		NextToken     string `json:"nextToken"`
		MaxResults    int    `json:"maxResults"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	shares, next, err := h.Backend.ListShares(req.ResourceOwner, req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"shares": shares, keyNextToken: next})
}

func (h *Handler) handleCreateRunCache(c *echo.Context) error {
	var req struct {
		Tags            map[string]string `json:"tags"`
		Name            string            `json:"name"`
		CacheS3Location string            `json:"cacheS3Location"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rc, err := h.Backend.CreateRunCache(req.Name, req.CacheS3Location, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rc)
}

func (h *Handler) handleDeleteRunCache(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRunCache(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRunCache(c *echo.Context, id string) error {
	rc, err := h.Backend.GetRunCache(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rc)
}

func (h *Handler) handleListRunCaches(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	caches, next, err := h.Backend.ListRunCaches(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runCaches": caches, keyNextToken: next})
}

func (h *Handler) handleUpdateRunCache(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateRunCache(id, req.Name, req.Description); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStartRunBatch(c *echo.Context) error {
	var req struct {
		WorkflowID string `json:"workflowId"`
		RoleArn    string `json:"roleArn"`
		Name       string `json:"name"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rb, err := h.Backend.StartRunBatch(req.WorkflowID, req.RoleArn, req.Name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rb)
}

func (h *Handler) handleCancelRunBatch(c *echo.Context) error {
	var req struct {
		BatchID string `json:"batchId"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.CancelRunBatch(req.BatchID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteRunBatch(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRunBatch(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRunBatch(c *echo.Context, id string) error {
	rb, err := h.Backend.GetRunBatch(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rb)
}

func (h *Handler) handleListRunBatches(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	batches, next, err := h.Backend.ListRunBatches(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runBatches": batches, keyNextToken: next})
}

func (h *Handler) handleDeleteBatch(c *echo.Context) error {
	var req struct {
		BatchIDs []string `json:"batchIds"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	errs, err := h.Backend.DeleteRunBatches(req.BatchIDs)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyErrors: errs})
}

func (h *Handler) handleListRunsInBatch(c *echo.Context, batchID string) error {
	maxResults, nextToken := paginationQueryParams(c)
	runs, next, err := h.Backend.ListRunsInBatch(batchID, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runs": runs, keyNextToken: next})
}

func (h *Handler) handleCreateConfiguration(c *echo.Context) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	cfg, err := h.Backend.CreateConfiguration(req.Name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, cfg)
}

func (h *Handler) handleDeleteConfiguration(c *echo.Context, name string) error {
	if err := h.Backend.DeleteConfiguration(name); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetConfiguration(c *echo.Context, name string) error {
	cfg, err := h.Backend.GetConfiguration(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleListConfigurations(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	cfgs, next, err := h.Backend.ListConfigurations(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"configurations": cfgs, keyNextToken: next})
}

func (h *Handler) handlePutS3AccessPolicy(c *echo.Context, arn string) error {
	var req struct {
		S3AccessPolicy string `json:"s3AccessPolicy"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.PutS3AccessPolicy(arn, req.S3AccessPolicy); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetS3AccessPolicy(c *echo.Context, arn string) error {
	p, err := h.Backend.GetS3AccessPolicy(arn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDeleteS3AccessPolicy(c *echo.Context, arn string) error {
	if err := h.Backend.DeleteS3AccessPolicy(arn); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
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
