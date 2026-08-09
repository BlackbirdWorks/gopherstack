package omics

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

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

	pathReferenceStore    = "/referencestore"
	pathReferenceStores   = "/referencestores"
	pathSequenceStore     = "/sequencestore"
	pathSequenceStores    = "/sequencestores"
	pathAnnotationStore   = "/annotationStore"
	pathAnnotationStores  = "/annotationStores"
	pathVariantStore      = "/variantStore"
	pathVariantStores     = "/variantStores"
	pathShare             = "/share"
	pathShares            = "/shares"
	pathImportAnnotation  = "/import/annotation"
	pathImportAnnotations = "/import/annotations"
	pathImportVariant     = "/import/variant"
	pathImportVariants    = "/import/variants"

	// response key constants.
	keyNextToken  = "nextToken"
	keyImportJobs = "importJobs"
	keyErrors     = "errors"
	keyTags       = "tags"
	keyArn        = "arn"
	keyStatus     = "status"
	keyUUID       = "uuid"
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

// GetSupportedOperations returns all supported operation names, derived from
// the same opDispatch table handleREST dispatches through -- the two can
// never drift out of sync.
func (h *Handler) GetSupportedOperations() []string {
	table := opDispatch()
	ops := make([]string, 0, len(table))

	for op := range table {
		ops = append(ops, op)
	}

	sort.Strings(ops)

	return ops
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
		pathReferenceStore,
		pathReferenceStores,
		pathSequenceStore,
		pathSequenceStores,
		pathRunGroup,
		pathRun,
		pathRunCache,
		pathRunBatch,
		pathWorkflow,
		pathAnnotationStore,
		pathAnnotationStores,
		pathVariantStore,
		pathVariantStores,
		pathShare,
		pathShares,
		pathImportAnnotation,
		pathImportAnnotations,
		pathImportVariant,
		pathImportVariants,
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

// opHandlerFunc is one opDispatch table entry: given the request and the raw
// URL path (from which it extracts whatever resource IDs its operation
// needs), it invokes the matching handler_<family>.go implementation.
type opHandlerFunc func(h *Handler, c *echo.Context, path string) error

// opDispatch lazily builds the operation-name -> handler lookup table exactly
// once. It mirrors the restjson1 path routing in routes.go
// (classifyPath/classifyPOST/classifyGET/classifyDELETE) op-for-op: every
// value here extracts the same IDs from path that classifyPath's callers
// used to extract inline in the old switch-based handleREST. Using a map
// instead of a switch keeps this a flat, mechanically-checkable table (one
// entry per HealthOmics operation) with O(1) dispatch and no cyclomatic
// complexity of its own -- see GetSupportedOperations, which derives its
// list directly from this table's keys so the two can never drift apart.
//
//nolint:gochecknoglobals // read-only package-level lookup table (apigatewayv2 pattern)
var opDispatch = sync.OnceValue(func() map[string]opHandlerFunc {
	return map[string]opHandlerFunc{
		// ReferenceStore
		opCreateReferenceStore: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateReferenceStore(c)
		},
		opDeleteReferenceStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteReferenceStore(c, extractID(path, "/referencestore/"))
		},
		opGetReferenceStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetReferenceStore(c, extractID(path, "/referencestore/"))
		},
		opListReferenceStores: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListReferenceStores(c)
		},

		// Reference
		opDeleteReference: func(h *Handler, c *echo.Context, path string) error {
			storeID, refID := extractTwoIDs(path, "/referencestore/", "/reference/")

			return h.handleDeleteReference(c, storeID, refID)
		},
		opGetReference: func(h *Handler, c *echo.Context, path string) error {
			storeID, refID := extractTwoIDs(path, "/referencestore/", "/reference/")

			return h.handleGetReference(c, storeID, refID)
		},
		opGetReferenceMetadata: func(h *Handler, c *echo.Context, path string) error {
			storeID, refID := extractRefMetadataIDs(path)

			return h.handleGetReferenceMetadata(c, storeID, refID)
		},
		opListReferences: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListReferences(c, extractID(path, "/referencestore/"))
		},
		opStartReferenceImportJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleStartReferenceImportJob(c, extractID(path, "/referencestore/"))
		},
		opGetReferenceImportJob: func(h *Handler, c *echo.Context, path string) error {
			storeID, jobID := extractTwoIDs(path, "/referencestore/", "/importjob/")

			return h.handleGetReferenceImportJob(c, storeID, jobID)
		},
		opListReferenceImportJobs: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListReferenceImportJobs(c, extractID(path, "/referencestore/"))
		},

		// SequenceStore
		opCreateSequenceStore: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateSequenceStore(c)
		},
		opDeleteSequenceStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteSequenceStore(c, extractID(path, "/sequencestore/"))
		},
		opGetSequenceStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetSequenceStore(c, extractID(path, "/sequencestore/"))
		},
		opListSequenceStores: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListSequenceStores(c)
		},
		opUpdateSequenceStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleUpdateSequenceStore(c, extractID(path, "/sequencestore/"))
		},

		// ReadSet
		opBatchDeleteReadSet: func(h *Handler, c *echo.Context, path string) error {
			return h.handleBatchDeleteReadSet(c, extractID(path, "/sequencestore/"))
		},
		opGetReadSet: func(h *Handler, c *echo.Context, path string) error {
			storeID, rsID := extractTwoIDs(path, "/sequencestore/", "/readset/")

			return h.handleGetReadSet(c, storeID, rsID)
		},
		opGetReadSetMetadata: func(h *Handler, c *echo.Context, path string) error {
			storeID, rsID := extractReadSetMetadataIDs(path)

			return h.handleGetReadSetMetadata(c, storeID, rsID)
		},
		opListReadSets: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListReadSets(c, extractID(path, "/sequencestore/"))
		},
		opStartReadSetActivationJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleStartReadSetActivationJob(c, extractID(path, "/sequencestore/"))
		},
		opGetReadSetActivationJob: func(h *Handler, c *echo.Context, path string) error {
			storeID, jobID := extractTwoIDs(path, "/sequencestore/", "/activationjob/")

			return h.handleGetReadSetActivationJob(c, storeID, jobID)
		},
		opListReadSetActivationJobs: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListReadSetActivationJobs(c, extractID(path, "/sequencestore/"))
		},
		opStartReadSetExportJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleStartReadSetExportJob(c, extractID(path, "/sequencestore/"))
		},
		opGetReadSetExportJob: func(h *Handler, c *echo.Context, path string) error {
			storeID, jobID := extractTwoIDs(path, "/sequencestore/", "/exportjob/")

			return h.handleGetReadSetExportJob(c, storeID, jobID)
		},
		opListReadSetExportJobs: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListReadSetExportJobs(c, extractID(path, "/sequencestore/"))
		},
		opStartReadSetImportJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleStartReadSetImportJob(c, extractID(path, "/sequencestore/"))
		},
		opGetReadSetImportJob: func(h *Handler, c *echo.Context, path string) error {
			storeID, jobID := extractTwoIDs(path, "/sequencestore/", "/importjob/")

			return h.handleGetReadSetImportJob(c, storeID, jobID)
		},
		opListReadSetImportJobs: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListReadSetImportJobs(c, extractID(path, "/sequencestore/"))
		},

		// Multipart Upload
		opCreateMultipartReadSetUpload: func(h *Handler, c *echo.Context, path string) error {
			return h.handleCreateMultipartReadSetUpload(c, extractID(path, "/sequencestore/"))
		},
		opAbortMultipartReadSetUpload: func(h *Handler, c *echo.Context, path string) error {
			storeID, uploadID := extractUploadIDs(path)

			return h.handleAbortMultipartReadSetUpload(c, storeID, uploadID)
		},
		opCompleteMultipartReadSetUpload: func(h *Handler, c *echo.Context, path string) error {
			storeID, uploadID := extractUploadIDs(path)

			return h.handleCompleteMultipartReadSetUpload(c, storeID, uploadID)
		},
		opListMultipartReadSetUploads: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListMultipartReadSetUploads(c, extractID(path, "/sequencestore/"))
		},
		opListReadSetUploadParts: func(h *Handler, c *echo.Context, path string) error {
			storeID, uploadID := extractUploadIDs(path)

			return h.handleListReadSetUploadParts(c, storeID, uploadID)
		},
		opUploadReadSetPart: func(h *Handler, c *echo.Context, path string) error {
			storeID, uploadID := extractUploadIDs(path)

			return h.handleUploadReadSetPart(c, storeID, uploadID)
		},

		// RunGroup
		opCreateRunGroup: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateRunGroup(c)
		},
		opDeleteRunGroup: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteRunGroup(c, extractID(path, "/runGroup/"))
		},
		opGetRunGroup: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetRunGroup(c, extractID(path, "/runGroup/"))
		},
		opListRunGroups: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListRunGroups(c)
		},
		opUpdateRunGroup: func(h *Handler, c *echo.Context, path string) error {
			return h.handleUpdateRunGroup(c, extractID(path, "/runGroup/"))
		},

		// Run
		opStartRun: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleStartRun(c)
		},
		opCancelRun: func(h *Handler, c *echo.Context, path string) error {
			return h.handleCancelRun(c, extractID(path, "/run/"))
		},
		opDeleteRun: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteRun(c, extractID(path, "/run/"))
		},
		opGetRun: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetRun(c, extractID(path, "/run/"))
		},
		opListRuns: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListRuns(c)
		},
		opGetRunTask: func(h *Handler, c *echo.Context, path string) error {
			runID, taskID := extractTwoIDs(path, "/run/", "/task/")

			return h.handleGetRunTask(c, runID, taskID)
		},
		opListRunTasks: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListRunTasks(c, extractID(path, "/run/"))
		},

		// Workflow
		opCreateWorkflow: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateWorkflow(c)
		},
		opDeleteWorkflow: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteWorkflow(c, extractID(path, "/workflow/"))
		},
		opGetWorkflow: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetWorkflow(c, extractID(path, "/workflow/"))
		},
		opListWorkflows: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListWorkflows(c)
		},
		opUpdateWorkflow: func(h *Handler, c *echo.Context, path string) error {
			return h.handleUpdateWorkflow(c, extractID(path, "/workflow/"))
		},

		// WorkflowVersion
		opCreateWorkflowVersion: func(h *Handler, c *echo.Context, path string) error {
			return h.handleCreateWorkflowVersion(c, extractID(path, "/workflow/"))
		},
		opDeleteWorkflowVersion: func(h *Handler, c *echo.Context, path string) error {
			wfID, verName := extractTwoIDs(path, "/workflow/", "/version/")

			return h.handleDeleteWorkflowVersion(c, wfID, verName)
		},
		opGetWorkflowVersion: func(h *Handler, c *echo.Context, path string) error {
			wfID, verName := extractTwoIDs(path, "/workflow/", "/version/")

			return h.handleGetWorkflowVersion(c, wfID, verName)
		},
		opListWorkflowVersions: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListWorkflowVersions(c, extractID(path, "/workflow/"))
		},
		opUpdateWorkflowVersion: func(h *Handler, c *echo.Context, path string) error {
			wfID, verName := extractTwoIDs(path, "/workflow/", "/version/")

			return h.handleUpdateWorkflowVersion(c, wfID, verName)
		},

		// AnnotationStore
		opCreateAnnotationStore: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateAnnotationStore(c)
		},
		opDeleteAnnotationStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteAnnotationStore(c, extractID(path, "/annotationStore/"))
		},
		opGetAnnotationStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetAnnotationStore(c, extractID(path, "/annotationStore/"))
		},
		opListAnnotationStores: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListAnnotationStores(c)
		},
		opUpdateAnnotationStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleUpdateAnnotationStore(c, extractID(path, "/annotationStore/"))
		},
		opStartAnnotationImportJob: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleStartAnnotationImportJob(c)
		},
		opGetAnnotationImportJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetAnnotationImportJob(c, extractID(path, "/import/annotation/"))
		},
		opListAnnotationImportJobs: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListAnnotationImportJobs(c)
		},
		opCancelAnnotationImportJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleCancelAnnotationImportJob(c, extractID(path, "/import/annotation/"))
		},
		opCreateAnnotationStoreVersion: func(h *Handler, c *echo.Context, path string) error {
			return h.handleCreateAnnotationStoreVersion(c, extractID(path, "/annotationStore/"))
		},
		opDeleteAnnotationStoreVersions: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteAnnotationStoreVersions(c, extractID(path, "/annotationStore/"))
		},
		opGetAnnotationStoreVersion: func(h *Handler, c *echo.Context, path string) error {
			name, verName := extractTwoIDs(path, "/annotationStore/", "/version/")

			return h.handleGetAnnotationStoreVersion(c, name, verName)
		},
		opListAnnotationStoreVersions: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListAnnotationStoreVersions(c, extractID(path, "/annotationStore/"))
		},
		opUpdateAnnotationStoreVersion: func(h *Handler, c *echo.Context, path string) error {
			name, verName := extractTwoIDs(path, "/annotationStore/", "/version/")

			return h.handleUpdateAnnotationStoreVersion(c, name, verName)
		},

		// VariantStore
		opCreateVariantStore: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateVariantStore(c)
		},
		opDeleteVariantStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteVariantStore(c, extractID(path, "/variantStore/"))
		},
		opGetVariantStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetVariantStore(c, extractID(path, "/variantStore/"))
		},
		opListVariantStores: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListVariantStores(c)
		},
		opUpdateVariantStore: func(h *Handler, c *echo.Context, path string) error {
			return h.handleUpdateVariantStore(c, extractID(path, "/variantStore/"))
		},
		opStartVariantImportJob: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleStartVariantImportJob(c)
		},
		opGetVariantImportJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetVariantImportJob(c, extractID(path, "/import/variant/"))
		},
		opListVariantImportJobs: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListVariantImportJobs(c)
		},
		opCancelVariantImportJob: func(h *Handler, c *echo.Context, path string) error {
			return h.handleCancelVariantImportJob(c, extractID(path, "/import/variant/"))
		},

		// Share
		opCreateShare: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateShare(c)
		},
		opAcceptShare: func(h *Handler, c *echo.Context, path string) error {
			return h.handleAcceptShare(c, extractID(path, "/share/"))
		},
		opDeleteShare: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteShare(c, extractID(path, "/share/"))
		},
		opGetShare: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetShare(c, extractID(path, "/share/"))
		},
		opListShares: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListShares(c)
		},

		// RunCache
		opCreateRunCache: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateRunCache(c)
		},
		opDeleteRunCache: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteRunCache(c, extractID(path, "/runCache/"))
		},
		opGetRunCache: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetRunCache(c, extractID(path, "/runCache/"))
		},
		opListRunCaches: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListRunCaches(c)
		},
		opUpdateRunCache: func(h *Handler, c *echo.Context, path string) error {
			return h.handleUpdateRunCache(c, extractID(path, "/runCache/"))
		},

		// RunBatch
		opStartRunBatch: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleStartRunBatch(c)
		},
		opCancelRunBatch: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCancelRunBatch(c)
		},
		opDeleteBatch: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteBatch(c, extractID(path, "/runBatch/"))
		},
		opGetRunBatch: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetRunBatch(c, extractID(path, "/runBatch/"))
		},
		opListRunBatches: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListRunBatches(c)
		},
		opDeleteRunBatch: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleDeleteRunBatch(c)
		},
		opListRunsInBatch: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListRunsInBatch(c, extractID(path, "/runBatch/"))
		},

		// Configuration
		opCreateConfiguration: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleCreateConfiguration(c)
		},
		opDeleteConfiguration: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteConfiguration(c, extractID(path, "/configuration/"))
		},
		opGetConfiguration: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetConfiguration(c, extractID(path, "/configuration/"))
		},
		opListConfigurations: func(h *Handler, c *echo.Context, _ string) error {
			return h.handleListConfigurations(c)
		},

		// S3 Access Policy
		opPutS3AccessPolicy: func(h *Handler, c *echo.Context, path string) error {
			return h.handlePutS3AccessPolicy(c, strings.TrimPrefix(path, "/s3accesspolicy/"))
		},
		opGetS3AccessPolicy: func(h *Handler, c *echo.Context, path string) error {
			return h.handleGetS3AccessPolicy(c, strings.TrimPrefix(path, "/s3accesspolicy/"))
		},
		opDeleteS3AccessPolicy: func(h *Handler, c *echo.Context, path string) error {
			return h.handleDeleteS3AccessPolicy(c, strings.TrimPrefix(path, "/s3accesspolicy/"))
		},

		// Tags
		opTagResource: func(h *Handler, c *echo.Context, path string) error {
			return h.handleTagResource(c, strings.TrimPrefix(path, "/tags/"))
		},
		opUntagResource: func(h *Handler, c *echo.Context, path string) error {
			return h.handleUntagResource(c, strings.TrimPrefix(path, "/tags/"))
		},
		opListTagsForResource: func(h *Handler, c *echo.Context, path string) error {
			return h.handleListTagsForResource(c, strings.TrimPrefix(path, "/tags/"))
		},
	}
})

// handleREST is the top-level dispatch entry point: it classifies the
// request into an operation name (routes.go) and looks up its handler in
// opDispatch. This replaces what used to be a single large switch statement
// with a flat, O(1) table lookup.
func (h *Handler) handleREST(c *echo.Context) error {
	method := c.Request().Method
	path := c.Request().URL.Path

	if fn, ok := opDispatch()[classifyPath(method, path)]; ok {
		return fn(h, c, path)
	}

	return c.JSON(
		http.StatusNotImplemented,
		errResp("NotImplementedException", "operation not implemented"),
	)
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) mapError(c *echo.Context, err error) error {
	logger.Load(c.Request().Context()).Error("omics error", "error", err)

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp(errResourceNotFound, err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists), errors.Is(err, awserr.ErrConflict):
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
		if jsonErr := c.JSON(http.StatusBadRequest, errResp(errValidation, "invalid body")); jsonErr != nil {
			return jsonErr
		}

		return err
	}

	if len(body) == 0 {
		return nil
	}

	if jsonErr := json.Unmarshal(body, v); jsonErr != nil {
		if writeErr := c.JSON(http.StatusBadRequest, errResp(errValidation, "invalid JSON")); writeErr != nil {
			return writeErr
		}

		return jsonErr
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
