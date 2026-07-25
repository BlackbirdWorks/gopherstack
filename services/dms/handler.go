package dms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

const (
	dmsTargetPrefix    = "AmazonDMSv20160101."
	contentType        = "application/x-amz-json-1.1"
	dmsDefaultPageSize = 100
)

// DMS operation names, as used in the X-Amz-Target header and the ops
// dispatch table below.
const (
	opAddTagsToResource                            = "AddTagsToResource"
	opApplyPendingMaintenanceAction                = "ApplyPendingMaintenanceAction"
	opCreateDataMigration                          = "CreateDataMigration"
	opCreateDataProvider                           = "CreateDataProvider"
	opCreateEndpoint                               = "CreateEndpoint"
	opCreateEventSubscription                      = "CreateEventSubscription"
	opCreateFleetAdvisorCollector                  = "CreateFleetAdvisorCollector"
	opCreateInstanceProfile                        = "CreateInstanceProfile"
	opCreateMigrationProject                       = "CreateMigrationProject"
	opCreateReplicationConfig                      = "CreateReplicationConfig"
	opCreateReplicationInstance                    = "CreateReplicationInstance"
	opCreateReplicationSubnetGroup                 = "CreateReplicationSubnetGroup"
	opCreateReplicationTask                        = "CreateReplicationTask"
	opDeleteCertificate                            = "DeleteCertificate"
	opDeleteConnection                             = "DeleteConnection"
	opDeleteDataMigration                          = "DeleteDataMigration"
	opDeleteDataProvider                           = "DeleteDataProvider"
	opDeleteEndpoint                               = "DeleteEndpoint"
	opDeleteEventSubscription                      = "DeleteEventSubscription"
	opDeleteFleetAdvisorCollector                  = "DeleteFleetAdvisorCollector"
	opDeleteFleetAdvisorDatabases                  = "DeleteFleetAdvisorDatabases"
	opDeleteInstanceProfile                        = "DeleteInstanceProfile"
	opDeleteMigrationProject                       = "DeleteMigrationProject"
	opDeleteReplicationConfig                      = "DeleteReplicationConfig"
	opDeleteReplicationInstance                    = "DeleteReplicationInstance"
	opDeleteReplicationSubnetGroup                 = "DeleteReplicationSubnetGroup"
	opDeleteReplicationTask                        = "DeleteReplicationTask"
	opDeleteReplicationTaskAssessmentRun           = "DeleteReplicationTaskAssessmentRun"
	opDescribeAccountAttributes                    = "DescribeAccountAttributes"
	opDescribeApplicableIndividualAssessments      = "DescribeApplicableIndividualAssessments"
	opDescribeCertificates                         = "DescribeCertificates"
	opDescribeConnections                          = "DescribeConnections"
	opDescribeConversionConfiguration              = "DescribeConversionConfiguration"
	opDescribeDataMigrations                       = "DescribeDataMigrations"
	opDescribeDataProviders                        = "DescribeDataProviders"
	opDescribeEndpointSettings                     = "DescribeEndpointSettings"
	opDescribeEndpointTypes                        = "DescribeEndpointTypes"
	opDescribeEndpoints                            = "DescribeEndpoints"
	opDescribeEngineVersions                       = "DescribeEngineVersions"
	opDescribeEventCategories                      = "DescribeEventCategories"
	opDescribeEventSubscriptions                   = "DescribeEventSubscriptions"
	opDescribeEvents                               = "DescribeEvents"
	opDescribeExtensionPackAssociations            = "DescribeExtensionPackAssociations"
	opDescribeFleetAdvisorCollectors               = "DescribeFleetAdvisorCollectors"
	opDescribeFleetAdvisorDatabases                = "DescribeFleetAdvisorDatabases"
	opDescribeFleetAdvisorLsaAnalysis              = "DescribeFleetAdvisorLsaAnalysis"
	opDescribeFleetAdvisorSchemaObjectSummary      = "DescribeFleetAdvisorSchemaObjectSummary"
	opDescribeFleetAdvisorSchemas                  = "DescribeFleetAdvisorSchemas"
	opDescribeInstanceProfiles                     = "DescribeInstanceProfiles"
	opDescribeMetadataModel                        = "DescribeMetadataModel"
	opDescribeMetadataModelAssessments             = "DescribeMetadataModelAssessments"
	opDescribeMetadataModelChildren                = "DescribeMetadataModelChildren"
	opDescribeMetadataModelConversions             = "DescribeMetadataModelConversions"
	opDescribeMetadataModelCreations               = "DescribeMetadataModelCreations"
	opDescribeMetadataModelExportsAsScript         = "DescribeMetadataModelExportsAsScript"
	opDescribeMetadataModelExportsToTarget         = "DescribeMetadataModelExportsToTarget"
	opDescribeMetadataModelImports                 = "DescribeMetadataModelImports"
	opDescribeMigrationProjects                    = "DescribeMigrationProjects"
	opDescribeOrderableReplicationInstances        = "DescribeOrderableReplicationInstances"
	opDescribePendingMaintenanceActions            = "DescribePendingMaintenanceActions"
	opDescribeRecommendationLimitations            = "DescribeRecommendationLimitations"
	opDescribeRecommendations                      = "DescribeRecommendations"
	opDescribeRefreshSchemasStatus                 = "DescribeRefreshSchemasStatus"
	opDescribeReplicationConfigs                   = "DescribeReplicationConfigs"
	opDescribeReplicationInstanceTaskLogs          = "DescribeReplicationInstanceTaskLogs"
	opDescribeReplicationInstances                 = "DescribeReplicationInstances"
	opDescribeReplicationSubnetGroups              = "DescribeReplicationSubnetGroups"
	opDescribeReplicationTableStatistics           = "DescribeReplicationTableStatistics"
	opDescribeReplicationTaskAssessmentResults     = "DescribeReplicationTaskAssessmentResults"
	opDescribeReplicationTaskAssessmentRuns        = "DescribeReplicationTaskAssessmentRuns"
	opDescribeReplicationTaskIndividualAssessments = "DescribeReplicationTaskIndividualAssessments"
	opDescribeReplicationTasks                     = "DescribeReplicationTasks"
	opDescribeReplications                         = "DescribeReplications"
	opDescribeSchemas                              = "DescribeSchemas"
	opDescribeTableStatistics                      = "DescribeTableStatistics"
	opExportMetadataModelAssessment                = "ExportMetadataModelAssessment"
	opGetTargetSelectionRules                      = "GetTargetSelectionRules"
	opImportCertificate                            = "ImportCertificate"
	opListTagsForResource                          = "ListTagsForResource"
	opModifyConversionConfiguration                = "ModifyConversionConfiguration"
	opModifyDataMigration                          = "ModifyDataMigration"
	opModifyDataProvider                           = "ModifyDataProvider"
	opModifyEndpoint                               = "ModifyEndpoint"
	opModifyEventSubscription                      = "ModifyEventSubscription"
	opModifyInstanceProfile                        = "ModifyInstanceProfile"
	opModifyMigrationProject                       = "ModifyMigrationProject"
	opModifyReplicationConfig                      = "ModifyReplicationConfig"
	opModifyReplicationInstance                    = "ModifyReplicationInstance"
	opModifyReplicationSubnetGroup                 = "ModifyReplicationSubnetGroup"
	opModifyReplicationTask                        = "ModifyReplicationTask"
	opMoveReplicationTask                          = "MoveReplicationTask"
	opRebootReplicationInstance                    = "RebootReplicationInstance"
	opRefreshSchemas                               = "RefreshSchemas"
	opReloadReplicationTables                      = "ReloadReplicationTables"
	opReloadTables                                 = "ReloadTables"
	opRemoveTagsFromResource                       = "RemoveTagsFromResource"
	opRunFleetAdvisorLsaAnalysis                   = "RunFleetAdvisorLsaAnalysis"
	opStartDataMigration                           = "StartDataMigration"
	opStartExtensionPackAssociation                = "StartExtensionPackAssociation"
	opStartMetadataModelAssessment                 = "StartMetadataModelAssessment"
	opStartMetadataModelConversion                 = "StartMetadataModelConversion"
	opStartMetadataModelCreation                   = "StartMetadataModelCreation"
	opStartMetadataModelExportAsScript             = "StartMetadataModelExportAsScript"
	opStartMetadataModelExportToTarget             = "StartMetadataModelExportToTarget"
	opStartMetadataModelImport                     = "StartMetadataModelImport"
	opStartRecommendations                         = "StartRecommendations"
	opStartReplication                             = "StartReplication"
	opStartReplicationTask                         = "StartReplicationTask"
	opStartReplicationTaskAssessment               = "StartReplicationTaskAssessment"
	opStartReplicationTaskAssessmentRun            = "StartReplicationTaskAssessmentRun"
	opStopDataMigration                            = "StopDataMigration"
	opStopReplication                              = "StopReplication"
	opStopReplicationTask                          = "StopReplicationTask"
	opTestConnection                               = "TestConnection"
	opUpdateSubscriptionsToEventBridge             = "UpdateSubscriptionsToEventBridge"
)

// Handler is the Echo HTTP handler for AWS DMS operations (JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new DMS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	families := []map[string]service.JSONOpFunc{
		h.opsReplicationInstances(),
		h.opsEndpoints(),
		h.opsReplicationTasks(),
		h.opsTags(),
		h.opsMaintenance(),
		h.opsRecommendations(),
		h.opsMetadataModel(),
		h.opsAssessmentRuns(),
		h.opsDataMigrations(),
		h.opsDataProviders(),
		h.opsEventSubscriptions(),
		h.opsFleetAdvisor(),
		h.opsInstanceProfiles(),
		h.opsMigrationProjects(),
		h.opsReplicationConfigs(),
		h.opsReplicationSubnetGroups(),
		h.opsCertificates(),
		h.opsConnections(),
		h.opsAccount(),
	}

	ops := make(map[string]service.JSONOpFunc)
	for _, fam := range families {
		maps.Copy(ops, fam)
	}

	return ops
}

// Reset delegates to the backend Reset, clearing all in-memory state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "DMS" }

// GetSupportedOperations returns the list of supported DMS operations, sorted
// alphabetically for deterministic output.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for op := range h.ops {
		ops = append(ops, op)
	}

	sort.Strings(ops)

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "dms" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this DMS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS DMS requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), dmsTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the DMS operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, dmsTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the primary resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	action := h.ExtractOperation(c)
	if r := extractCoreResourceField(c, action); r != "" {
		return r
	}

	return extractExtendedResourceField(c, action)
}

// extractCoreResourceField extracts resource identifiers for core DMS operations.
func extractCoreResourceField(c *echo.Context, action string) string {
	switch action {
	case opCreateReplicationInstance, opDescribeReplicationInstances, opDeleteReplicationInstance,
		opModifyReplicationInstance, opRebootReplicationInstance:

		return extractField(c, "ReplicationInstanceIdentifier", "ReplicationInstanceArn")
	case opCreateEndpoint, opDescribeEndpoints, opDeleteEndpoint, opModifyEndpoint:

		return extractField(c, "EndpointIdentifier", "EndpointArn")
	case opCreateReplicationTask, opDescribeReplicationTasks,
		opStartReplicationTask, opStopReplicationTask, opDeleteReplicationTask,
		opModifyReplicationTask, opMoveReplicationTask:

		return extractField(c, "ReplicationTaskIdentifier", "ReplicationTaskArn")
	case opAddTagsToResource, opListTagsForResource, opRemoveTagsFromResource:

		return extractField(c, "ResourceArn")
	case opApplyPendingMaintenanceAction:

		return extractField(c, "ReplicationInstanceArn")
	case opCreateDataMigration, opDeleteDataMigration, opModifyDataMigration,
		opStartDataMigration, opStopDataMigration:

		return extractField(c, "DataMigrationName", "DataMigrationArn")
	case opCreateDataProvider, opDeleteDataProvider, opModifyDataProvider:

		return extractField(c, "DataProviderName", "DataProviderArn")
	case opCreateEventSubscription, opDeleteEventSubscription, opModifyEventSubscription:

		return extractField(c, "SubscriptionName")
	}

	return ""
}

// extractExtendedResourceField extracts resource identifiers for extended DMS operations.
func extractExtendedResourceField(c *echo.Context, action string) string {
	switch action {
	case opCreateFleetAdvisorCollector, opDeleteFleetAdvisorCollector:

		return extractField(c, "CollectorName")
	case opCreateInstanceProfile, opDeleteInstanceProfile, opModifyInstanceProfile:

		return extractField(c, "InstanceProfileName", "InstanceProfileArn")
	case opCreateMigrationProject, opDeleteMigrationProject, opModifyMigrationProject:

		return extractField(c, "MigrationProjectName", "MigrationProjectArn")
	case opCreateReplicationConfig, opDeleteReplicationConfig, opModifyReplicationConfig,
		opStartReplication, opStopReplication:

		return extractField(c, "ReplicationConfigIdentifier", "ReplicationConfigArn")
	case opCreateReplicationSubnetGroup,
		opDeleteReplicationSubnetGroup,
		opModifyReplicationSubnetGroup:

		return extractField(c, "ReplicationSubnetGroupIdentifier")
	case opDeleteCertificate, opImportCertificate:

		return extractField(c, "CertificateIdentifier", "CertificateArn")
	case opTestConnection:

		return extractField(c, "EndpointArn")
	}

	return ""
}

// extractField reads the request body (re-seeding it for subsequent reads) and
// returns the first non-empty string value found for the given JSON keys.
// Returns an empty string if the body cannot be read, is not valid JSON,
// or none of the given keys has a non-empty string value.
func extractField(c *echo.Context, keys ...string) string {
	bodyBytes, readErr := httputils.ReadBody(c.Request())
	if readErr != nil || len(bodyBytes) == 0 {
		return ""
	}

	var raw map[string]string
	if unmarshalErr := json.Unmarshal(bodyBytes, &raw); unmarshalErr != nil {
		return ""
	}

	for _, k := range keys {
		if v := raw[k]; v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for DMS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c,
			logger.Load(c.Request().Context()),
			"AmazonDMSv20160101", contentType,
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):

		return c.JSON(http.StatusNotFound, service.JSONErrorResponse{
			Type:    "ResourceNotFoundFault",
			Message: err.Error(),
		})
	case errors.Is(err, ErrAlreadyExists):

		return c.JSON(http.StatusConflict, service.JSONErrorResponse{
			Type:    "ResourceAlreadyExistsFault",
			Message: err.Error(),
		})
	case errors.Is(err, ErrInvalidState):

		return c.JSON(http.StatusBadRequest, service.JSONErrorResponse{
			Type:    "InvalidResourceStateFault",
			Message: err.Error(),
		})
	case errors.Is(err, ErrValidation):

		return c.JSON(http.StatusBadRequest, service.JSONErrorResponse{
			Type:    "ValidationException",
			Message: err.Error(),
		})
	case errors.Is(err, errUnknownAction):

		return c.JSON(http.StatusBadRequest, service.JSONErrorResponse{
			Type:    "UnknownOperationException",
			Message: err.Error(),
		})
	default:

		return c.JSON(http.StatusInternalServerError, service.JSONErrorResponse{
			Type:    "InternalFailure",
			Message: err.Error(),
		})
	}
}

type filterEntry struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// extractFilterValue searches filters for the first matching name and returns the first value.
func extractFilterValue(filters []filterEntry, names ...string) string {
	for _, f := range filters {
		for _, name := range names {
			if f.Name == name && len(f.Values) > 0 {
				return f.Values[0]
			}
		}
	}

	return ""
}

func ptrInt32(p *int32) int32 {
	if p == nil {
		return 0
	}

	return *p
}

// dmsPaginate applies cursor-based pagination to a pre-sorted slice, returning
// the page data and an optional next-marker pointer.
func dmsPaginate[T any](all []T, marker *string, maxRecords *int32) ([]T, *string) {
	var limit int
	if maxRecords != nil && *maxRecords > 0 {
		limit = int(*maxRecords)
	}

	p := page.New(all, ptrconv.String(marker), limit, dmsDefaultPageSize)

	var nextMarker *string
	if p.Next != "" {
		nextMarker = &p.Next
	}

	return p.Data, nextMarker
}

// dmsHMACPaginate applies HMAC-signed cursor-based pagination to a pre-sorted slice.
func dmsHMACPaginate[T any](
	all []T,
	marker *string,
	maxRecords *int32,
	secret string,
) ([]T, *string) {
	var limit int
	if maxRecords != nil && *maxRecords > 0 {
		limit = int(*maxRecords)
	}

	p := page.NewHMAC(all, ptrconv.String(marker), secret, limit, dmsDefaultPageSize)

	var nextMarker *string
	if p.Next != "" {
		nextMarker = &p.Next
	}

	return p.Data, nextMarker
}

// ensureNonNil returns the slice if non-nil, otherwise an empty slice.
func ensureNonNil(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}
