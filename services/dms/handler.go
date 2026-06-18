package dms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

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

const (
	dmsTargetPrefix    = "AmazonDMSv20160101."
	contentType        = "application/x-amz-json-1.1"
	dmsDefaultPageSize = 100
)

// errUnknownAction is returned when an unsupported DMS action is requested.
var errUnknownAction = errors.New("UnknownOperationException")

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

func (h *Handler) buildOps() map[string]service.JSONOpFunc { //nolint:funlen // large but unavoidable dispatch table
	return map[string]service.JSONOpFunc{
		opCreateReplicationInstance: service.WrapOp(
			h.handleCreateReplicationInstance,
		),
		opDescribeReplicationInstances: service.WrapOp(
			h.handleDescribeReplicationInstances,
		),
		opDeleteReplicationInstance: service.WrapOp(
			h.handleDeleteReplicationInstance,
		),
		opCreateEndpoint:    service.WrapOp(h.handleCreateEndpoint),
		opDescribeEndpoints: service.WrapOp(h.handleDescribeEndpoints),
		opDeleteEndpoint:    service.WrapOp(h.handleDeleteEndpoint),
		opCreateReplicationTask: service.WrapOp(
			h.handleCreateReplicationTask,
		),
		opDescribeReplicationTasks: service.WrapOp(
			h.handleDescribeReplicationTasks,
		),
		opStartReplicationTask: service.WrapOp(
			h.handleStartReplicationTask,
		),
		opStopReplicationTask: service.WrapOp(h.handleStopReplicationTask),
		opDeleteReplicationTask: service.WrapOp(
			h.handleDeleteReplicationTask,
		),
		opAddTagsToResource:   service.WrapOp(h.handleAddTagsToResource),
		opListTagsForResource: service.WrapOp(h.handleListTagsForResource),
		opApplyPendingMaintenanceAction: service.WrapOp(
			h.handleApplyPendingMaintenanceAction,
		),
		"BatchStartRecommendations": service.WrapOp(
			h.handleBatchStartRecommendations,
		),
		"CancelMetadataModelConversion": service.WrapOp(
			h.handleCancelMetadataModelConversion,
		),
		"CancelMetadataModelCreation": service.WrapOp(
			h.handleCancelMetadataModelCreation,
		),
		"CancelReplicationTaskAssessmentRun": service.WrapOp(
			h.handleCancelReplicationTaskAssessmentRun,
		),
		opCreateDataMigration: service.WrapOp(h.handleCreateDataMigration),
		opCreateDataProvider:  service.WrapOp(h.handleCreateDataProvider),
		opCreateEventSubscription: service.WrapOp(
			h.handleCreateEventSubscription,
		),
		opCreateFleetAdvisorCollector: service.WrapOp(
			h.handleCreateFleetAdvisorCollector,
		),
		opCreateInstanceProfile: service.WrapOp(
			h.handleCreateInstanceProfile,
		),
		opCreateMigrationProject: service.WrapOp(
			h.handleCreateMigrationProject,
		),
		opCreateReplicationConfig: service.WrapOp(
			h.handleCreateReplicationConfig,
		),
		opCreateReplicationSubnetGroup: service.WrapOp(
			h.handleCreateReplicationSubnetGroup,
		),
		opDeleteCertificate:   service.WrapOp(h.handleDeleteCertificate),
		opDeleteConnection:    service.WrapOp(h.handleDeleteConnection),
		opDeleteDataMigration: service.WrapOp(h.handleDeleteDataMigration),
		opDeleteDataProvider:  service.WrapOp(h.handleDeleteDataProvider),
		opDeleteEventSubscription: service.WrapOp(
			h.handleDeleteEventSubscription,
		),
		opDeleteFleetAdvisorCollector: service.WrapOp(
			h.handleDeleteFleetAdvisorCollector,
		),
		opDeleteFleetAdvisorDatabases: service.WrapOp(
			h.handleDeleteFleetAdvisorDatabases,
		),
		opDeleteInstanceProfile: service.WrapOp(
			h.handleDeleteInstanceProfile,
		),
		opDeleteMigrationProject: service.WrapOp(
			h.handleDeleteMigrationProject,
		),
		opDeleteReplicationConfig: service.WrapOp(
			h.handleDeleteReplicationConfig,
		),
		opDeleteReplicationSubnetGroup: service.WrapOp(
			h.handleDeleteReplicationSubnetGroup,
		),
		opDeleteReplicationTaskAssessmentRun: service.WrapOp(
			h.handleDeleteReplicationTaskAssessmentRun,
		),
		opDescribeAccountAttributes: service.WrapOp(
			h.handleDescribeAccountAttributes,
		),
		opDescribeApplicableIndividualAssessments: service.WrapOp(
			h.handleDescribeApplicableIndividualAssessments,
		),
		opDescribeCertificates: service.WrapOp(
			h.handleDescribeCertificates,
		),
		opDescribeConnections: service.WrapOp(h.handleDescribeConnections),
		opDescribeConversionConfiguration: service.WrapOp(
			h.handleDescribeConversionConfiguration,
		),
		opDescribeDataMigrations: service.WrapOp(
			h.handleDescribeDataMigrations,
		),
		opDescribeDataProviders: service.WrapOp(
			h.handleDescribeDataProviders,
		),
		opDescribeEndpointSettings: service.WrapOp(
			h.handleDescribeEndpointSettings,
		),
		opDescribeEndpointTypes: service.WrapOp(
			h.handleDescribeEndpointTypes,
		),
		opDescribeEngineVersions: service.WrapOp(
			h.handleDescribeEngineVersions,
		),
		opDescribeEventCategories: service.WrapOp(
			h.handleDescribeEventCategories,
		),
		opDescribeEventSubscriptions: service.WrapOp(
			h.handleDescribeEventSubscriptions,
		),
		opDescribeEvents: service.WrapOp(h.handleDescribeEvents),
		opDescribeExtensionPackAssociations: service.WrapOp(
			h.handleDescribeExtensionPackAssociations,
		),
		opDescribeFleetAdvisorCollectors: service.WrapOp(
			h.handleDescribeFleetAdvisorCollectors,
		),
		opDescribeFleetAdvisorDatabases: service.WrapOp(
			h.handleDescribeFleetAdvisorDatabases,
		),
		opDescribeFleetAdvisorLsaAnalysis: service.WrapOp(
			h.handleDescribeFleetAdvisorLsaAnalysis,
		),
		opDescribeFleetAdvisorSchemaObjectSummary: service.WrapOp(
			h.handleDescribeFleetAdvisorSchemaObjectSummary,
		),
		opDescribeFleetAdvisorSchemas: service.WrapOp(
			h.handleDescribeFleetAdvisorSchemas,
		),
		opDescribeInstanceProfiles: service.WrapOp(
			h.handleDescribeInstanceProfiles,
		),
		opDescribeMetadataModel: service.WrapOp(
			h.handleDescribeMetadataModel,
		),
		opDescribeMetadataModelAssessments: service.WrapOp(
			h.handleDescribeMetadataModelAssessments,
		),
		opDescribeMetadataModelChildren: service.WrapOp(
			h.handleDescribeMetadataModelChildren,
		),
		opDescribeMetadataModelConversions: service.WrapOp(
			h.handleDescribeMetadataModelConversions,
		),
		opDescribeMetadataModelCreations: service.WrapOp(
			h.handleDescribeMetadataModelCreations,
		),
		opDescribeMetadataModelExportsAsScript: service.WrapOp(
			h.handleDescribeMetadataModelExportsAsScript,
		),
		opDescribeMetadataModelExportsToTarget: service.WrapOp(
			h.handleDescribeMetadataModelExportsToTarget,
		),
		opDescribeMetadataModelImports: service.WrapOp(
			h.handleDescribeMetadataModelImports,
		),
		opDescribeMigrationProjects: service.WrapOp(
			h.handleDescribeMigrationProjects,
		),
		opDescribeOrderableReplicationInstances: service.WrapOp(
			h.handleDescribeOrderableReplicationInstances,
		),
		opDescribePendingMaintenanceActions: service.WrapOp(
			h.handleDescribePendingMaintenanceActions,
		),
		opDescribeRecommendationLimitations: service.WrapOp(
			h.handleDescribeRecommendationLimitations,
		),
		opDescribeRecommendations: service.WrapOp(
			h.handleDescribeRecommendations,
		),
		opDescribeRefreshSchemasStatus: service.WrapOp(
			h.handleDescribeRefreshSchemasStatus,
		),
		opDescribeReplicationConfigs: service.WrapOp(
			h.handleDescribeReplicationConfigs,
		),
		opDescribeReplicationInstanceTaskLogs: service.WrapOp(
			h.handleDescribeReplicationInstanceTaskLogs,
		),
		opDescribeReplicationSubnetGroups: service.WrapOp(
			h.handleDescribeReplicationSubnetGroups,
		),
		opDescribeReplicationTableStatistics: service.WrapOp(
			h.handleDescribeReplicationTableStatistics,
		),
		opDescribeReplicationTaskAssessmentResults: service.WrapOp(
			h.handleDescribeReplicationTaskAssessmentResults,
		),
		opDescribeReplicationTaskAssessmentRuns: service.WrapOp(
			h.handleDescribeReplicationTaskAssessmentRuns,
		),
		opDescribeReplicationTaskIndividualAssessments: service.WrapOp(
			h.handleDescribeReplicationTaskIndividualAssessments,
		),
		opDescribeReplications: service.WrapOp(
			h.handleDescribeReplications,
		),
		opDescribeSchemas: service.WrapOp(h.handleDescribeSchemas),
		opDescribeTableStatistics: service.WrapOp(
			h.handleDescribeTableStatistics,
		),
		opExportMetadataModelAssessment: service.WrapOp(
			h.handleExportMetadataModelAssessment,
		),
		opGetTargetSelectionRules: service.WrapOp(
			h.handleGetTargetSelectionRules,
		),
		opImportCertificate: service.WrapOp(h.handleImportCertificate),
		opModifyConversionConfiguration: service.WrapOp(
			h.handleModifyConversionConfiguration,
		),
		opModifyDataMigration: service.WrapOp(h.handleModifyDataMigration),
		opModifyDataProvider:  service.WrapOp(h.handleModifyDataProvider),
		opModifyEndpoint:      service.WrapOp(h.handleModifyEndpoint),
		opModifyEventSubscription: service.WrapOp(
			h.handleModifyEventSubscription,
		),
		opModifyInstanceProfile: service.WrapOp(
			h.handleModifyInstanceProfile,
		),
		opModifyMigrationProject: service.WrapOp(
			h.handleModifyMigrationProject,
		),
		opModifyReplicationConfig: service.WrapOp(
			h.handleModifyReplicationConfig,
		),
		opModifyReplicationInstance: service.WrapOp(
			h.handleModifyReplicationInstance,
		),
		opModifyReplicationSubnetGroup: service.WrapOp(
			h.handleModifyReplicationSubnetGroup,
		),
		opModifyReplicationTask: service.WrapOp(
			h.handleModifyReplicationTask,
		),
		opMoveReplicationTask: service.WrapOp(h.handleMoveReplicationTask),
		opRebootReplicationInstance: service.WrapOp(
			h.handleRebootReplicationInstance,
		),
		opRefreshSchemas: service.WrapOp(h.handleRefreshSchemas),
		opReloadReplicationTables: service.WrapOp(
			h.handleReloadReplicationTables,
		),
		opReloadTables: service.WrapOp(h.handleReloadTables),
		opRemoveTagsFromResource: service.WrapOp(
			h.handleRemoveTagsFromResource,
		),
		opRunFleetAdvisorLsaAnalysis: service.WrapOp(
			h.handleRunFleetAdvisorLsaAnalysis,
		),
		opStartDataMigration: service.WrapOp(h.handleStartDataMigration),
		opStartExtensionPackAssociation: service.WrapOp(
			h.handleStartExtensionPackAssociation,
		),
		opStartMetadataModelAssessment: service.WrapOp(
			h.handleStartMetadataModelAssessment,
		),
		opStartMetadataModelConversion: service.WrapOp(
			h.handleStartMetadataModelConversion,
		),
		opStartMetadataModelCreation: service.WrapOp(
			h.handleStartMetadataModelCreation,
		),
		opStartMetadataModelExportAsScript: service.WrapOp(
			h.handleStartMetadataModelExportAsScript,
		),
		opStartMetadataModelExportToTarget: service.WrapOp(
			h.handleStartMetadataModelExportToTarget,
		),
		opStartMetadataModelImport: service.WrapOp(
			h.handleStartMetadataModelImport,
		),
		opStartRecommendations: service.WrapOp(
			h.handleStartRecommendations,
		),
		opStartReplication: service.WrapOp(h.handleStartReplication),
		opStartReplicationTaskAssessment: service.WrapOp(
			h.handleStartReplicationTaskAssessment,
		),
		opStartReplicationTaskAssessmentRun: service.WrapOp(
			h.handleStartReplicationTaskAssessmentRun,
		),
		opStopDataMigration: service.WrapOp(h.handleStopDataMigration),
		opStopReplication:   service.WrapOp(h.handleStopReplication),
		opTestConnection:    service.WrapOp(h.handleTestConnection),
		opUpdateSubscriptionsToEventBridge: service.WrapOp(
			h.handleUpdateSubscriptionsToEventBridge,
		),
	}
}

// Reset delegates to the backend Reset, clearing all in-memory state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "DMS" }

// GetSupportedOperations returns the list of supported DMS operations.
func (h *Handler) GetSupportedOperations() []string { //nolint:funlen // large but complete operation list
	return []string{
		opAddTagsToResource,
		opApplyPendingMaintenanceAction,
		"BatchStartRecommendations",
		"CancelMetadataModelConversion",
		"CancelMetadataModelCreation",
		"CancelReplicationTaskAssessmentRun",
		opCreateDataMigration,
		opCreateDataProvider,
		opCreateEndpoint,
		opCreateEventSubscription,
		opCreateFleetAdvisorCollector,
		opCreateInstanceProfile,
		opCreateMigrationProject,
		opCreateReplicationConfig,
		opCreateReplicationInstance,
		opCreateReplicationSubnetGroup,
		opCreateReplicationTask,
		opDeleteCertificate,
		opDeleteConnection,
		opDeleteDataMigration,
		opDeleteDataProvider,
		opDeleteEndpoint,
		opDeleteEventSubscription,
		opDeleteFleetAdvisorCollector,
		opDeleteFleetAdvisorDatabases,
		opDeleteInstanceProfile,
		opDeleteMigrationProject,
		opDeleteReplicationConfig,
		opDeleteReplicationInstance,
		opDeleteReplicationSubnetGroup,
		opDeleteReplicationTask,
		opDeleteReplicationTaskAssessmentRun,
		opDescribeAccountAttributes,
		opDescribeApplicableIndividualAssessments,
		opDescribeCertificates,
		opDescribeConnections,
		opDescribeConversionConfiguration,
		opDescribeDataMigrations,
		opDescribeDataProviders,
		opDescribeEndpointSettings,
		opDescribeEndpointTypes,
		opDescribeEndpoints,
		opDescribeEngineVersions,
		opDescribeEventCategories,
		opDescribeEventSubscriptions,
		opDescribeEvents,
		opDescribeExtensionPackAssociations,
		opDescribeFleetAdvisorCollectors,
		opDescribeFleetAdvisorDatabases,
		opDescribeFleetAdvisorLsaAnalysis,
		opDescribeFleetAdvisorSchemaObjectSummary,
		opDescribeFleetAdvisorSchemas,
		opDescribeInstanceProfiles,
		opDescribeMetadataModel,
		opDescribeMetadataModelAssessments,
		opDescribeMetadataModelChildren,
		opDescribeMetadataModelConversions,
		opDescribeMetadataModelCreations,
		opDescribeMetadataModelExportsAsScript,
		opDescribeMetadataModelExportsToTarget,
		opDescribeMetadataModelImports,
		opDescribeMigrationProjects,
		opDescribeOrderableReplicationInstances,
		opDescribePendingMaintenanceActions,
		opDescribeRecommendationLimitations,
		opDescribeRecommendations,
		opDescribeRefreshSchemasStatus,
		opDescribeReplicationConfigs,
		opDescribeReplicationInstanceTaskLogs,
		opDescribeReplicationInstances,
		opDescribeReplicationSubnetGroups,
		opDescribeReplicationTableStatistics,
		opDescribeReplicationTaskAssessmentResults,
		opDescribeReplicationTaskAssessmentRuns,
		opDescribeReplicationTaskIndividualAssessments,
		opDescribeReplicationTasks,
		opDescribeReplications,
		opDescribeSchemas,
		opDescribeTableStatistics,
		opExportMetadataModelAssessment,
		opGetTargetSelectionRules,
		opImportCertificate,
		opListTagsForResource,
		opModifyConversionConfiguration,
		opModifyDataMigration,
		opModifyDataProvider,
		opModifyEndpoint,
		opModifyEventSubscription,
		opModifyInstanceProfile,
		opModifyMigrationProject,
		opModifyReplicationConfig,
		opModifyReplicationInstance,
		opModifyReplicationSubnetGroup,
		opModifyReplicationTask,
		opMoveReplicationTask,
		opRebootReplicationInstance,
		opRefreshSchemas,
		opReloadReplicationTables,
		opReloadTables,
		opRemoveTagsFromResource,
		opRunFleetAdvisorLsaAnalysis,
		opStartDataMigration,
		opStartExtensionPackAssociation,
		opStartMetadataModelAssessment,
		opStartMetadataModelConversion,
		opStartMetadataModelCreation,
		opStartMetadataModelExportAsScript,
		opStartMetadataModelExportToTarget,
		opStartMetadataModelImport,
		opStartRecommendations,
		opStartReplication,
		opStartReplicationTask,
		opStartReplicationTaskAssessment,
		opStartReplicationTaskAssessmentRun,
		opStopDataMigration,
		opStopReplication,
		opStopReplicationTask,
		opTestConnection,
		opUpdateSubscriptionsToEventBridge,
	}
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
	case opCreateReplicationConfig, opDeleteReplicationConfig, opModifyReplicationConfig:

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

// --- Replication Instance handlers ---

type createReplicationInstanceInput struct {
	ReplicationInstanceIdentifier *string    `json:"ReplicationInstanceIdentifier"`
	ReplicationInstanceClass      *string    `json:"ReplicationInstanceClass"`
	EngineVersion                 *string    `json:"EngineVersion"`
	AvailabilityZone              *string    `json:"AvailabilityZone"`
	AllocatedStorage              *int32     `json:"AllocatedStorage"`
	MultiAZ                       *bool      `json:"MultiAZ"`
	AutoMinorVersionUpgrade       *bool      `json:"AutoMinorVersionUpgrade"`
	PubliclyAccessible            *bool      `json:"PubliclyAccessible"`
	Tags                          []tagEntry `json:"Tags"`
}

type createReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleCreateReplicationInstance(
	ctx context.Context, in *createReplicationInstanceInput,
) (*createReplicationInstanceOutput, error) {
	identifier := ptrStr(in.ReplicationInstanceIdentifier)
	class := ptrStr(in.ReplicationInstanceClass)

	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceIdentifier is required", ErrValidation)
	}

	if class == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceClass is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	ri, err := h.Backend.CreateReplicationInstance(
		ctx,
		identifier, class,
		ptrStr(in.EngineVersion),
		ptrStr(in.AvailabilityZone),
		ptrInt32(in.AllocatedStorage),
		ptrBool(in.MultiAZ),
		ptrBool(in.AutoMinorVersionUpgrade),
		ptrBool(in.PubliclyAccessible),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationInstanceOutput{
		ReplicationInstance: riToJSON(ri),
	}, nil
}

type describeReplicationInstancesInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationInstancesOutput struct {
	Marker               *string                   `json:"Marker,omitempty"`
	ReplicationInstances []replicationInstanceJSON `json:"ReplicationInstances"`
}

func (h *Handler) handleDescribeReplicationInstances(
	ctx context.Context,
	in *describeReplicationInstancesInput,
) (*describeReplicationInstancesOutput, error) {
	identifier := extractFilterValue(in.Filters, "replication-instance-id")
	arnFilter := extractFilterValue(in.Filters, "replication-instance-arn")

	// ARN filter takes precedence when present.
	lookup := identifier
	if arnFilter != "" {
		lookup = arnFilter
	}

	list, err := h.Backend.DescribeReplicationInstances(ctx, lookup)
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationInstanceIdentifier < list[j].ReplicationInstanceIdentifier
	})

	all := make([]replicationInstanceJSON, 0, len(list))
	for _, ri := range list {
		all = append(all, riToJSON(ri))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationInstancesOutput{ReplicationInstances: data, Marker: nextMarker}, nil
}

type deleteReplicationInstanceInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type deleteReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleDeleteReplicationInstance(
	ctx context.Context, in *deleteReplicationInstanceInput,
) (*deleteReplicationInstanceOutput, error) {
	arnOrID := ptrStr(in.ReplicationInstanceArn)
	// Retrieve before deletion to return it in the response.
	instances, err := h.Backend.DescribeReplicationInstances(ctx, arnOrID)
	if err != nil {
		// Try ARN lookup via delete directly.
		if delErr := h.Backend.DeleteReplicationInstance(ctx, arnOrID); delErr != nil {
			return nil, delErr
		}

		return &deleteReplicationInstanceOutput{}, nil
	}

	if delErr := h.Backend.DeleteReplicationInstance(ctx, arnOrID); delErr != nil {
		return nil, delErr
	}

	if len(instances) == 0 {
		return &deleteReplicationInstanceOutput{}, nil
	}

	return &deleteReplicationInstanceOutput{ReplicationInstance: riToJSON(instances[0])}, nil
}

// --- Endpoint handlers ---

type createEndpointInput struct {
	EndpointIdentifier *string    `json:"EndpointIdentifier"`
	EndpointType       *string    `json:"EndpointType"`
	EngineName         *string    `json:"EngineName"`
	ServerName         *string    `json:"ServerName"`
	DatabaseName       *string    `json:"DatabaseName"`
	Username           *string    `json:"Username"`
	Port               *int32     `json:"Port"`
	Tags               []tagEntry `json:"Tags"`
}

type createEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleCreateEndpoint(
	ctx context.Context, in *createEndpointInput,
) (*createEndpointOutput, error) {
	identifier := ptrStr(in.EndpointIdentifier)
	endpointType := ptrStr(in.EndpointType)
	engineName := ptrStr(in.EngineName)

	if identifier == "" {
		return nil, fmt.Errorf("%w: EndpointIdentifier is required", ErrValidation)
	}

	if endpointType == "" {
		return nil, fmt.Errorf("%w: EndpointType is required", ErrValidation)
	}

	if engineName == "" {
		return nil, fmt.Errorf("%w: EngineName is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	ep, err := h.Backend.CreateEndpoint(
		ctx,
		identifier,
		endpointType,
		engineName,
		ptrStr(in.ServerName),
		ptrStr(in.DatabaseName),
		ptrStr(in.Username),
		ptrInt32(in.Port),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type describeEndpointsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeEndpointsOutput struct {
	Marker    *string        `json:"Marker,omitempty"`
	Endpoints []endpointJSON `json:"Endpoints"`
}

func (h *Handler) handleDescribeEndpoints(
	ctx context.Context,
	in *describeEndpointsInput,
) (*describeEndpointsOutput, error) {
	identifier := extractFilterValue(in.Filters, "endpoint-id")
	arnFilter := extractFilterValue(in.Filters, "endpoint-arn")

	lookup := identifier
	if arnFilter != "" {
		lookup = arnFilter
	}

	list, err := h.Backend.DescribeEndpoints(ctx, lookup)
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].EndpointIdentifier < list[j].EndpointIdentifier
	})

	all := make([]endpointJSON, 0, len(list))
	for _, ep := range list {
		all = append(all, epToJSON(ep))
	}

	data, nextMarker := dmsHMACPaginate(all, in.Marker, in.MaxRecords, h.Backend.PaginationSecret())

	return &describeEndpointsOutput{Endpoints: data, Marker: nextMarker}, nil
}

type deleteEndpointInput struct {
	EndpointArn *string `json:"EndpointArn"`
}

type deleteEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleDeleteEndpoint(
	ctx context.Context, in *deleteEndpointInput,
) (*deleteEndpointOutput, error) {
	ep, err := h.Backend.DeleteEndpoint(ctx, ptrStr(in.EndpointArn))
	if err != nil {
		return nil, err
	}

	return &deleteEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

// --- Replication Task handlers ---

type createReplicationTaskInput struct {
	ReplicationTaskIdentifier *string    `json:"ReplicationTaskIdentifier"`
	SourceEndpointArn         *string    `json:"SourceEndpointArn"`
	TargetEndpointArn         *string    `json:"TargetEndpointArn"`
	ReplicationInstanceArn    *string    `json:"ReplicationInstanceArn"`
	MigrationType             *string    `json:"MigrationType"`
	TableMappings             *string    `json:"TableMappings"`
	ReplicationTaskSettings   *string    `json:"ReplicationTaskSettings"`
	Tags                      []tagEntry `json:"Tags"`
}

type createReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleCreateReplicationTask(
	ctx context.Context, in *createReplicationTaskInput,
) (*createReplicationTaskOutput, error) {
	identifier := ptrStr(in.ReplicationTaskIdentifier)
	sourceEndpointArn := ptrStr(in.SourceEndpointArn)
	targetEndpointArn := ptrStr(in.TargetEndpointArn)
	replicationInstanceArn := ptrStr(in.ReplicationInstanceArn)
	migrationType := ptrStr(in.MigrationType)

	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationTaskIdentifier is required", ErrValidation)
	}

	if sourceEndpointArn == "" {
		return nil, fmt.Errorf("%w: SourceEndpointArn is required", ErrValidation)
	}

	if targetEndpointArn == "" {
		return nil, fmt.Errorf("%w: TargetEndpointArn is required", ErrValidation)
	}

	if replicationInstanceArn == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceArn is required", ErrValidation)
	}

	if migrationType == "" {
		return nil, fmt.Errorf("%w: MigrationType is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	rt, err := h.Backend.CreateReplicationTask(
		ctx,
		identifier,
		sourceEndpointArn,
		targetEndpointArn,
		replicationInstanceArn,
		migrationType,
		ptrStr(in.TableMappings),
		ptrStr(in.ReplicationTaskSettings),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type describeReplicationTasksInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationTasksOutput struct {
	Marker           *string               `json:"Marker,omitempty"`
	ReplicationTasks []replicationTaskJSON `json:"ReplicationTasks"`
}

func (h *Handler) handleDescribeReplicationTasks(
	ctx context.Context, in *describeReplicationTasksInput,
) (*describeReplicationTasksOutput, error) {
	arnOrID := extractFilterValue(in.Filters, "replication-task-id", "replication-task-arn")
	list, err := h.Backend.DescribeReplicationTasks(ctx, arnOrID)
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationTaskIdentifier < list[j].ReplicationTaskIdentifier
	})

	all := make([]replicationTaskJSON, 0, len(list))
	for _, rt := range list {
		all = append(all, rtToJSON(rt))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationTasksOutput{ReplicationTasks: data, Marker: nextMarker}, nil
}

type startReplicationTaskInput struct {
	ReplicationTaskArn       *string `json:"ReplicationTaskArn"`
	StartReplicationTaskType *string `json:"StartReplicationTaskType"`
}

type startReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func isValidStartReplicationTaskType(s string) bool {
	return s == "start-replication" || s == "resume-processing" || s == "reload-target"
}

func (h *Handler) handleStartReplicationTask(
	ctx context.Context, in *startReplicationTaskInput,
) (*startReplicationTaskOutput, error) {
	taskType := ptrStr(in.StartReplicationTaskType)
	if taskType == "" {
		taskType = "start-replication"
	}

	if !isValidStartReplicationTaskType(taskType) {
		return nil, fmt.Errorf(
			"%w: invalid StartReplicationTaskType %q; valid: start-replication, resume-processing, reload-target",
			ErrValidation,
			taskType,
		)
	}

	rt, err := h.Backend.StartReplicationTask(ctx, ptrStr(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &startReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type stopReplicationTaskInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type stopReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleStopReplicationTask(
	ctx context.Context, in *stopReplicationTaskInput,
) (*stopReplicationTaskOutput, error) {
	rt, err := h.Backend.StopReplicationTask(ctx, ptrStr(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &stopReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

type deleteReplicationTaskInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type deleteReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleDeleteReplicationTask(
	ctx context.Context, in *deleteReplicationTaskInput,
) (*deleteReplicationTaskOutput, error) {
	rt, err := h.Backend.DeleteReplicationTask(ctx, ptrStr(in.ReplicationTaskArn))
	if err != nil {
		return nil, err
	}

	return &deleteReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

// --- Tag handlers ---

type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type addTagsToResourceInput struct {
	ResourceArn *string    `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

type addTagsToResourceOutput struct{}

func (h *Handler) handleAddTagsToResource(
	ctx context.Context, in *addTagsToResourceInput,
) (*addTagsToResourceOutput, error) {
	kv := tagsToMap(in.Tags)
	if err := h.Backend.AddTagsToResource(ctx, ptrStr(in.ResourceArn), kv); err != nil {
		return nil, err
	}

	return &addTagsToResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn *string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	TagList []tagEntry `json:"TagList"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context, in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kv, err := h.Backend.ListTagsForResource(ctx, ptrStr(in.ResourceArn))
	if err != nil {
		return nil, err
	}

	list := make([]tagEntry, 0, len(kv))
	for k, v := range kv {
		list = append(list, tagEntry{Key: k, Value: v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })

	return &listTagsForResourceOutput{TagList: list}, nil
}

// --- JSON response types ---

// replicationSubnetGroupJSON is a minimal representation of a DMS replication
// subnet group. The Terraform AWS provider accesses ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier
// unconditionally in its Read function, so we must always return a non-null object.
type replicationSubnetGroupJSON struct {
	ReplicationSubnetGroupIdentifier *string `json:"ReplicationSubnetGroupIdentifier"`
}

type replicationInstanceJSON struct {
	ReplicationSubnetGroup                replicationSubnetGroupJSON `json:"ReplicationSubnetGroup"`
	ReplicationInstanceIdentifier         string                     `json:"ReplicationInstanceIdentifier"`
	ReplicationInstanceArn                string                     `json:"ReplicationInstanceArn"`
	ReplicationInstanceClass              string                     `json:"ReplicationInstanceClass"`
	EngineVersion                         string                     `json:"EngineVersion"`
	AvailabilityZone                      string                     `json:"AvailabilityZone"`
	ReplicationInstanceStatus             string                     `json:"ReplicationInstanceStatus"`
	ReplicationInstancePrivateIPAddresses []string                   `json:"ReplicationInstancePrivateIpAddresses"`
	ReplicationInstancePublicIPAddresses  []string                   `json:"ReplicationInstancePublicIpAddresses"`
	VpcSecurityGroups                     []any                      `json:"VpcSecurityGroups"`
	AllocatedStorage                      int32                      `json:"AllocatedStorage"`
	MultiAZ                               bool                       `json:"MultiAZ"`
	AutoMinorVersionUpgrade               bool                       `json:"AutoMinorVersionUpgrade"`
	PubliclyAccessible                    bool                       `json:"PubliclyAccessible"`
}

func riToJSON(ri *ReplicationInstance) replicationInstanceJSON {
	emptyID := ""

	privateIPs := []string{ri.PrivateIPAddress}
	publicIPs := []string{}

	return replicationInstanceJSON{
		// ReplicationSubnetGroup must always be present with a non-nil Identifier.
		// The Terraform AWS provider accesses ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier
		// directly (no nil check), so a nil pointer causes a panic.
		ReplicationSubnetGroup: replicationSubnetGroupJSON{
			ReplicationSubnetGroupIdentifier: &emptyID,
		},
		ReplicationInstanceIdentifier:         ri.ReplicationInstanceIdentifier,
		ReplicationInstanceArn:                ri.ReplicationInstanceArn,
		ReplicationInstanceClass:              ri.ReplicationInstanceClass,
		EngineVersion:                         ri.EngineVersion,
		AvailabilityZone:                      ri.AvailabilityZone,
		ReplicationInstanceStatus:             ri.ReplicationInstanceStatus,
		ReplicationInstancePrivateIPAddresses: privateIPs,
		ReplicationInstancePublicIPAddresses:  publicIPs,
		VpcSecurityGroups:                     []any{},
		AllocatedStorage:                      ri.AllocatedStorage,
		MultiAZ:                               ri.MultiAZ,
		AutoMinorVersionUpgrade:               ri.AutoMinorVersionUpgrade,
		PubliclyAccessible:                    ri.PubliclyAccessible,
	}
}

type endpointJSON struct {
	EndpointIdentifier string `json:"EndpointIdentifier"`
	EndpointArn        string `json:"EndpointArn"`
	EndpointType       string `json:"EndpointType"`
	EngineName         string `json:"EngineName"`
	ServerName         string `json:"ServerName,omitempty"`
	DatabaseName       string `json:"DatabaseName,omitempty"`
	Username           string `json:"Username,omitempty"`
	Status             string `json:"Status"`
	Port               int32  `json:"Port,omitempty"`
}

func epToJSON(ep *Endpoint) endpointJSON {
	return endpointJSON{
		EndpointIdentifier: ep.EndpointIdentifier,
		EndpointArn:        ep.EndpointArn,
		EndpointType:       ep.EndpointType,
		EngineName:         ep.EngineName,
		ServerName:         ep.ServerName,
		DatabaseName:       ep.DatabaseName,
		Username:           ep.Username,
		Status:             ep.Status,
		Port:               ep.Port,
	}
}

type replicationTaskJSON struct {
	ReplicationTaskIdentifier string `json:"ReplicationTaskIdentifier"`
	ReplicationTaskArn        string `json:"ReplicationTaskArn"`
	SourceEndpointArn         string `json:"SourceEndpointArn"`
	TargetEndpointArn         string `json:"TargetEndpointArn"`
	ReplicationInstanceArn    string `json:"ReplicationInstanceArn"`
	MigrationType             string `json:"MigrationType"`
	TableMappings             string `json:"TableMappings,omitempty"`
	ReplicationTaskSettings   string `json:"ReplicationTaskSettings,omitempty"`
	Status                    string `json:"Status"`
}

func rtToJSON(rt *ReplicationTask) replicationTaskJSON {
	return replicationTaskJSON{
		ReplicationTaskIdentifier: rt.ReplicationTaskIdentifier,
		ReplicationTaskArn:        rt.ReplicationTaskArn,
		SourceEndpointArn:         rt.SourceEndpointArn,
		TargetEndpointArn:         rt.TargetEndpointArn,
		ReplicationInstanceArn:    rt.ReplicationInstanceArn,
		MigrationType:             rt.MigrationType,
		TableMappings:             rt.TableMappings,
		ReplicationTaskSettings:   rt.ReplicationTaskSettings,
		Status:                    rt.Status,
	}
}

// --- Filter types ---

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

// --- Pointer helpers ---

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}

	return *p
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

	p := page.New(all, ptrStr(marker), limit, dmsDefaultPageSize)

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

	p := page.NewHMAC(all, ptrStr(marker), secret, limit, dmsDefaultPageSize)

	var nextMarker *string
	if p.Next != "" {
		nextMarker = &p.Next
	}

	return p.Data, nextMarker
}

func ptrBool(p *bool) bool {
	if p == nil {
		return false
	}

	return *p
}

// tagsToMap converts a slice of tag entries to a map.
func tagsToMap(entries []tagEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}

	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}

// --- ApplyPendingMaintenanceAction handler ---

type applyPendingMaintenanceActionInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	ApplyAction            *string `json:"ApplyAction"`
	OptInType              *string `json:"OptInType"`
}

type resourcePendingMaintenanceActionsJSON struct {
	ResourceIdentifier              string `json:"ResourceIdentifier"`
	PendingMaintenanceActionDetails []any  `json:"PendingMaintenanceActionDetails"`
}

type applyPendingMaintenanceActionOutput struct {
	ResourcePendingMaintenanceActions resourcePendingMaintenanceActionsJSON `json:"ResourcePendingMaintenanceActions"`
}

func (h *Handler) handleApplyPendingMaintenanceAction(
	ctx context.Context, in *applyPendingMaintenanceActionInput,
) (*applyPendingMaintenanceActionOutput, error) {
	instanceArn := ptrStr(in.ReplicationInstanceArn)
	if instanceArn == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceArn is required", ErrValidation)
	}

	ri, err := h.Backend.ApplyPendingMaintenanceAction(
		ctx,
		instanceArn,
		ptrStr(in.ApplyAction),
		ptrStr(in.OptInType),
	)
	if err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionOutput{
		ResourcePendingMaintenanceActions: resourcePendingMaintenanceActionsJSON{
			ResourceIdentifier:              ri.ReplicationInstanceArn,
			PendingMaintenanceActionDetails: []any{},
		},
	}, nil
}

// --- BatchStartRecommendations handler ---

type batchStartRecommendationsInput struct {
	Data []any `json:"Data"`
}

type batchStartRecommendationsErrorEntryJSON struct {
	Code       string `json:"Code"`
	Message    string `json:"Message"`
	DatabaseID string `json:"DatabaseId"`
}

type batchStartRecommendationsOutput struct {
	ErrorEntries []batchStartRecommendationsErrorEntryJSON `json:"ErrorEntries"`
}

func (h *Handler) handleBatchStartRecommendations(
	ctx context.Context, _ *batchStartRecommendationsInput,
) (*batchStartRecommendationsOutput, error) {
	if err := h.Backend.BatchStartRecommendations(ctx); err != nil {
		return nil, err
	}

	return &batchStartRecommendationsOutput{
		ErrorEntries: []batchStartRecommendationsErrorEntryJSON{},
	}, nil
}

// --- CancelMetadataModelConversion handler ---

type cancelMetadataModelConversionInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	RequestIdentifier          *string `json:"RequestIdentifier"`
}

type cancelMetadataModelConversionOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleCancelMetadataModelConversion(
	ctx context.Context, in *cancelMetadataModelConversionInput,
) (*cancelMetadataModelConversionOutput, error) {
	reqID, err := h.Backend.CancelMetadataModelConversion(
		ctx,
		ptrStr(in.MigrationProjectIdentifier),
		ptrStr(in.RequestIdentifier),
	)
	if err != nil {
		return nil, err
	}

	return &cancelMetadataModelConversionOutput{RequestIdentifier: reqID}, nil
}

// --- CancelMetadataModelCreation handler ---

type cancelMetadataModelCreationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	RequestIdentifier          *string `json:"RequestIdentifier"`
}

type cancelMetadataModelCreationOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleCancelMetadataModelCreation(
	ctx context.Context, in *cancelMetadataModelCreationInput,
) (*cancelMetadataModelCreationOutput, error) {
	reqID, err := h.Backend.CancelMetadataModelCreation(
		ctx,
		ptrStr(in.MigrationProjectIdentifier),
		ptrStr(in.RequestIdentifier),
	)
	if err != nil {
		return nil, err
	}

	return &cancelMetadataModelCreationOutput{RequestIdentifier: reqID}, nil
}

// --- CancelReplicationTaskAssessmentRun handler ---

type cancelReplicationTaskAssessmentRunInput struct {
	ReplicationTaskAssessmentRunArn *string `json:"ReplicationTaskAssessmentRunArn"`
}

type cancelReplicationTaskAssessmentRunOutput struct {
	ReplicationTaskAssessmentRun map[string]any `json:"ReplicationTaskAssessmentRun"`
}

func (h *Handler) handleCancelReplicationTaskAssessmentRun(
	ctx context.Context, in *cancelReplicationTaskAssessmentRunInput,
) (*cancelReplicationTaskAssessmentRunOutput, error) {
	if err := h.Backend.CancelReplicationTaskAssessmentRun(
		ctx,
		ptrStr(in.ReplicationTaskAssessmentRunArn),
	); err != nil {
		return nil, err
	}

	return &cancelReplicationTaskAssessmentRunOutput{
		ReplicationTaskAssessmentRun: map[string]any{
			"ReplicationTaskAssessmentRunArn": ptrStr(in.ReplicationTaskAssessmentRunArn),
			"Status":                          "cancelling",
		},
	}, nil
}

// --- CreateDataMigration handler ---

type createDataMigrationInput struct {
	DataMigrationName          *string    `json:"DataMigrationName"`
	MigrationProjectIdentifier *string    `json:"MigrationProjectIdentifier"`
	DataMigrationType          *string    `json:"DataMigrationType"`
	ServiceAccessRoleArn       *string    `json:"ServiceAccessRoleArn"`
	SelectionRules             *string    `json:"SelectionRules"`
	NumberOfJobs               *int32     `json:"NumberOfJobs"`
	EnableCloudwatchLogs       *bool      `json:"EnableCloudwatchLogs"`
	Tags                       []tagEntry `json:"Tags"`
}

type dataMigrationJSON struct {
	DataMigrationName    string `json:"DataMigrationName"`
	DataMigrationArn     string `json:"DataMigrationArn"`
	MigrationProjectArn  string `json:"MigrationProjectArn"`
	DataMigrationType    string `json:"DataMigrationType"`
	ServiceAccessRoleArn string `json:"ServiceAccessRoleArn"`
	DataMigrationStatus  string `json:"DataMigrationStatus"`
	NumberOfJobs         int32  `json:"NumberOfJobs"`
	EnableCloudwatchLogs bool   `json:"EnableCloudwatchLogs"`
}

type createDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleCreateDataMigration(
	ctx context.Context, in *createDataMigrationInput,
) (*createDataMigrationOutput, error) {
	name := ptrStr(in.DataMigrationName)
	if name == "" {
		return nil, fmt.Errorf("%w: DataMigrationName is required", ErrValidation)
	}

	migrationType := ptrStr(in.DataMigrationType)
	if migrationType == "" {
		return nil, fmt.Errorf("%w: DataMigrationType is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	dm, err := h.Backend.CreateDataMigration(
		ctx,
		name,
		ptrStr(in.MigrationProjectIdentifier),
		migrationType,
		ptrStr(in.ServiceAccessRoleArn),
		ptrStr(in.SelectionRules),
		ptrInt32(in.NumberOfJobs),
		ptrBool(in.EnableCloudwatchLogs),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

func dmToJSON(dm *DataMigration) dataMigrationJSON {
	return dataMigrationJSON{
		DataMigrationName:    dm.DataMigrationName,
		DataMigrationArn:     dm.DataMigrationArn,
		MigrationProjectArn:  dm.MigrationProjectArn,
		DataMigrationType:    dm.DataMigrationType,
		ServiceAccessRoleArn: dm.ServiceAccessRoleArn,
		DataMigrationStatus:  dm.DataMigrationStatus,
		NumberOfJobs:         dm.NumberOfJobs,
		EnableCloudwatchLogs: dm.EnableCloudwatchLogs,
	}
}

// --- CreateDataProvider handler ---

type createDataProviderInput struct {
	DataProviderName *string    `json:"DataProviderName"`
	Engine           *string    `json:"Engine"`
	Description      *string    `json:"Description"`
	Tags             []tagEntry `json:"Tags"`
}

type dataProviderJSON struct {
	DataProviderName string `json:"DataProviderName"`
	DataProviderArn  string `json:"DataProviderArn"`
	Engine           string `json:"Engine"`
	Description      string `json:"Description,omitempty"`
}

type createDataProviderOutput struct {
	DataProvider dataProviderJSON `json:"DataProvider"`
}

func (h *Handler) handleCreateDataProvider(
	ctx context.Context, in *createDataProviderInput,
) (*createDataProviderOutput, error) {
	name := ptrStr(in.DataProviderName)
	if name == "" {
		return nil, fmt.Errorf("%w: DataProviderName is required", ErrValidation)
	}

	engine := ptrStr(in.Engine)
	if engine == "" {
		return nil, fmt.Errorf("%w: Engine is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	dp, err := h.Backend.CreateDataProvider(ctx, name, engine, ptrStr(in.Description), kv)
	if err != nil {
		return nil, err
	}

	return &createDataProviderOutput{DataProvider: dpToJSON(dp)}, nil
}

func dpToJSON(dp *DataProvider) dataProviderJSON {
	return dataProviderJSON{
		DataProviderName: dp.DataProviderName,
		DataProviderArn:  dp.DataProviderArn,
		Engine:           dp.Engine,
		Description:      dp.Description,
	}
}

// --- CreateEventSubscription handler ---

type createEventSubscriptionInput struct {
	SubscriptionName *string    `json:"SubscriptionName"`
	SnsTopicArn      *string    `json:"SnsTopicArn"`
	SourceType       *string    `json:"SourceType"`
	SourceIDs        []string   `json:"SourceIds"`
	EventCategories  []string   `json:"EventCategories"`
	Enabled          *bool      `json:"Enabled"`
	Tags             []tagEntry `json:"Tags"`
}

type eventSubscriptionJSON struct {
	SubscriptionName string   `json:"SubscriptionName"`
	SnsTopicArn      string   `json:"SnsTopicArn"`
	SourceType       string   `json:"SourceType,omitempty"`
	Status           string   `json:"Status"`
	SourceIDsList    []string `json:"SourceIdsList"`
	EventCategories  []string `json:"EventCategories"`
	Enabled          bool     `json:"Enabled"`
}

type createEventSubscriptionOutput struct {
	EventSubscription eventSubscriptionJSON `json:"EventSubscription"`
}

func (h *Handler) handleCreateEventSubscription(
	ctx context.Context, in *createEventSubscriptionInput,
) (*createEventSubscriptionOutput, error) {
	name := ptrStr(in.SubscriptionName)
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrValidation)
	}

	snsTopicArn := ptrStr(in.SnsTopicArn)
	if snsTopicArn == "" {
		return nil, fmt.Errorf("%w: SnsTopicArn is required", ErrValidation)
	}

	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}

	kv := tagsToMap(in.Tags)
	es, err := h.Backend.CreateEventSubscription(
		ctx,
		name,
		snsTopicArn,
		ptrStr(in.SourceType),
		in.SourceIDs,
		in.EventCategories,
		enabled,
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionOutput{EventSubscription: esToJSON(es)}, nil
}

func esToJSON(es *EventSubscription) eventSubscriptionJSON {
	return eventSubscriptionJSON{
		SubscriptionName: es.SubscriptionName,
		SnsTopicArn:      es.SnsTopicArn,
		SourceType:       es.SourceType,
		SourceIDsList:    ensureNonNil(es.SourceIDsList),
		EventCategories:  ensureNonNil(es.EventCategories),
		Status:           es.Status,
		Enabled:          es.Enabled,
	}
}

// ensureNonNil returns the slice if non-nil, otherwise an empty slice.
func ensureNonNil(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

// --- CreateFleetAdvisorCollector handler ---

type createFleetAdvisorCollectorInput struct {
	CollectorName        *string `json:"CollectorName"`
	Description          *string `json:"Description"`
	ServiceAccessRoleArn *string `json:"ServiceAccessRoleArn"`
	S3BucketName         *string `json:"S3BucketName"`
}

type createFleetAdvisorCollectorOutput struct {
	CollectorName         string `json:"CollectorName"`
	CollectorReferencedID string `json:"CollectorReferencedId"`
	Description           string `json:"Description,omitempty"`
	ServiceAccessRoleArn  string `json:"ServiceAccessRoleArn"`
	S3BucketName          string `json:"S3BucketName"`
}

func (h *Handler) handleCreateFleetAdvisorCollector(
	ctx context.Context, in *createFleetAdvisorCollectorInput,
) (*createFleetAdvisorCollectorOutput, error) {
	name := ptrStr(in.CollectorName)
	if name == "" {
		return nil, fmt.Errorf("%w: CollectorName is required", ErrValidation)
	}

	col, err := h.Backend.CreateFleetAdvisorCollector(
		ctx,
		name,
		ptrStr(in.Description),
		ptrStr(in.ServiceAccessRoleArn),
		ptrStr(in.S3BucketName),
	)
	if err != nil {
		return nil, err
	}

	return &createFleetAdvisorCollectorOutput{
		CollectorName:         col.CollectorName,
		CollectorReferencedID: col.CollectorReferencedID,
		Description:           col.Description,
		ServiceAccessRoleArn:  col.ServiceAccessRoleArn,
		S3BucketName:          col.S3BucketName,
	}, nil
}

// --- CreateInstanceProfile handler ---

type createInstanceProfileInput struct {
	InstanceProfileName   *string    `json:"InstanceProfileName"`
	AvailabilityZone      *string    `json:"AvailabilityZone"`
	KmsKeyArn             *string    `json:"KmsKeyArn"`
	NetworkType           *string    `json:"NetworkType"`
	Description           *string    `json:"Description"`
	SubnetGroupIdentifier *string    `json:"SubnetGroupIdentifier"`
	PubliclyAccessible    *bool      `json:"PubliclyAccessible"`
	Tags                  []tagEntry `json:"Tags"`
}

type instanceProfileJSON struct {
	InstanceProfileName   string `json:"InstanceProfileName"`
	InstanceProfileArn    string `json:"InstanceProfileArn"`
	AvailabilityZone      string `json:"AvailabilityZone,omitempty"`
	KmsKeyArn             string `json:"KmsKeyArn,omitempty"`
	NetworkType           string `json:"NetworkType,omitempty"`
	Description           string `json:"Description,omitempty"`
	SubnetGroupIdentifier string `json:"SubnetGroupIdentifier,omitempty"`
	PubliclyAccessible    bool   `json:"PubliclyAccessible"`
}

type createInstanceProfileOutput struct {
	InstanceProfile instanceProfileJSON `json:"InstanceProfile"`
}

func (h *Handler) handleCreateInstanceProfile(
	ctx context.Context, in *createInstanceProfileInput,
) (*createInstanceProfileOutput, error) {
	kv := tagsToMap(in.Tags)
	ip, err := h.Backend.CreateInstanceProfile(
		ctx,
		ptrStr(in.InstanceProfileName),
		ptrStr(in.AvailabilityZone),
		ptrStr(in.KmsKeyArn),
		ptrStr(in.NetworkType),
		ptrStr(in.Description),
		ptrStr(in.SubnetGroupIdentifier),
		ptrBool(in.PubliclyAccessible),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createInstanceProfileOutput{InstanceProfile: ipToJSON(ip)}, nil
}

func ipToJSON(ip *InstanceProfile) instanceProfileJSON {
	return instanceProfileJSON{
		InstanceProfileName:   ip.InstanceProfileName,
		InstanceProfileArn:    ip.InstanceProfileArn,
		AvailabilityZone:      ip.AvailabilityZone,
		KmsKeyArn:             ip.KmsKeyArn,
		NetworkType:           ip.NetworkType,
		Description:           ip.Description,
		SubnetGroupIdentifier: ip.SubnetGroupIdentifier,
		PubliclyAccessible:    ip.PubliclyAccessible,
	}
}

// --- CreateMigrationProject handler ---

type createMigrationProjectInput struct {
	MigrationProjectName *string    `json:"MigrationProjectName"`
	Description          *string    `json:"Description"`
	Tags                 []tagEntry `json:"Tags"`
}

type migrationProjectJSON struct {
	MigrationProjectName       string `json:"MigrationProjectName"`
	MigrationProjectArn        string `json:"MigrationProjectArn"`
	MigrationProjectIdentifier string `json:"MigrationProjectIdentifier"`
	Description                string `json:"Description,omitempty"`
}

type createMigrationProjectOutput struct {
	MigrationProject migrationProjectJSON `json:"MigrationProject"`
}

func mpToJSON(mp *MigrationProject) migrationProjectJSON {
	return migrationProjectJSON{
		MigrationProjectName:       mp.MigrationProjectName,
		MigrationProjectArn:        mp.MigrationProjectArn,
		MigrationProjectIdentifier: mp.MigrationProjectIdentifier,
		Description:                mp.Description,
	}
}

func (h *Handler) handleCreateMigrationProject(
	ctx context.Context, in *createMigrationProjectInput,
) (*createMigrationProjectOutput, error) {
	name := ptrStr(in.MigrationProjectName)
	if name == "" {
		return nil, fmt.Errorf("%w: MigrationProjectName is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	mp, err := h.Backend.CreateMigrationProject(ctx, name, ptrStr(in.Description), kv)
	if err != nil {
		return nil, err
	}

	return &createMigrationProjectOutput{MigrationProject: mpToJSON(mp)}, nil
}

// --- CreateReplicationConfig handler ---

type createReplicationConfigInput struct {
	ReplicationConfigIdentifier *string    `json:"ReplicationConfigIdentifier"`
	ReplicationType             *string    `json:"ReplicationType"`
	SourceEndpointArn           *string    `json:"SourceEndpointArn"`
	TargetEndpointArn           *string    `json:"TargetEndpointArn"`
	Tags                        []tagEntry `json:"Tags"`
}

type replicationConfigJSON struct {
	ReplicationConfigIdentifier string `json:"ReplicationConfigIdentifier"`
	ReplicationConfigArn        string `json:"ReplicationConfigArn"`
	ReplicationType             string `json:"ReplicationType"`
	SourceEndpointArn           string `json:"SourceEndpointArn"`
	TargetEndpointArn           string `json:"TargetEndpointArn"`
}

type createReplicationConfigOutput struct {
	ReplicationConfig replicationConfigJSON `json:"ReplicationConfig"`
}

func rcToJSON(rc *ReplicationConfig) replicationConfigJSON {
	return replicationConfigJSON{
		ReplicationConfigIdentifier: rc.ReplicationConfigIdentifier,
		ReplicationConfigArn:        rc.ReplicationConfigArn,
		ReplicationType:             rc.ReplicationType,
		SourceEndpointArn:           rc.SourceEndpointArn,
		TargetEndpointArn:           rc.TargetEndpointArn,
	}
}

func (h *Handler) handleCreateReplicationConfig(
	ctx context.Context, in *createReplicationConfigInput,
) (*createReplicationConfigOutput, error) {
	identifier := ptrStr(in.ReplicationConfigIdentifier)
	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationConfigIdentifier is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	rc, err := h.Backend.CreateReplicationConfig(
		ctx,
		identifier,
		ptrStr(in.ReplicationType),
		ptrStr(in.SourceEndpointArn),
		ptrStr(in.TargetEndpointArn),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationConfigOutput{ReplicationConfig: rcToJSON(rc)}, nil
}

// --- CreateReplicationSubnetGroup handler ---

type createReplicationSubnetGroupInput struct {
	ReplicationSubnetGroupIdentifier  *string    `json:"ReplicationSubnetGroupIdentifier"`
	ReplicationSubnetGroupDescription *string    `json:"ReplicationSubnetGroupDescription"`
	SubnetIDs                         []string   `json:"SubnetIds"`
	Tags                              []tagEntry `json:"Tags"`
}

type replicationSubnetGroupFullJSON struct {
	ReplicationSubnetGroupIdentifier  string `json:"ReplicationSubnetGroupIdentifier"`
	ReplicationSubnetGroupArn         string `json:"ReplicationSubnetGroupArn"`
	ReplicationSubnetGroupDescription string `json:"ReplicationSubnetGroupDescription"`
	VpcID                             string `json:"VpcId"`
}

type createReplicationSubnetGroupOutput struct {
	ReplicationSubnetGroup replicationSubnetGroupFullJSON `json:"ReplicationSubnetGroup"`
}

func rsgToJSON(sg *ReplicationSubnetGroup) replicationSubnetGroupFullJSON {
	return replicationSubnetGroupFullJSON{
		ReplicationSubnetGroupIdentifier:  sg.ReplicationSubnetGroupIdentifier,
		ReplicationSubnetGroupArn:         sg.ReplicationSubnetGroupArn,
		ReplicationSubnetGroupDescription: sg.ReplicationSubnetGroupDescription,
		VpcID:                             sg.VpcID,
	}
}

func (h *Handler) handleCreateReplicationSubnetGroup(
	ctx context.Context, in *createReplicationSubnetGroupInput,
) (*createReplicationSubnetGroupOutput, error) {
	identifier := ptrStr(in.ReplicationSubnetGroupIdentifier)
	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationSubnetGroupIdentifier is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	sg, err := h.Backend.CreateReplicationSubnetGroup(
		ctx,
		identifier,
		ptrStr(in.ReplicationSubnetGroupDescription),
		"",
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationSubnetGroupOutput{ReplicationSubnetGroup: rsgToJSON(sg)}, nil
}

// --- DeleteCertificate handler ---

type deleteCertificateInput struct {
	CertificateArn *string `json:"CertificateArn"`
}

type certificateJSON struct {
	CertificateIdentifier string `json:"CertificateIdentifier"`
	CertificateArn        string `json:"CertificateArn"`
}

type deleteCertificateOutput struct {
	Certificate certificateJSON `json:"Certificate"`
}

func certToJSON(c *Certificate) certificateJSON {
	return certificateJSON{
		CertificateIdentifier: c.CertificateIdentifier,
		CertificateArn:        c.CertificateArn,
	}
}

func (h *Handler) handleDeleteCertificate(
	ctx context.Context, in *deleteCertificateInput,
) (*deleteCertificateOutput, error) {
	cert, err := h.Backend.DeleteCertificate(ctx, ptrStr(in.CertificateArn))
	if err != nil {
		return nil, err
	}

	return &deleteCertificateOutput{Certificate: certToJSON(cert)}, nil
}

// --- DeleteConnection handler ---

type deleteConnectionInput struct {
	EndpointArn            *string `json:"EndpointArn"`
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type deleteConnectionOutput struct {
	Connection map[string]any `json:"Connection"`
}

func (h *Handler) handleDeleteConnection(
	_ context.Context, _ *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	return nil, fmt.Errorf("%w: connection not found", ErrNotFound)
}

// --- DeleteDataMigration handler ---

type deleteDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
}

type deleteDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleDeleteDataMigration(
	ctx context.Context, in *deleteDataMigrationInput,
) (*deleteDataMigrationOutput, error) {
	dm, err := h.Backend.DeleteDataMigration(ctx, ptrStr(in.DataMigrationIdentifier))
	if err != nil {
		return nil, err
	}

	return &deleteDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

// --- DeleteDataProvider handler ---

type deleteDataProviderInput struct {
	DataProviderArn *string `json:"DataProviderArn"`
}

type deleteDataProviderOutput struct {
	DataProvider dataProviderJSON `json:"DataProvider"`
}

func (h *Handler) handleDeleteDataProvider(
	ctx context.Context, in *deleteDataProviderInput,
) (*deleteDataProviderOutput, error) {
	dp, err := h.Backend.DeleteDataProvider(ctx, ptrStr(in.DataProviderArn))
	if err != nil {
		return nil, err
	}

	return &deleteDataProviderOutput{DataProvider: dpToJSON(dp)}, nil
}

// --- DeleteEventSubscription handler ---

type deleteEventSubscriptionInput struct {
	SubscriptionName *string `json:"SubscriptionName"`
}

type deleteEventSubscriptionOutput struct {
	EventSubscription eventSubscriptionJSON `json:"EventSubscription"`
}

func (h *Handler) handleDeleteEventSubscription(
	ctx context.Context, in *deleteEventSubscriptionInput,
) (*deleteEventSubscriptionOutput, error) {
	es, err := h.Backend.DeleteEventSubscription(ctx, ptrStr(in.SubscriptionName))
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionOutput{EventSubscription: esToJSON(es)}, nil
}

// --- DeleteFleetAdvisorCollector handler ---

type deleteFleetAdvisorCollectorInput struct {
	CollectorReferencedID *string `json:"CollectorReferencedId"`
}

type deleteFleetAdvisorCollectorOutput struct{}

func (h *Handler) handleDeleteFleetAdvisorCollector(
	ctx context.Context, in *deleteFleetAdvisorCollectorInput,
) (*deleteFleetAdvisorCollectorOutput, error) {
	if err := h.Backend.DeleteFleetAdvisorCollector(ctx, ptrStr(in.CollectorReferencedID)); err != nil {
		return nil, err
	}

	return &deleteFleetAdvisorCollectorOutput{}, nil
}

// --- DeleteFleetAdvisorDatabases handler ---

type deleteFleetAdvisorDatabasesInput struct {
	DatabaseIDs []string `json:"DatabaseIds"`
}

type deleteFleetAdvisorDatabasesOutput struct {
	DatabaseIDs []string `json:"DatabaseIds"`
}

func (h *Handler) handleDeleteFleetAdvisorDatabases(
	_ context.Context, _ *deleteFleetAdvisorDatabasesInput,
) (*deleteFleetAdvisorDatabasesOutput, error) {
	return &deleteFleetAdvisorDatabasesOutput{DatabaseIDs: []string{}}, nil
}

// --- DeleteInstanceProfile handler ---

type deleteInstanceProfileInput struct {
	InstanceProfileArn *string `json:"InstanceProfileArn"`
}

type deleteInstanceProfileOutput struct {
	InstanceProfile instanceProfileJSON `json:"InstanceProfile"`
}

func (h *Handler) handleDeleteInstanceProfile(
	ctx context.Context, in *deleteInstanceProfileInput,
) (*deleteInstanceProfileOutput, error) {
	// We need to get the profile before deleting it for the response.
	arnOrName := ptrStr(in.InstanceProfileArn)

	profiles, _ := h.Backend.DescribeInstanceProfiles(ctx)
	var found *InstanceProfile
	for _, p := range profiles {
		if p.InstanceProfileArn == arnOrName || p.InstanceProfileName == arnOrName {
			found = p

			break
		}
	}

	if err := h.Backend.DeleteInstanceProfile(ctx, arnOrName); err != nil {
		return nil, err
	}

	if found == nil {
		return &deleteInstanceProfileOutput{}, nil
	}

	return &deleteInstanceProfileOutput{InstanceProfile: ipToJSON(found)}, nil
}

// --- DeleteMigrationProject handler ---

type deleteMigrationProjectInput struct {
	MigrationProjectArn *string `json:"MigrationProjectArn"`
}

type deleteMigrationProjectOutput struct {
	MigrationProject migrationProjectJSON `json:"MigrationProject"`
}

func (h *Handler) handleDeleteMigrationProject(
	ctx context.Context, in *deleteMigrationProjectInput,
) (*deleteMigrationProjectOutput, error) {
	nameOrArn := ptrStr(in.MigrationProjectArn)

	projects, _ := h.Backend.DescribeMigrationProjects(ctx)
	var found *MigrationProject
	for _, p := range projects {
		if p.MigrationProjectArn == nameOrArn || p.MigrationProjectName == nameOrArn {
			found = p

			break
		}
	}

	if err := h.Backend.DeleteMigrationProject(ctx, nameOrArn); err != nil {
		return nil, err
	}

	if found == nil {
		return &deleteMigrationProjectOutput{}, nil
	}

	return &deleteMigrationProjectOutput{MigrationProject: mpToJSON(found)}, nil
}

// --- DeleteReplicationConfig handler ---

type deleteReplicationConfigInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
}

type deleteReplicationConfigOutput struct {
	ReplicationConfig replicationConfigJSON `json:"ReplicationConfig"`
}

func (h *Handler) handleDeleteReplicationConfig(
	ctx context.Context, in *deleteReplicationConfigInput,
) (*deleteReplicationConfigOutput, error) {
	identifierOrArn := ptrStr(in.ReplicationConfigArn)

	configs, _ := h.Backend.DescribeReplicationConfigs(ctx)
	var found *ReplicationConfig
	for _, rc := range configs {
		if rc.ReplicationConfigArn == identifierOrArn ||
			rc.ReplicationConfigIdentifier == identifierOrArn {
			found = rc

			break
		}
	}

	if err := h.Backend.DeleteReplicationConfig(ctx, identifierOrArn); err != nil {
		return nil, err
	}

	if found == nil {
		return &deleteReplicationConfigOutput{}, nil
	}

	return &deleteReplicationConfigOutput{ReplicationConfig: rcToJSON(found)}, nil
}

// --- DeleteReplicationSubnetGroup handler ---

type deleteReplicationSubnetGroupInput struct {
	ReplicationSubnetGroupIdentifier *string `json:"ReplicationSubnetGroupIdentifier"`
}

type deleteReplicationSubnetGroupOutput struct{}

func (h *Handler) handleDeleteReplicationSubnetGroup(
	ctx context.Context, in *deleteReplicationSubnetGroupInput,
) (*deleteReplicationSubnetGroupOutput, error) {
	if err := h.Backend.DeleteReplicationSubnetGroup(ctx, ptrStr(in.ReplicationSubnetGroupIdentifier)); err != nil {
		return nil, err
	}

	return &deleteReplicationSubnetGroupOutput{}, nil
}

// --- DeleteReplicationTaskAssessmentRun handler ---

type deleteReplicationTaskAssessmentRunInput struct {
	ReplicationTaskAssessmentRunArn *string `json:"ReplicationTaskAssessmentRunArn"`
}

type deleteReplicationTaskAssessmentRunOutput struct {
	ReplicationTaskAssessmentRun map[string]any `json:"ReplicationTaskAssessmentRun"`
}

func (h *Handler) handleDeleteReplicationTaskAssessmentRun(
	_ context.Context, in *deleteReplicationTaskAssessmentRunInput,
) (*deleteReplicationTaskAssessmentRunOutput, error) {
	return nil, fmt.Errorf(
		"%w: assessment run %s not found",
		ErrNotFound,
		ptrStr(in.ReplicationTaskAssessmentRunArn),
	)
}

// --- DescribeAccountAttributes handler ---

type describeAccountAttributesInput struct{}

type accountQuotaJSON struct {
	AccountQuotaName string  `json:"AccountQuotaName"`
	Used             float64 `json:"Used"`
	Max              float64 `json:"Max"`
}

type describeAccountAttributesOutput struct {
	UniqueAccountIdentifier string             `json:"UniqueAccountIdentifier"`
	AccountQuotas           []accountQuotaJSON `json:"AccountQuotas"`
}

const (
	quotaMaxReplicationInstances = float64(60)
	quotaMaxAllocatedStorage     = float64(6000)
	quotaMaxEndpoints            = float64(1000)
	quotaMaxReplicationTasks     = float64(200)
	quotaStoragePerInstance      = int64(50)
)

func (h *Handler) handleDescribeAccountAttributes(
	ctx context.Context, _ *describeAccountAttributesInput,
) (*describeAccountAttributesOutput, error) {
	riCount := int64(len(h.Backend.mustDescribeReplicationInstances(ctx)))
	epCount := int64(len(h.Backend.mustDescribeEndpoints(ctx)))
	taskCount := int64(len(h.Backend.mustDescribeReplicationTasks(ctx)))

	return &describeAccountAttributesOutput{
		UniqueAccountIdentifier: h.Backend.AccountID(),
		AccountQuotas: []accountQuotaJSON{
			{AccountQuotaName: "ReplicationInstances", Used: float64(riCount), Max: quotaMaxReplicationInstances},
			{
				AccountQuotaName: "AllocatedStorage",
				Used:             float64(riCount * quotaStoragePerInstance),
				Max:              quotaMaxAllocatedStorage,
			},
			{AccountQuotaName: "Endpoints", Used: float64(epCount), Max: quotaMaxEndpoints},
			{AccountQuotaName: "ReplicationTasks", Used: float64(taskCount), Max: quotaMaxReplicationTasks},
		},
	}, nil
}

// --- DescribeApplicableIndividualAssessments handler ---

type describeApplicableIndividualAssessmentsInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeApplicableIndividualAssessmentsOutput struct {
	Marker                    *string  `json:"Marker,omitempty"`
	IndividualAssessmentNames []string `json:"IndividualAssessmentNames"`
}

func (h *Handler) handleDescribeApplicableIndividualAssessments(
	_ context.Context, _ *describeApplicableIndividualAssessmentsInput,
) (*describeApplicableIndividualAssessmentsOutput, error) {
	return &describeApplicableIndividualAssessmentsOutput{
		IndividualAssessmentNames: []string{},
	}, nil
}

// --- DescribeCertificates handler ---

type describeCertificatesInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeCertificatesOutput struct {
	Marker       *string           `json:"Marker,omitempty"`
	Certificates []certificateJSON `json:"Certificates"`
}

func (h *Handler) handleDescribeCertificates(
	ctx context.Context, in *describeCertificatesInput,
) (*describeCertificatesOutput, error) {
	list, err := h.Backend.DescribeCertificates(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].CertificateIdentifier < list[j].CertificateIdentifier
	})

	all := make([]certificateJSON, 0, len(list))
	for _, cert := range list {
		all = append(all, certToJSON(cert))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeCertificatesOutput{Certificates: data, Marker: nextMarker}, nil
}

// --- DescribeConnections handler ---

type describeConnectionsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeConnectionsOutput struct {
	Marker      *string          `json:"Marker,omitempty"`
	Connections []connectionJSON `json:"Connections"`
}

func (h *Handler) handleDescribeConnections(
	ctx context.Context, in *describeConnectionsInput,
) (*describeConnectionsOutput, error) {
	riArn := extractFilterValue(in.Filters, "replication-instance-id")
	epArn := extractFilterValue(in.Filters, "endpoint-id")

	list, err := h.Backend.DescribeConnections(ctx, riArn, epArn)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].EndpointIdentifier < list[j].EndpointIdentifier
	})

	all := make([]connectionJSON, 0, len(list))
	for _, conn := range list {
		all = append(all, connToJSON(conn))
	}

	return &describeConnectionsOutput{Connections: all}, nil
}

// --- DescribeConversionConfiguration handler ---

type describeConversionConfigurationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
}

type describeConversionConfigurationOutput struct {
	MigrationProjectIdentifier string `json:"MigrationProjectIdentifier"`
	ConversionConfiguration    string `json:"ConversionConfiguration"`
}

func (h *Handler) handleDescribeConversionConfiguration(
	_ context.Context, in *describeConversionConfigurationInput,
) (*describeConversionConfigurationOutput, error) {
	return &describeConversionConfigurationOutput{
		MigrationProjectIdentifier: ptrStr(in.MigrationProjectIdentifier),
		ConversionConfiguration:    "{}",
	}, nil
}

// --- DescribeDataMigrations handler ---

type describeDataMigrationsInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
	Marker                  *string `json:"Marker"`
	MaxRecords              *int32  `json:"MaxRecords"`
}

type describeDataMigrationsOutput struct {
	Marker         *string             `json:"Marker,omitempty"`
	DataMigrations []dataMigrationJSON `json:"DataMigrations"`
}

func (h *Handler) handleDescribeDataMigrations(
	ctx context.Context, in *describeDataMigrationsInput,
) (*describeDataMigrationsOutput, error) {
	list, err := h.Backend.DescribeDataMigrations(ctx, ptrStr(in.DataMigrationIdentifier))
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].DataMigrationName < list[j].DataMigrationName
	})

	all := make([]dataMigrationJSON, 0, len(list))
	for _, dm := range list {
		all = append(all, dmToJSON(dm))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeDataMigrationsOutput{DataMigrations: data, Marker: nextMarker}, nil
}

// --- DescribeDataProviders handler ---

type describeDataProvidersInput struct {
	DataProviderIdentifier *string `json:"DataProviderIdentifier"`
	Marker                 *string `json:"Marker"`
	MaxRecords             *int32  `json:"MaxRecords"`
}

type describeDataProvidersOutput struct {
	Marker        *string            `json:"Marker,omitempty"`
	DataProviders []dataProviderJSON `json:"DataProviders"`
}

func (h *Handler) handleDescribeDataProviders(
	ctx context.Context, in *describeDataProvidersInput,
) (*describeDataProvidersOutput, error) {
	list, err := h.Backend.DescribeDataProviders(ctx, ptrStr(in.DataProviderIdentifier))
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].DataProviderName < list[j].DataProviderName
	})

	all := make([]dataProviderJSON, 0, len(list))
	for _, dp := range list {
		all = append(all, dpToJSON(dp))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeDataProvidersOutput{DataProviders: data, Marker: nextMarker}, nil
}

// --- DescribeEndpointSettings handler ---

type describeEndpointSettingsInput struct {
	EngineName *string `json:"EngineName"`
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeEndpointSettingsOutput struct {
	Marker           *string          `json:"Marker,omitempty"`
	EndpointSettings []map[string]any `json:"EndpointSettings"`
}

func (h *Handler) handleDescribeEndpointSettings(
	_ context.Context, _ *describeEndpointSettingsInput,
) (*describeEndpointSettingsOutput, error) {
	return &describeEndpointSettingsOutput{EndpointSettings: []map[string]any{}}, nil
}

// --- DescribeEndpointTypes handler ---

type describeEndpointTypesInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type supportedEndpointTypeJSON struct {
	EngineName        string `json:"EngineName"`
	EndpointType      string `json:"EndpointType"`
	EngineDisplayName string `json:"EngineDisplayName"`
	SupportsCDC       bool   `json:"SupportsCDC"`
}

type describeEndpointTypesOutput struct {
	Marker                 *string                     `json:"Marker,omitempty"`
	SupportedEndpointTypes []supportedEndpointTypeJSON `json:"SupportedEndpointTypes"`
}

func (h *Handler) handleDescribeEndpointTypes(
	_ context.Context, _ *describeEndpointTypesInput,
) (*describeEndpointTypesOutput, error) {
	engines := []string{
		"mysql",
		"postgres",
		"oracle",
		"sqlserver",
		"mongodb",
		"s3",
		"kinesis",
		"kafka",
		"aurora",
		"aurora-postgresql",
		"mariadb",
		"redshift",
		"dynamodb",
	}
	const endpointDirections = 2 // SOURCE and TARGET
	types := make([]supportedEndpointTypeJSON, 0, len(engines)*endpointDirections)

	for _, e := range engines {
		types = append(
			types,
			supportedEndpointTypeJSON{
				EngineName:        e,
				SupportsCDC:       true,
				EndpointType:      "SOURCE",
				EngineDisplayName: e,
			},
			supportedEndpointTypeJSON{
				EngineName:        e,
				SupportsCDC:       true,
				EndpointType:      "TARGET",
				EngineDisplayName: e,
			},
		)
	}

	return &describeEndpointTypesOutput{SupportedEndpointTypes: types}, nil
}

// --- DescribeEngineVersions handler ---

type describeEngineVersionsInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type engineVersionJSON struct {
	Version          string `json:"Version"`
	Lifecycle        string `json:"Lifecycle"`
	ReleaseNotes     string `json:"ReleaseNotes,omitempty"`
	LaunchDate       string `json:"LaunchDate,omitempty"`
	AutoUpgradeDate  string `json:"AutoUpgradeDate,omitempty"`
	DeprecationDate  string `json:"DeprecationDate,omitempty"`
	ForceUpgradeDate string `json:"ForceUpgradeDate,omitempty"`
}

type describeEngineVersionsOutput struct {
	Marker         *string             `json:"Marker,omitempty"`
	EngineVersions []engineVersionJSON `json:"EngineVersions"`
}

func dmsEngineVersionList() []engineVersionJSON {
	return []engineVersionJSON{
		{Version: defaultEngineVersion, Lifecycle: statusAvailable, LaunchDate: "2023-11-01"},
		{Version: "3.5.2", Lifecycle: statusAvailable, LaunchDate: "2023-07-01"},
		{Version: "3.5.1", Lifecycle: statusAvailable, LaunchDate: "2023-03-01"},
		{Version: "3.4.7", Lifecycle: statusAvailable, LaunchDate: "2022-11-01"},
		{Version: "3.4.6", Lifecycle: statusAvailable, LaunchDate: "2022-07-01"},
		{Version: "3.4.5", Lifecycle: "deprecated", LaunchDate: "2022-03-01", DeprecationDate: "2023-06-01"},
	}
}

func (h *Handler) handleDescribeEngineVersions(
	_ context.Context, in *describeEngineVersionsInput,
) (*describeEngineVersionsOutput, error) {
	data, nextMarker := dmsPaginate(dmsEngineVersionList(), in.Marker, in.MaxRecords)

	return &describeEngineVersionsOutput{EngineVersions: data, Marker: nextMarker}, nil
}

// --- DescribeEventCategories handler ---

type describeEventCategoriesInput struct {
	SourceType *string `json:"SourceType"`
}

type describeEventCategoriesOutput struct {
	EventCategoryGroupList []map[string]any `json:"EventCategoryGroupList"`
}

type eventCategoryGroupJSON struct {
	SourceType      string   `json:"SourceType"`
	EventCategories []string `json:"EventCategories"`
}

func dmsEventCategoryGroupList() []eventCategoryGroupJSON {
	return []eventCategoryGroupJSON{
		{
			SourceType: "replication-instance",
			EventCategories: []string{
				"low storage",
				"configuration change",
				"maintenance",
				"deletion",
				"creation",
				"failover",
				"failure",
			},
		},
		{
			SourceType: "replication-task",
			EventCategories: []string{
				"state change",
				"configuration change",
				"deletion",
				"creation",
				"failure",
			},
		},
	}
}

func (h *Handler) handleDescribeEventCategories(
	_ context.Context, in *describeEventCategoriesInput,
) (*describeEventCategoriesOutput, error) {
	sourceType := ptrStr(in.SourceType)
	groups := dmsEventCategoryGroupList()
	result := make([]eventCategoryGroupJSON, 0, len(groups))

	for _, group := range groups {
		if sourceType == "" || group.SourceType == sourceType {
			result = append(result, group)
		}
	}

	out := make([]map[string]any, 0, len(result))
	for _, g := range result {
		out = append(out, map[string]any{
			"SourceType":      g.SourceType,
			"EventCategories": g.EventCategories,
		})
	}

	return &describeEventCategoriesOutput{EventCategoryGroupList: out}, nil
}

// --- DescribeEventSubscriptions handler ---

type describeEventSubscriptionsInput struct {
	SubscriptionName *string `json:"SubscriptionName"`
	Marker           *string `json:"Marker"`
	MaxRecords       *int32  `json:"MaxRecords"`
}

type describeEventSubscriptionsOutput struct {
	Marker                 *string                 `json:"Marker,omitempty"`
	EventSubscriptionsList []eventSubscriptionJSON `json:"EventSubscriptionsList"`
}

func (h *Handler) handleDescribeEventSubscriptions(
	ctx context.Context, in *describeEventSubscriptionsInput,
) (*describeEventSubscriptionsOutput, error) {
	list, err := h.Backend.DescribeEventSubscriptions(ctx, ptrStr(in.SubscriptionName))
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].SubscriptionName < list[j].SubscriptionName
	})

	all := make([]eventSubscriptionJSON, 0, len(list))
	for _, es := range list {
		all = append(all, esToJSON(es))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeEventSubscriptionsOutput{EventSubscriptionsList: data, Marker: nextMarker}, nil
}

// --- DescribeEvents handler ---

type describeEventsInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeEventsOutput struct {
	Marker *string          `json:"Marker,omitempty"`
	Events []map[string]any `json:"Events"`
}

func (h *Handler) handleDescribeEvents(
	_ context.Context, _ *describeEventsInput,
) (*describeEventsOutput, error) {
	return &describeEventsOutput{Events: []map[string]any{}}, nil
}

// --- DescribeExtensionPackAssociations handler ---

type describeExtensionPackAssociationsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeExtensionPackAssociationsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeExtensionPackAssociations(
	_ context.Context, _ *describeExtensionPackAssociationsInput,
) (*describeExtensionPackAssociationsOutput, error) {
	return &describeExtensionPackAssociationsOutput{Requests: []map[string]any{}}, nil
}

// --- DescribeFleetAdvisorCollectors handler ---

type describeFleetAdvisorCollectorsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type fleetAdvisorCollectorJSON struct {
	CollectorName         string `json:"CollectorName"`
	CollectorReferencedID string `json:"CollectorReferencedId"`
	CollectorVersion      string `json:"CollectorVersion"`
	Description           string `json:"Description,omitempty"`
	ServiceAccessRoleArn  string `json:"ServiceAccessRoleArn"`
	S3BucketName          string `json:"S3BucketName"`
	CollectorHealthCheck  string `json:"CollectorHealthCheck"`
}

type describeFleetAdvisorCollectorsOutput struct {
	Collectors []fleetAdvisorCollectorJSON `json:"Collectors"`
}

func (h *Handler) handleDescribeFleetAdvisorCollectors(
	ctx context.Context, _ *describeFleetAdvisorCollectorsInput,
) (*describeFleetAdvisorCollectorsOutput, error) {
	list, err := h.Backend.DescribeFleetAdvisorCollectors(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]fleetAdvisorCollectorJSON, 0, len(list))
	for _, col := range list {
		result = append(result, fleetAdvisorCollectorJSON{
			CollectorName:         col.CollectorName,
			CollectorReferencedID: col.CollectorReferencedID,
			CollectorVersion:      col.CollectorVersion,
			Description:           col.Description,
			ServiceAccessRoleArn:  col.ServiceAccessRoleArn,
			S3BucketName:          col.S3BucketName,
			CollectorHealthCheck:  col.CollectorHealthCheck,
		})
	}

	return &describeFleetAdvisorCollectorsOutput{Collectors: result}, nil
}

// --- DescribeFleetAdvisorDatabases handler ---

type describeFleetAdvisorDatabasesInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeFleetAdvisorDatabasesOutput struct {
	NextToken *string          `json:"NextToken,omitempty"`
	Databases []map[string]any `json:"Databases"`
}

func (h *Handler) handleDescribeFleetAdvisorDatabases(
	_ context.Context, _ *describeFleetAdvisorDatabasesInput,
) (*describeFleetAdvisorDatabasesOutput, error) {
	return &describeFleetAdvisorDatabasesOutput{Databases: []map[string]any{}}, nil
}

// --- DescribeFleetAdvisorLsaAnalysis handler ---

type describeFleetAdvisorLsaAnalysisInput struct {
	NextToken  *string `json:"NextToken"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeFleetAdvisorLsaAnalysisOutput struct {
	NextToken *string          `json:"NextToken,omitempty"`
	Analysis  []map[string]any `json:"Analysis"`
}

func (h *Handler) handleDescribeFleetAdvisorLsaAnalysis(
	_ context.Context, _ *describeFleetAdvisorLsaAnalysisInput,
) (*describeFleetAdvisorLsaAnalysisOutput, error) {
	return &describeFleetAdvisorLsaAnalysisOutput{Analysis: []map[string]any{}}, nil
}

// --- DescribeFleetAdvisorSchemaObjectSummary handler ---

type describeFleetAdvisorSchemaObjectSummaryInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeFleetAdvisorSchemaObjectSummaryOutput struct {
	NextToken                 *string          `json:"NextToken,omitempty"`
	FleetAdvisorSchemaObjects []map[string]any `json:"FleetAdvisorSchemaObjects"`
}

func (h *Handler) handleDescribeFleetAdvisorSchemaObjectSummary(
	_ context.Context, _ *describeFleetAdvisorSchemaObjectSummaryInput,
) (*describeFleetAdvisorSchemaObjectSummaryOutput, error) {
	return &describeFleetAdvisorSchemaObjectSummaryOutput{
		FleetAdvisorSchemaObjects: []map[string]any{},
	}, nil
}

// --- DescribeFleetAdvisorSchemas handler ---

type describeFleetAdvisorSchemasInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeFleetAdvisorSchemasOutput struct {
	NextToken           *string          `json:"NextToken,omitempty"`
	FleetAdvisorSchemas []map[string]any `json:"FleetAdvisorSchemas"`
}

func (h *Handler) handleDescribeFleetAdvisorSchemas(
	_ context.Context, _ *describeFleetAdvisorSchemasInput,
) (*describeFleetAdvisorSchemasOutput, error) {
	return &describeFleetAdvisorSchemasOutput{FleetAdvisorSchemas: []map[string]any{}}, nil
}

// --- DescribeInstanceProfiles handler ---

type describeInstanceProfilesInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeInstanceProfilesOutput struct {
	Marker           *string               `json:"Marker,omitempty"`
	InstanceProfiles []instanceProfileJSON `json:"InstanceProfiles"`
}

func (h *Handler) handleDescribeInstanceProfiles(
	ctx context.Context, in *describeInstanceProfilesInput,
) (*describeInstanceProfilesOutput, error) {
	list, err := h.Backend.DescribeInstanceProfiles(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].InstanceProfileName < list[j].InstanceProfileName
	})

	all := make([]instanceProfileJSON, 0, len(list))
	for _, ip := range list {
		all = append(all, ipToJSON(ip))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeInstanceProfilesOutput{InstanceProfiles: data, Marker: nextMarker}, nil
}

// --- DescribeMetadataModel handler ---

type describeMetadataModelInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type describeMetadataModelOutput struct{}

func (h *Handler) handleDescribeMetadataModel(
	_ context.Context, _ *describeMetadataModelInput,
) (*describeMetadataModelOutput, error) {
	return &describeMetadataModelOutput{}, nil
}

// --- DescribeMetadataModelAssessments handler ---

type describeMetadataModelAssessmentsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelAssessmentsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelAssessments(
	_ context.Context, _ *describeMetadataModelAssessmentsInput,
) (*describeMetadataModelAssessmentsOutput, error) {
	return &describeMetadataModelAssessmentsOutput{Requests: []map[string]any{}}, nil
}

// --- DescribeMetadataModelChildren handler ---

type describeMetadataModelChildrenInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelChildrenOutput struct {
	Marker *string          `json:"Marker,omitempty"`
	Items  []map[string]any `json:"Items"`
}

func (h *Handler) handleDescribeMetadataModelChildren(
	_ context.Context, _ *describeMetadataModelChildrenInput,
) (*describeMetadataModelChildrenOutput, error) {
	return &describeMetadataModelChildrenOutput{Items: []map[string]any{}}, nil
}

// --- DescribeMetadataModelConversions handler ---

type describeMetadataModelConversionsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelConversionsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelConversions(
	_ context.Context, _ *describeMetadataModelConversionsInput,
) (*describeMetadataModelConversionsOutput, error) {
	return &describeMetadataModelConversionsOutput{Requests: []map[string]any{}}, nil
}

// --- DescribeMetadataModelCreations handler ---

type describeMetadataModelCreationsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelCreationsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelCreations(
	_ context.Context, _ *describeMetadataModelCreationsInput,
) (*describeMetadataModelCreationsOutput, error) {
	return &describeMetadataModelCreationsOutput{Requests: []map[string]any{}}, nil
}

// --- DescribeMetadataModelExportsAsScript handler ---

type describeMetadataModelExportsAsScriptInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelExportsAsScriptOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelExportsAsScript(
	_ context.Context, _ *describeMetadataModelExportsAsScriptInput,
) (*describeMetadataModelExportsAsScriptOutput, error) {
	return &describeMetadataModelExportsAsScriptOutput{Requests: []map[string]any{}}, nil
}

// --- DescribeMetadataModelExportsToTarget handler ---

type describeMetadataModelExportsToTargetInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelExportsToTargetOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelExportsToTarget(
	_ context.Context, _ *describeMetadataModelExportsToTargetInput,
) (*describeMetadataModelExportsToTargetOutput, error) {
	return &describeMetadataModelExportsToTargetOutput{Requests: []map[string]any{}}, nil
}

// --- DescribeMetadataModelImports handler ---

type describeMetadataModelImportsInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	Marker                     *string `json:"Marker"`
	MaxRecords                 *int32  `json:"MaxRecords"`
}

type describeMetadataModelImportsOutput struct {
	Marker   *string          `json:"Marker,omitempty"`
	Requests []map[string]any `json:"Requests"`
}

func (h *Handler) handleDescribeMetadataModelImports(
	_ context.Context, _ *describeMetadataModelImportsInput,
) (*describeMetadataModelImportsOutput, error) {
	return &describeMetadataModelImportsOutput{Requests: []map[string]any{}}, nil
}

// --- DescribeMigrationProjects handler ---

type describeMigrationProjectsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeMigrationProjectsOutput struct {
	Marker            *string                `json:"Marker,omitempty"`
	MigrationProjects []migrationProjectJSON `json:"MigrationProjects"`
}

func (h *Handler) handleDescribeMigrationProjects(
	ctx context.Context, in *describeMigrationProjectsInput,
) (*describeMigrationProjectsOutput, error) {
	list, err := h.Backend.DescribeMigrationProjects(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].MigrationProjectName < list[j].MigrationProjectName
	})

	all := make([]migrationProjectJSON, 0, len(list))
	for _, mp := range list {
		all = append(all, mpToJSON(mp))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeMigrationProjectsOutput{MigrationProjects: data, Marker: nextMarker}, nil
}

// --- DescribeOrderableReplicationInstances handler ---

type describeOrderableReplicationInstancesInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type orderableReplicationInstanceJSON struct {
	ReplicationInstanceClass string `json:"ReplicationInstanceClass"`
	StorageType              string `json:"StorageType"`
	ReleaseStatus            string `json:"ReleaseStatus"`
	DefaultAllocatedStorage  int32  `json:"DefaultAllocatedStorage"`
	MinAllocatedStorage      int32  `json:"MinAllocatedStorage"`
	MaxAllocatedStorage      int32  `json:"MaxAllocatedStorage"`
}

type describeOrderableReplicationInstancesOutput struct {
	Marker                        *string                            `json:"Marker,omitempty"`
	OrderableReplicationInstances []orderableReplicationInstanceJSON `json:"OrderableReplicationInstances"`
}

type orderableInstanceSpec struct {
	class          string
	defaultStorage int32
	minStorage     int32
	maxStorage     int32
}

const (
	t3DefaultStorage   int32 = 50
	t3MaxStorage       int32 = 200
	c5r5DefaultStorage int32 = 100
	c5r5MaxStorage     int32 = 1024
	minStorageAll      int32 = 5
)

func dmsOrderableInstanceList() []orderableInstanceSpec {
	return []orderableInstanceSpec{
		{class: "dms.t3.micro", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{class: "dms.t3.small", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{class: "dms.t3.medium", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{class: "dms.t3.large", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{
			class:          "dms.c5.large",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.c5.xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.c5.2xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.c5.4xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.large",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.2xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.4xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.8xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.16xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
	}
}

func (h *Handler) handleDescribeOrderableReplicationInstances(
	_ context.Context, in *describeOrderableReplicationInstancesInput,
) (*describeOrderableReplicationInstancesOutput, error) {
	specs := dmsOrderableInstanceList()
	all := make([]orderableReplicationInstanceJSON, 0, len(specs))

	for _, spec := range specs {
		all = append(all, orderableReplicationInstanceJSON{
			ReplicationInstanceClass: spec.class,
			StorageType:              "gp2",
			DefaultAllocatedStorage:  spec.defaultStorage,
			MinAllocatedStorage:      spec.minStorage,
			MaxAllocatedStorage:      spec.maxStorage,
			ReleaseStatus:            "GA",
		})
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeOrderableReplicationInstancesOutput{
		OrderableReplicationInstances: data,
		Marker:                        nextMarker,
	}, nil
}

// --- DescribePendingMaintenanceActions handler ---

type describePendingMaintenanceActionsInput struct {
	ReplicationInstanceArn *string       `json:"ReplicationInstanceArn"`
	Marker                 *string       `json:"Marker"`
	MaxRecords             *int32        `json:"MaxRecords"`
	Filters                []filterEntry `json:"Filters"`
}

type describePendingMaintenanceActionsOutput struct {
	Marker                    *string          `json:"Marker,omitempty"`
	PendingMaintenanceActions []map[string]any `json:"PendingMaintenanceActions"`
}

func (h *Handler) handleDescribePendingMaintenanceActions(
	_ context.Context, _ *describePendingMaintenanceActionsInput,
) (*describePendingMaintenanceActionsOutput, error) {
	return &describePendingMaintenanceActionsOutput{
		PendingMaintenanceActions: []map[string]any{},
	}, nil
}

// --- DescribeRecommendationLimitations handler ---

type describeRecommendationLimitationsInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeRecommendationLimitationsOutput struct {
	NextToken   *string          `json:"NextToken,omitempty"`
	Limitations []map[string]any `json:"Limitations"`
}

func (h *Handler) handleDescribeRecommendationLimitations(
	_ context.Context, _ *describeRecommendationLimitationsInput,
) (*describeRecommendationLimitationsOutput, error) {
	return &describeRecommendationLimitationsOutput{Limitations: []map[string]any{}}, nil
}

// --- DescribeRecommendations handler ---

type describeRecommendationsInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeRecommendationsOutput struct {
	NextToken       *string          `json:"NextToken,omitempty"`
	Recommendations []map[string]any `json:"Recommendations"`
}

func (h *Handler) handleDescribeRecommendations(
	_ context.Context, _ *describeRecommendationsInput,
) (*describeRecommendationsOutput, error) {
	return &describeRecommendationsOutput{Recommendations: []map[string]any{}}, nil
}

// --- DescribeRefreshSchemasStatus handler ---

type describeRefreshSchemasStatusInput struct {
	EndpointArn *string `json:"EndpointArn"`
}

type refreshSchemasStatusJSON struct {
	Status string `json:"Status"`
}

type describeRefreshSchemasStatusOutput struct {
	RefreshSchemasStatus refreshSchemasStatusJSON `json:"RefreshSchemasStatus"`
}

func (h *Handler) handleDescribeRefreshSchemasStatus(
	_ context.Context, _ *describeRefreshSchemasStatusInput,
) (*describeRefreshSchemasStatusOutput, error) {
	return &describeRefreshSchemasStatusOutput{
		RefreshSchemasStatus: refreshSchemasStatusJSON{Status: "successful"},
	}, nil
}

// --- DescribeReplicationConfigs handler ---

type describeReplicationConfigsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationConfigsOutput struct {
	Marker             *string                 `json:"Marker,omitempty"`
	ReplicationConfigs []replicationConfigJSON `json:"ReplicationConfigs"`
}

func (h *Handler) handleDescribeReplicationConfigs(
	ctx context.Context, in *describeReplicationConfigsInput,
) (*describeReplicationConfigsOutput, error) {
	list, err := h.Backend.DescribeReplicationConfigs(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationConfigIdentifier < list[j].ReplicationConfigIdentifier
	})

	all := make([]replicationConfigJSON, 0, len(list))
	for _, rc := range list {
		all = append(all, rcToJSON(rc))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationConfigsOutput{ReplicationConfigs: data, Marker: nextMarker}, nil
}

// --- DescribeReplicationInstanceTaskLogs handler ---

type describeReplicationInstanceTaskLogsInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	Marker                 *string `json:"Marker"`
	MaxRecords             *int32  `json:"MaxRecords"`
}

type describeReplicationInstanceTaskLogsOutput struct {
	Marker                      *string          `json:"Marker,omitempty"`
	ReplicationInstanceArn      string           `json:"ReplicationInstanceArn,omitempty"`
	ReplicationInstanceTaskLogs []map[string]any `json:"ReplicationInstanceTaskLogs"`
}

func (h *Handler) handleDescribeReplicationInstanceTaskLogs(
	_ context.Context, in *describeReplicationInstanceTaskLogsInput,
) (*describeReplicationInstanceTaskLogsOutput, error) {
	return &describeReplicationInstanceTaskLogsOutput{
		ReplicationInstanceArn:      ptrStr(in.ReplicationInstanceArn),
		ReplicationInstanceTaskLogs: []map[string]any{},
	}, nil
}

// --- DescribeReplicationSubnetGroups handler ---

type describeReplicationSubnetGroupsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationSubnetGroupsOutput struct {
	Marker                  *string                          `json:"Marker,omitempty"`
	ReplicationSubnetGroups []replicationSubnetGroupFullJSON `json:"ReplicationSubnetGroups"`
}

func (h *Handler) handleDescribeReplicationSubnetGroups(
	ctx context.Context, in *describeReplicationSubnetGroupsInput,
) (*describeReplicationSubnetGroupsOutput, error) {
	list, err := h.Backend.DescribeReplicationSubnetGroups(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationSubnetGroupIdentifier < list[j].ReplicationSubnetGroupIdentifier
	})

	all := make([]replicationSubnetGroupFullJSON, 0, len(list))
	for _, sg := range list {
		all = append(all, rsgToJSON(sg))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationSubnetGroupsOutput{
		ReplicationSubnetGroups: data,
		Marker:                  nextMarker,
	}, nil
}

// tableStatisticJSON represents a single table statistic entry.
type tableStatisticJSON struct {
	SchemaName                   string `json:"SchemaName"`
	TableName                    string `json:"TableName"`
	ValidationState              string `json:"ValidationState"`
	TableState                   string `json:"TableState"`
	FullLoadRows                 int64  `json:"FullLoadRows"`
	FullLoadCondtnlChkFailedRows int64  `json:"FullLoadCondtnlChkFailedRows"`
	FullLoadErrorRows            int64  `json:"FullLoadErrorRows"`
	ValidationPendingRecords     int64  `json:"ValidationPendingRecords"`
	ValidationFailedRecords      int64  `json:"ValidationFailedRecords"`
	ValidationSuspendedRecords   int64  `json:"ValidationSuspendedRecords"`
}

// tableMappingRule is used to parse table mappings JSON.
type tableMappingRule struct {
	SchemaName string `json:"schema-name"`
	TableName  string `json:"table-name"`
	RuleType   string `json:"rule-type"`
}

// tableMappings is the top-level table mappings structure.
type tableMappingsDoc struct {
	Rules []tableMappingRule `json:"rules"`
}

// buildTableStatistics parses TableMappings JSON and returns mock table statistics.
func buildTableStatistics(tableMappings string) []tableStatisticJSON {
	if tableMappings == "" {
		return []tableStatisticJSON{}
	}

	var doc tableMappingsDoc
	if err := json.Unmarshal([]byte(tableMappings), &doc); err != nil {
		return []tableStatisticJSON{}
	}

	stats := make([]tableStatisticJSON, 0, len(doc.Rules))
	for _, rule := range doc.Rules {
		if rule.RuleType == "selection" || rule.RuleType == "" {
			stats = append(stats, tableStatisticJSON{
				SchemaName:      rule.SchemaName,
				TableName:       rule.TableName,
				ValidationState: "Not enabled",
				TableState:      "Not started",
			})
		}
	}

	return stats
}

// --- DescribeReplicationTableStatistics handler ---

type describeReplicationTableStatisticsInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
	Marker             *string `json:"Marker"`
	MaxRecords         *int32  `json:"MaxRecords"`
}

type describeReplicationTableStatisticsOutput struct {
	ReplicationTaskArn string               `json:"ReplicationTaskArn,omitempty"`
	Marker             *string              `json:"Marker,omitempty"`
	TableStatistics    []tableStatisticJSON `json:"TableStatistics"`
}

func (h *Handler) handleDescribeReplicationTableStatistics(
	ctx context.Context, in *describeReplicationTableStatisticsInput,
) (*describeReplicationTableStatisticsOutput, error) {
	taskArn := ptrStr(in.ReplicationTaskArn)

	tasks, err := h.Backend.DescribeReplicationTasks(ctx, taskArn)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return &describeReplicationTableStatisticsOutput{
			ReplicationTaskArn: taskArn,
			TableStatistics:    []tableStatisticJSON{},
		}, nil
	}

	stats := buildTableStatistics(tasks[0].TableMappings)

	return &describeReplicationTableStatisticsOutput{
		ReplicationTaskArn: taskArn,
		TableStatistics:    stats,
	}, nil
}

// --- DescribeReplicationTaskAssessmentResults handler ---

type describeReplicationTaskAssessmentResultsInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
	Marker             *string `json:"Marker"`
	MaxRecords         *int32  `json:"MaxRecords"`
}

type describeReplicationTaskAssessmentResultsOutput struct {
	Marker                           *string          `json:"Marker,omitempty"`
	ReplicationTaskAssessmentResults []map[string]any `json:"ReplicationTaskAssessmentResults"`
}

func (h *Handler) handleDescribeReplicationTaskAssessmentResults(
	_ context.Context, _ *describeReplicationTaskAssessmentResultsInput,
) (*describeReplicationTaskAssessmentResultsOutput, error) {
	return &describeReplicationTaskAssessmentResultsOutput{
		ReplicationTaskAssessmentResults: []map[string]any{},
	}, nil
}

// --- DescribeReplicationTaskAssessmentRuns handler ---

type describeReplicationTaskAssessmentRunsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationTaskAssessmentRunsOutput struct {
	Marker                        *string          `json:"Marker,omitempty"`
	ReplicationTaskAssessmentRuns []map[string]any `json:"ReplicationTaskAssessmentRuns"`
}

func (h *Handler) handleDescribeReplicationTaskAssessmentRuns(
	_ context.Context, _ *describeReplicationTaskAssessmentRunsInput,
) (*describeReplicationTaskAssessmentRunsOutput, error) {
	return &describeReplicationTaskAssessmentRunsOutput{
		ReplicationTaskAssessmentRuns: []map[string]any{},
	}, nil
}

// --- DescribeReplicationTaskIndividualAssessments handler ---

type describeReplicationTaskIndividualAssessmentsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationTaskIndividualAssessmentsOutput struct {
	Marker                               *string          `json:"Marker,omitempty"`
	ReplicationTaskIndividualAssessments []map[string]any `json:"ReplicationTaskIndividualAssessments"`
}

func (h *Handler) handleDescribeReplicationTaskIndividualAssessments(
	_ context.Context, _ *describeReplicationTaskIndividualAssessmentsInput,
) (*describeReplicationTaskIndividualAssessmentsOutput, error) {
	return &describeReplicationTaskIndividualAssessmentsOutput{
		ReplicationTaskIndividualAssessments: []map[string]any{},
	}, nil
}

// --- DescribeReplications handler ---

type describeReplicationsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationsOutput struct {
	Marker       *string          `json:"Marker,omitempty"`
	Replications []map[string]any `json:"Replications"`
}

func (h *Handler) handleDescribeReplications(
	_ context.Context, _ *describeReplicationsInput,
) (*describeReplicationsOutput, error) {
	return &describeReplicationsOutput{Replications: []map[string]any{}}, nil
}

// --- DescribeSchemas handler ---

type describeSchemasInput struct {
	EndpointArn            *string `json:"EndpointArn"`
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	Marker                 *string `json:"Marker"`
	MaxRecords             *int32  `json:"MaxRecords"`
}

type describeSchemasOutput struct {
	Marker  *string  `json:"Marker,omitempty"`
	Schemas []string `json:"Schemas"`
}

func (h *Handler) handleDescribeSchemas(
	_ context.Context, _ *describeSchemasInput,
) (*describeSchemasOutput, error) {
	return &describeSchemasOutput{Schemas: []string{}}, nil
}

// --- DescribeTableStatistics handler ---

type describeTableStatisticsInput struct {
	ReplicationTaskArn *string       `json:"ReplicationTaskArn"`
	Marker             *string       `json:"Marker"`
	MaxRecords         *int32        `json:"MaxRecords"`
	Filters            []filterEntry `json:"Filters"`
}

type describeTableStatisticsOutput struct {
	ReplicationTaskArn string               `json:"ReplicationTaskArn,omitempty"`
	Marker             *string              `json:"Marker,omitempty"`
	TableStatistics    []tableStatisticJSON `json:"TableStatistics"`
}

func (h *Handler) handleDescribeTableStatistics(
	ctx context.Context, in *describeTableStatisticsInput,
) (*describeTableStatisticsOutput, error) {
	taskArn := ptrStr(in.ReplicationTaskArn)

	tasks, err := h.Backend.DescribeReplicationTasks(ctx, taskArn)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return &describeTableStatisticsOutput{
			ReplicationTaskArn: taskArn,
			TableStatistics:    []tableStatisticJSON{},
		}, nil
	}

	stats := buildTableStatistics(tasks[0].TableMappings)

	return &describeTableStatisticsOutput{
		ReplicationTaskArn: taskArn,
		TableStatistics:    stats,
	}, nil
}

// --- ExportMetadataModelAssessment handler ---

type exportMetadataModelAssessmentInput struct {
	MigrationProjectIdentifier *string  `json:"MigrationProjectIdentifier"`
	SelectionRules             *string  `json:"SelectionRules"`
	FileName                   *string  `json:"FileName"`
	AssessmentReportTypes      []string `json:"AssessmentReportTypes"`
}

type s3ObjectKeyJSON struct {
	S3ObjectKey string `json:"S3ObjectKey"`
}

type exportMetadataModelAssessmentOutput struct {
	PdfReport s3ObjectKeyJSON `json:"PdfReport"`
	CsvReport s3ObjectKeyJSON `json:"CsvReport"`
}

func (h *Handler) handleExportMetadataModelAssessment(
	_ context.Context, _ *exportMetadataModelAssessmentInput,
) (*exportMetadataModelAssessmentOutput, error) {
	return &exportMetadataModelAssessmentOutput{
		PdfReport: s3ObjectKeyJSON{S3ObjectKey: ""},
		CsvReport: s3ObjectKeyJSON{S3ObjectKey: ""},
	}, nil
}

// --- GetTargetSelectionRules handler ---

type getTargetSelectionRulesInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type getTargetSelectionRulesOutput struct {
	Marker *string          `json:"Marker,omitempty"`
	Rules  []map[string]any `json:"Rules"`
}

func (h *Handler) handleGetTargetSelectionRules(
	_ context.Context, _ *getTargetSelectionRulesInput,
) (*getTargetSelectionRulesOutput, error) {
	return &getTargetSelectionRulesOutput{Rules: []map[string]any{}}, nil
}

// --- ImportCertificate handler ---

type importCertificateInput struct {
	CertificateIdentifier *string `json:"CertificateIdentifier"`
	CertificatePem        *string `json:"CertificatePem"`
}

type importCertificateOutput struct {
	Certificate certificateJSON `json:"Certificate"`
}

func (h *Handler) handleImportCertificate(
	ctx context.Context, in *importCertificateInput,
) (*importCertificateOutput, error) {
	identifier := ptrStr(in.CertificateIdentifier)
	if identifier == "" {
		return nil, fmt.Errorf("%w: CertificateIdentifier is required", ErrValidation)
	}

	cert, err := h.Backend.ImportCertificate(ctx, identifier, ptrStr(in.CertificatePem))
	if err != nil {
		return nil, err
	}

	return &importCertificateOutput{Certificate: certToJSON(cert)}, nil
}

// --- ModifyConversionConfiguration handler ---

type modifyConversionConfigurationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	ConversionConfiguration    *string `json:"ConversionConfiguration"`
}

type modifyConversionConfigurationOutput struct {
	MigrationProjectIdentifier string `json:"MigrationProjectIdentifier"`
	ConversionConfiguration    string `json:"ConversionConfiguration"`
}

func (h *Handler) handleModifyConversionConfiguration(
	_ context.Context, in *modifyConversionConfigurationInput,
) (*modifyConversionConfigurationOutput, error) {
	return &modifyConversionConfigurationOutput{
		MigrationProjectIdentifier: ptrStr(in.MigrationProjectIdentifier),
		ConversionConfiguration:    ptrStr(in.ConversionConfiguration),
	}, nil
}

// --- ModifyDataMigration handler ---

type modifyDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
	DataMigrationType       *string `json:"DataMigrationType"`
	ServiceAccessRoleArn    *string `json:"ServiceAccessRoleArn"`
	NumberOfJobs            *int32  `json:"NumberOfJobs"`
}

type modifyDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleModifyDataMigration(
	ctx context.Context, in *modifyDataMigrationInput,
) (*modifyDataMigrationOutput, error) {
	dm, err := h.Backend.ModifyDataMigration(
		ctx,
		ptrStr(in.DataMigrationIdentifier),
		ptrStr(in.DataMigrationType),
		ptrStr(in.ServiceAccessRoleArn),
		in.NumberOfJobs,
	)
	if err != nil {
		return nil, err
	}

	return &modifyDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

// --- ModifyDataProvider handler ---

type modifyDataProviderInput struct {
	DataProviderArn *string `json:"DataProviderArn"`
	Engine          *string `json:"Engine"`
	Description     *string `json:"Description"`
}

type modifyDataProviderOutput struct {
	DataProvider dataProviderJSON `json:"DataProvider"`
}

func (h *Handler) handleModifyDataProvider(
	ctx context.Context, in *modifyDataProviderInput,
) (*modifyDataProviderOutput, error) {
	dp, err := h.Backend.ModifyDataProvider(
		ctx,
		ptrStr(in.DataProviderArn),
		ptrStr(in.Engine),
		ptrStr(in.Description),
	)
	if err != nil {
		return nil, err
	}

	return &modifyDataProviderOutput{DataProvider: dpToJSON(dp)}, nil
}

// --- ModifyEndpoint handler ---

type modifyEndpointInput struct {
	EndpointArn  *string `json:"EndpointArn"`
	ServerName   *string `json:"ServerName"`
	DatabaseName *string `json:"DatabaseName"`
	Username     *string `json:"Username"`
	Port         *int32  `json:"Port"`
}

type modifyEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleModifyEndpoint(
	ctx context.Context, in *modifyEndpointInput,
) (*modifyEndpointOutput, error) {
	ep, err := h.Backend.ModifyEndpoint(
		ctx,
		ptrStr(in.EndpointArn),
		ptrStr(in.ServerName),
		ptrStr(in.DatabaseName),
		ptrStr(in.Username),
		ptrInt32(in.Port),
	)
	if err != nil {
		return nil, err
	}

	return &modifyEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

// --- ModifyEventSubscription handler ---

type modifyEventSubscriptionInput struct {
	SubscriptionName *string `json:"SubscriptionName"`
	Enabled          *bool   `json:"Enabled"`
}

type modifyEventSubscriptionOutput struct {
	EventSubscription eventSubscriptionJSON `json:"EventSubscription"`
}

func (h *Handler) handleModifyEventSubscription(
	ctx context.Context, in *modifyEventSubscriptionInput,
) (*modifyEventSubscriptionOutput, error) {
	es, err := h.Backend.ModifyEventSubscription(ctx, ptrStr(in.SubscriptionName), in.Enabled)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionOutput{EventSubscription: esToJSON(es)}, nil
}

// --- ModifyInstanceProfile handler ---

type modifyInstanceProfileInput struct {
	InstanceProfileArn *string `json:"InstanceProfileArn"`
	AvailabilityZone   *string `json:"AvailabilityZone"`
	Description        *string `json:"Description"`
	NetworkType        *string `json:"NetworkType"`
}

type modifyInstanceProfileOutput struct {
	InstanceProfile instanceProfileJSON `json:"InstanceProfile"`
}

func (h *Handler) handleModifyInstanceProfile(
	ctx context.Context, in *modifyInstanceProfileInput,
) (*modifyInstanceProfileOutput, error) {
	ip, err := h.Backend.ModifyInstanceProfile(
		ctx,
		ptrStr(in.InstanceProfileArn),
		ptrStr(in.AvailabilityZone),
		ptrStr(in.Description),
		ptrStr(in.NetworkType),
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceProfileOutput{InstanceProfile: ipToJSON(ip)}, nil
}

// --- ModifyMigrationProject handler ---

type modifyMigrationProjectInput struct {
	MigrationProjectArn *string `json:"MigrationProjectArn"`
	Description         *string `json:"Description"`
}

type modifyMigrationProjectOutput struct {
	MigrationProject migrationProjectJSON `json:"MigrationProject"`
}

func (h *Handler) handleModifyMigrationProject(
	ctx context.Context, in *modifyMigrationProjectInput,
) (*modifyMigrationProjectOutput, error) {
	nameOrArn := ptrStr(in.MigrationProjectArn)

	projects, _ := h.Backend.DescribeMigrationProjects(ctx)
	for _, mp := range projects {
		if mp.MigrationProjectArn == nameOrArn || mp.MigrationProjectName == nameOrArn {
			return &modifyMigrationProjectOutput{MigrationProject: mpToJSON(mp)}, nil
		}
	}

	return nil, fmt.Errorf("%w: migration project %s not found", ErrNotFound, nameOrArn)
}

// --- ModifyReplicationConfig handler ---

type modifyReplicationConfigInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
	ReplicationType      *string `json:"ReplicationType"`
}

type modifyReplicationConfigOutput struct {
	ReplicationConfig replicationConfigJSON `json:"ReplicationConfig"`
}

func (h *Handler) handleModifyReplicationConfig(
	ctx context.Context, in *modifyReplicationConfigInput,
) (*modifyReplicationConfigOutput, error) {
	identifierOrArn := ptrStr(in.ReplicationConfigArn)

	configs, _ := h.Backend.DescribeReplicationConfigs(ctx)
	for _, rc := range configs {
		if rc.ReplicationConfigArn == identifierOrArn ||
			rc.ReplicationConfigIdentifier == identifierOrArn {
			return &modifyReplicationConfigOutput{ReplicationConfig: rcToJSON(rc)}, nil
		}
	}

	return nil, fmt.Errorf("%w: replication config %s not found", ErrNotFound, identifierOrArn)
}

// --- ModifyReplicationInstance handler ---

type modifyReplicationInstanceInput struct {
	ReplicationInstanceArn   *string `json:"ReplicationInstanceArn"`
	ReplicationInstanceClass *string `json:"ReplicationInstanceClass"`
	EngineVersion            *string `json:"EngineVersion"`
	MultiAZ                  *bool   `json:"MultiAZ"`
	AutoMinorVersionUpgrade  *bool   `json:"AutoMinorVersionUpgrade"`
	AllocatedStorage         *int32  `json:"AllocatedStorage"`
}

type modifyReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleModifyReplicationInstance(
	ctx context.Context, in *modifyReplicationInstanceInput,
) (*modifyReplicationInstanceOutput, error) {
	ri, err := h.Backend.ModifyReplicationInstance(
		ctx,
		ptrStr(in.ReplicationInstanceArn),
		ptrStr(in.ReplicationInstanceClass),
		ptrStr(in.EngineVersion),
		in.MultiAZ,
		in.AutoMinorVersionUpgrade,
		in.AllocatedStorage,
	)
	if err != nil {
		return nil, err
	}

	return &modifyReplicationInstanceOutput{ReplicationInstance: riToJSON(ri)}, nil
}

// --- ModifyReplicationSubnetGroup handler ---

type modifyReplicationSubnetGroupInput struct {
	ReplicationSubnetGroupIdentifier  *string  `json:"ReplicationSubnetGroupIdentifier"`
	ReplicationSubnetGroupDescription *string  `json:"ReplicationSubnetGroupDescription"`
	SubnetIDs                         []string `json:"SubnetIds"`
}

type modifyReplicationSubnetGroupOutput struct {
	ReplicationSubnetGroup replicationSubnetGroupFullJSON `json:"ReplicationSubnetGroup"`
}

func (h *Handler) handleModifyReplicationSubnetGroup(
	ctx context.Context, in *modifyReplicationSubnetGroupInput,
) (*modifyReplicationSubnetGroupOutput, error) {
	identifier := ptrStr(in.ReplicationSubnetGroupIdentifier)

	groups, _ := h.Backend.DescribeReplicationSubnetGroups(ctx)
	for _, sg := range groups {
		if sg.ReplicationSubnetGroupIdentifier == identifier ||
			sg.ReplicationSubnetGroupArn == identifier {
			return &modifyReplicationSubnetGroupOutput{ReplicationSubnetGroup: rsgToJSON(sg)}, nil
		}
	}

	return nil, fmt.Errorf("%w: replication subnet group %s not found", ErrNotFound, identifier)
}

// --- ModifyReplicationTask handler ---

type modifyReplicationTaskInput struct {
	ReplicationTaskArn      *string `json:"ReplicationTaskArn"`
	MigrationType           *string `json:"MigrationType"`
	TableMappings           *string `json:"TableMappings"`
	ReplicationTaskSettings *string `json:"ReplicationTaskSettings"`
}

type modifyReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleModifyReplicationTask(
	ctx context.Context, in *modifyReplicationTaskInput,
) (*modifyReplicationTaskOutput, error) {
	rt, err := h.Backend.ModifyReplicationTask(
		ctx,
		ptrStr(in.ReplicationTaskArn),
		ptrStr(in.MigrationType),
		ptrStr(in.TableMappings),
		ptrStr(in.ReplicationTaskSettings),
	)
	if err != nil {
		return nil, err
	}

	return &modifyReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

// --- MoveReplicationTask handler ---

type moveReplicationTaskInput struct {
	ReplicationTaskArn           *string `json:"ReplicationTaskArn"`
	TargetReplicationInstanceArn *string `json:"TargetReplicationInstanceArn"`
}

type moveReplicationTaskOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleMoveReplicationTask(
	ctx context.Context, in *moveReplicationTaskInput,
) (*moveReplicationTaskOutput, error) {
	rt, err := h.Backend.MoveReplicationTask(
		ctx,
		ptrStr(in.ReplicationTaskArn),
		ptrStr(in.TargetReplicationInstanceArn),
	)
	if err != nil {
		return nil, err
	}

	return &moveReplicationTaskOutput{ReplicationTask: rtToJSON(rt)}, nil
}

// --- RebootReplicationInstance handler ---

type rebootReplicationInstanceInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	ForcePlannedFailover   *bool   `json:"ForcePlannedFailover"`
	ForceFailover          *bool   `json:"ForceFailover"`
}

type rebootReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleRebootReplicationInstance(
	ctx context.Context, in *rebootReplicationInstanceInput,
) (*rebootReplicationInstanceOutput, error) {
	ri, err := h.Backend.RebootReplicationInstance(ctx, ptrStr(in.ReplicationInstanceArn))
	if err != nil {
		return nil, err
	}

	return &rebootReplicationInstanceOutput{ReplicationInstance: riToJSON(ri)}, nil
}

// --- RefreshSchemas handler ---

type refreshSchemasInput struct {
	EndpointArn            *string `json:"EndpointArn"`
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type refreshSchemasOutput struct {
	RefreshSchemasStatus refreshSchemasStatusJSON `json:"RefreshSchemasStatus"`
}

func (h *Handler) handleRefreshSchemas(
	_ context.Context, _ *refreshSchemasInput,
) (*refreshSchemasOutput, error) {
	return &refreshSchemasOutput{
		RefreshSchemasStatus: refreshSchemasStatusJSON{Status: "refreshing"},
	}, nil
}

// --- ReloadReplicationTables handler ---

type reloadReplicationTablesInput struct {
	ReplicationTaskArn *string          `json:"ReplicationTaskArn"`
	ReloadOption       *string          `json:"ReloadOption"`
	TablesToReload     []map[string]any `json:"TablesToReload"`
}

type reloadReplicationTablesOutput struct {
	ReplicationTaskArn string `json:"ReplicationTaskArn"`
}

func (h *Handler) handleReloadReplicationTables(
	_ context.Context, in *reloadReplicationTablesInput,
) (*reloadReplicationTablesOutput, error) {
	return &reloadReplicationTablesOutput{ReplicationTaskArn: ptrStr(in.ReplicationTaskArn)}, nil
}

// --- ReloadTables handler ---

type reloadTablesInput struct {
	ReplicationTaskArn *string          `json:"ReplicationTaskArn"`
	ReloadOption       *string          `json:"ReloadOption"`
	TablesToReload     []map[string]any `json:"TablesToReload"`
}

type reloadTablesOutput struct {
	ReplicationTaskArn string `json:"ReplicationTaskArn"`
}

func (h *Handler) handleReloadTables(
	_ context.Context, in *reloadTablesInput,
) (*reloadTablesOutput, error) {
	return &reloadTablesOutput{ReplicationTaskArn: ptrStr(in.ReplicationTaskArn)}, nil
}

// --- RemoveTagsFromResource handler ---

type removeTagsFromResourceInput struct {
	ResourceArn *string  `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type removeTagsFromResourceOutput struct{}

func (h *Handler) handleRemoveTagsFromResource(
	ctx context.Context, in *removeTagsFromResourceInput,
) (*removeTagsFromResourceOutput, error) {
	if err := h.Backend.RemoveTagsFromResource(ctx, ptrStr(in.ResourceArn), in.TagKeys); err != nil {
		return nil, err
	}

	return &removeTagsFromResourceOutput{}, nil
}

// --- RunFleetAdvisorLsaAnalysis handler ---

type runFleetAdvisorLsaAnalysisInput struct{}

type runFleetAdvisorLsaAnalysisOutput struct {
	LsaAnalysisID string `json:"LsaAnalysisId"`
	Status        string `json:"Status"`
}

func (h *Handler) handleRunFleetAdvisorLsaAnalysis(
	_ context.Context, _ *runFleetAdvisorLsaAnalysisInput,
) (*runFleetAdvisorLsaAnalysisOutput, error) {
	return &runFleetAdvisorLsaAnalysisOutput{
		LsaAnalysisID: uuid.NewString(),
		Status:        "RUNNING",
	}, nil
}

// --- StartDataMigration handler ---

type startDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
	StartType               *string `json:"StartType"`
}

type startDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleStartDataMigration(
	ctx context.Context, in *startDataMigrationInput,
) (*startDataMigrationOutput, error) {
	dm, err := h.Backend.StartDataMigration(ctx, ptrStr(in.DataMigrationIdentifier))
	if err != nil {
		return nil, err
	}

	return &startDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

// --- StartExtensionPackAssociation handler ---

type startExtensionPackAssociationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
}

type startExtensionPackAssociationOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartExtensionPackAssociation(
	_ context.Context, _ *startExtensionPackAssociationInput,
) (*startExtensionPackAssociationOutput, error) {
	return &startExtensionPackAssociationOutput{RequestIdentifier: uuid.NewString()}, nil
}

// --- StartMetadataModelAssessment handler ---

type startMetadataModelAssessmentInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type startMetadataModelAssessmentOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelAssessment(
	_ context.Context, _ *startMetadataModelAssessmentInput,
) (*startMetadataModelAssessmentOutput, error) {
	return &startMetadataModelAssessmentOutput{RequestIdentifier: uuid.NewString()}, nil
}

// --- StartMetadataModelConversion handler ---

type startMetadataModelConversionInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type startMetadataModelConversionOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelConversion(
	_ context.Context, _ *startMetadataModelConversionInput,
) (*startMetadataModelConversionOutput, error) {
	return &startMetadataModelConversionOutput{RequestIdentifier: uuid.NewString()}, nil
}

// --- StartMetadataModelCreation handler ---

type startMetadataModelCreationInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
}

type startMetadataModelCreationOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelCreation(
	_ context.Context, _ *startMetadataModelCreationInput,
) (*startMetadataModelCreationOutput, error) {
	return &startMetadataModelCreationOutput{RequestIdentifier: uuid.NewString()}, nil
}

// --- StartMetadataModelExportAsScript handler ---

type startMetadataModelExportAsScriptInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
	FileName                   *string `json:"FileName"`
	Origin                     *string `json:"Origin"`
}

type startMetadataModelExportAsScriptOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelExportAsScript(
	_ context.Context, _ *startMetadataModelExportAsScriptInput,
) (*startMetadataModelExportAsScriptOutput, error) {
	return &startMetadataModelExportAsScriptOutput{RequestIdentifier: uuid.NewString()}, nil
}

// --- StartMetadataModelExportToTarget handler ---

type startMetadataModelExportToTargetInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
	OverwriteExtensionPack     *bool   `json:"OverwriteExtensionPack"`
}

type startMetadataModelExportToTargetOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelExportToTarget(
	_ context.Context, _ *startMetadataModelExportToTargetInput,
) (*startMetadataModelExportToTargetOutput, error) {
	return &startMetadataModelExportToTargetOutput{RequestIdentifier: uuid.NewString()}, nil
}

// --- StartMetadataModelImport handler ---

type startMetadataModelImportInput struct {
	MigrationProjectIdentifier *string `json:"MigrationProjectIdentifier"`
	SelectionRules             *string `json:"SelectionRules"`
	Origin                     *string `json:"Origin"`
	Refresh                    *bool   `json:"Refresh"`
}

type startMetadataModelImportOutput struct {
	RequestIdentifier string `json:"RequestIdentifier"`
}

func (h *Handler) handleStartMetadataModelImport(
	_ context.Context, _ *startMetadataModelImportInput,
) (*startMetadataModelImportOutput, error) {
	return &startMetadataModelImportOutput{RequestIdentifier: uuid.NewString()}, nil
}

// --- StartRecommendations handler ---

type startRecommendationsInput struct {
	DatabaseID *string        `json:"DatabaseId"`
	Settings   map[string]any `json:"Settings"`
}

type startRecommendationsOutput struct{}

func (h *Handler) handleStartRecommendations(
	_ context.Context, _ *startRecommendationsInput,
) (*startRecommendationsOutput, error) {
	return &startRecommendationsOutput{}, nil
}

// --- StartReplication handler ---

type startReplicationInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
	StartReplicationType *string `json:"StartReplicationType"`
}

type startReplicationOutput struct{}

func (h *Handler) handleStartReplication(
	_ context.Context, _ *startReplicationInput,
) (*startReplicationOutput, error) {
	return &startReplicationOutput{}, nil
}

// --- StartReplicationTaskAssessment handler ---

type startReplicationTaskAssessmentInput struct {
	ReplicationTaskArn *string `json:"ReplicationTaskArn"`
}

type startReplicationTaskAssessmentOutput struct {
	ReplicationTask replicationTaskJSON `json:"ReplicationTask"`
}

func (h *Handler) handleStartReplicationTaskAssessment(
	_ context.Context, in *startReplicationTaskAssessmentInput,
) (*startReplicationTaskAssessmentOutput, error) {
	taskArn := ptrStr(in.ReplicationTaskArn)

	return &startReplicationTaskAssessmentOutput{
		ReplicationTask: replicationTaskJSON{
			ReplicationTaskArn: taskArn,
			Status:             "test-failed",
		},
	}, nil
}

// --- StartReplicationTaskAssessmentRun handler ---

type startReplicationTaskAssessmentRunInput struct {
	ReplicationTaskArn   *string  `json:"ReplicationTaskArn"`
	ServiceAccessRoleArn *string  `json:"ServiceAccessRoleArn"`
	ResultLocationBucket *string  `json:"ResultLocationBucket"`
	AssessmentRunName    *string  `json:"AssessmentRunName"`
	IncludeOnly          []string `json:"IncludeOnly"`
	Exclude              []string `json:"Exclude"`
}

type startReplicationTaskAssessmentRunOutput struct {
	ReplicationTaskAssessmentRun map[string]any `json:"ReplicationTaskAssessmentRun"`
}

func (h *Handler) handleStartReplicationTaskAssessmentRun(
	_ context.Context, _ *startReplicationTaskAssessmentRunInput,
) (*startReplicationTaskAssessmentRunOutput, error) {
	return &startReplicationTaskAssessmentRunOutput{
		ReplicationTaskAssessmentRun: map[string]any{
			"ReplicationTaskAssessmentRunArn": uuid.NewString(),
			"Status":                          statusRunning,
		},
	}, nil
}

// --- StopDataMigration handler ---

type stopDataMigrationInput struct {
	DataMigrationIdentifier *string `json:"DataMigrationIdentifier"`
}

type stopDataMigrationOutput struct {
	DataMigration dataMigrationJSON `json:"DataMigration"`
}

func (h *Handler) handleStopDataMigration(
	ctx context.Context, in *stopDataMigrationInput,
) (*stopDataMigrationOutput, error) {
	dm, err := h.Backend.StopDataMigration(ctx, ptrStr(in.DataMigrationIdentifier))
	if err != nil {
		return nil, err
	}

	return &stopDataMigrationOutput{DataMigration: dmToJSON(dm)}, nil
}

// --- StopReplication handler ---

type stopReplicationInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
}

type stopReplicationOutput struct{}

func (h *Handler) handleStopReplication(
	_ context.Context, _ *stopReplicationInput,
) (*stopReplicationOutput, error) {
	return &stopReplicationOutput{}, nil
}

// --- TestConnection handler ---

type testConnectionInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	EndpointArn            *string `json:"EndpointArn"`
}

type connectionJSON struct {
	ReplicationInstanceArn        string `json:"ReplicationInstanceArn,omitempty"`
	ReplicationInstanceIdentifier string `json:"ReplicationInstanceIdentifier,omitempty"`
	EndpointArn                   string `json:"EndpointArn,omitempty"`
	EndpointIdentifier            string `json:"EndpointIdentifier,omitempty"`
	Status                        string `json:"Status"`
	LastFailureMessage            string `json:"LastFailureMessage,omitempty"`
}

type testConnectionOutput struct {
	Connection connectionJSON `json:"Connection"`
}

func connToJSON(c *Connection) connectionJSON {
	return connectionJSON{
		ReplicationInstanceArn:        c.ReplicationInstanceArn,
		ReplicationInstanceIdentifier: c.ReplicationInstanceIdentifier,
		EndpointArn:                   c.EndpointArn,
		EndpointIdentifier:            c.EndpointIdentifier,
		Status:                        c.Status,
		LastFailureMessage:            c.LastFailureMessage,
	}
}

func (h *Handler) handleTestConnection(
	ctx context.Context, in *testConnectionInput,
) (*testConnectionOutput, error) {
	conn, err := h.Backend.TestConnection(
		ctx,
		ptrStr(in.ReplicationInstanceArn),
		ptrStr(in.EndpointArn),
	)
	if err != nil {
		return nil, err
	}

	return &testConnectionOutput{Connection: connToJSON(conn)}, nil
}

// --- UpdateSubscriptionsToEventBridge handler ---

type updateSubscriptionsToEventBridgeInput struct {
	ForceMove *bool `json:"ForceMove"`
}

type updateSubscriptionsToEventBridgeOutput struct {
	Applied bool `json:"Applied"`
}

func (h *Handler) handleUpdateSubscriptionsToEventBridge(
	_ context.Context, _ *updateSubscriptionsToEventBridgeInput,
) (*updateSubscriptionsToEventBridgeOutput, error) {
	return &updateSubscriptionsToEventBridgeOutput{Applied: false}, nil
}
