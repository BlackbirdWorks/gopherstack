package inspector2

import (
	"net/http"
	"sync"

	"github.com/labstack/echo/v5"
)

// This file holds the routing dispatch table for every Inspector2 operation
// beyond the core set handled directly in handler.go's handleREST switch.
// Each operation/path constant is declared alongside its handler in the
// matching handler_<family>.go file; this table only wires path -> operation
// -> handler func. classifyExtendedPath used to be a flat, exhaustive
// path-to-op switch across every family (62 ops); it's now a lookup table
// (see onceExtendedRoutes below) keyed by routeKey, which keeps the mapping
// just as exhaustive without the complexity of a 160-case switch.

// routeKey identifies a routing case by its exact HTTP method and URL path.
// Shared by classifyPath (handler.go) and classifyExtendedPath (this file).
type routeKey struct {
	method string
	path   string
}

// extendedOps returns every operation name handled outside handler.go's core
// switch (i.e. everything routed through classifyExtendedPath below).
func extendedOps() []string {
	return []string{
		opAssociateMember,
		opDisassociateMember,
		opGetMember,
		opListMembers,
		opEnableDelegatedAdminAccount,
		opDisableDelegatedAdminAccount,
		opGetDelegatedAdminAccount,
		opListDelegatedAdminAccounts,
		opDescribeOrganizationConfiguration,
		opUpdateOrganizationConfiguration,
		opGetEc2DeepInspectionConfiguration,
		opUpdateEc2DeepInspectionConfiguration,
		opUpdateOrgEc2DeepInspectionConfiguration,
		opBatchGetMemberEc2DeepInspectionStatus,
		opBatchUpdateMemberEc2DeepInspectionStatus,
		opGetEncryptionKey,
		opResetEncryptionKey,
		opUpdateEncryptionKey,
		opCreateCisScanConfiguration,
		opDeleteCisScanConfiguration,
		opUpdateCisScanConfiguration,
		opListCisScanConfigurations,
		opStartCisSession,
		opStopCisSession,
		opSendCisSessionHealth,
		opSendCisSessionTelemetry,
		opGetCisScanReport,
		opGetCisScanResultDetails,
		opListCisScans,
		opListCisScanResultsAggregatedByChecks,
		opListCisScanResultsAggregatedByTargetResource,
		opCreateCodeSecurityIntegration,
		opDeleteCodeSecurityIntegration,
		opGetCodeSecurityIntegration,
		opUpdateCodeSecurityIntegration,
		opListCodeSecurityIntegrations,
		opCreateCodeSecurityScanConfiguration,
		opDeleteCodeSecurityScanConfiguration,
		opGetCodeSecurityScanConfiguration,
		opUpdateCodeSecurityScanConfiguration,
		opListCodeSecurityScanConfigurations,
		opBatchAssociateCodeSecurityScanConfiguration,
		opBatchDisassociateCodeSecurityScanConfiguration,
		opListCodeSecurityScanConfigurationAssociations,
		opStartCodeSecurityScan,
		opGetCodeSecurityScan,
		opCreateFindingsReport,
		opCancelFindingsReport,
		opGetFindingsReportStatus,
		opCreateSbomExport,
		opCancelSbomExport,
		opGetSbomExport,
		opListCoverage,
		opListCoverageStatistics,
		opListFindingAggregations,
		opListUsageTotals,
		opListAccountPermissions,
		opSearchVulnerabilities,
		opBatchGetCodeSnippet,
		opBatchGetFindingDetails,
		opBatchGetFreeTrialInfo,
		opGetClustersForImage,
		opCreateConnector,
		opUpdateConnector,
		opDeleteConnector,
		opListConnectors,
		opListConnectorScanConfigurations,
		opUpdateConnectorScanConfiguration,
	}
}

// onceExtendedRoutes lazily builds the method+path -> operation lookup table
// used by classifyExtendedPath, exactly once.
//
//nolint:gochecknoglobals // read-only package-level lookup table, built once via sync.OnceValue
var onceExtendedRoutes = sync.OnceValue(func() map[routeKey]string {
	return map[routeKey]string{
		// Members
		{http.MethodPost, pathMembersAssociate}:    opAssociateMember,
		{http.MethodPost, pathMembersDisassociate}: opDisassociateMember,
		{http.MethodPost, pathMembersGet}:          opGetMember,
		{http.MethodPost, pathMembersList}:         opListMembers,

		// Delegated admin accounts
		{http.MethodPost, pathDelegatedAdminEnable}:  opEnableDelegatedAdminAccount,
		{http.MethodPost, pathDelegatedAdminDisable}: opDisableDelegatedAdminAccount,
		{http.MethodPost, pathDelegatedAdminGet}:     opGetDelegatedAdminAccount,
		{http.MethodPost, pathDelegatedAdminList}:    opListDelegatedAdminAccounts,

		// Organization configuration
		{http.MethodPost, pathOrgConfigDescribe}: opDescribeOrganizationConfiguration,
		{http.MethodPost, pathOrgConfigUpdate}:   opUpdateOrganizationConfiguration,

		// EC2 Deep Inspection
		{http.MethodPost, pathEc2DeepConfigGet}:     opGetEc2DeepInspectionConfiguration,
		{http.MethodPost, pathEc2DeepConfigUpdate}:  opUpdateEc2DeepInspectionConfiguration,
		{http.MethodPost, pathEc2DeepOrgUpdate}:     opUpdateOrgEc2DeepInspectionConfiguration,
		{http.MethodPost, pathEc2MemberBatchGet}:    opBatchGetMemberEc2DeepInspectionStatus,
		{http.MethodPost, pathEc2MemberBatchUpdate}: opBatchUpdateMemberEc2DeepInspectionStatus,

		// Encryption Key
		{http.MethodGet, pathEncryptionKeyGet}:    opGetEncryptionKey,
		{http.MethodPut, pathEncryptionKeyReset}:  opResetEncryptionKey,
		{http.MethodPut, pathEncryptionKeyUpdate}: opUpdateEncryptionKey,

		// CIS Scan Configuration
		{http.MethodPost, pathCisScanConfigCreate}: opCreateCisScanConfiguration,
		{http.MethodPost, pathCisScanConfigDelete}: opDeleteCisScanConfiguration,
		{http.MethodPost, pathCisScanConfigUpdate}: opUpdateCisScanConfiguration,
		{http.MethodPost, pathCisScanConfigList}:   opListCisScanConfigurations,

		// CIS Session
		{http.MethodPut, pathCisSessionStart}:            opStartCisSession,
		{http.MethodPut, pathCisSessionStop}:             opStopCisSession,
		{http.MethodPut, pathCisSessionHealthSend}:       opSendCisSessionHealth,
		{http.MethodPut, pathCisSessionTelemetrySend}:    opSendCisSessionTelemetry,
		{http.MethodPost, pathCisScanReportGet}:          opGetCisScanReport,
		{http.MethodPost, pathCisScanResultDetailsGet}:   opGetCisScanResultDetails,
		{http.MethodPost, pathCisScanList}:               opListCisScans,
		{http.MethodPost, pathCisScanResultCheckList}:    opListCisScanResultsAggregatedByChecks,
		{http.MethodPost, pathCisScanResultResourceList}: opListCisScanResultsAggregatedByTargetResource,

		// Code Security Integration
		{http.MethodPost, pathCodeSecurityIntegrationCreate}: opCreateCodeSecurityIntegration,
		{http.MethodPost, pathCodeSecurityIntegrationDelete}: opDeleteCodeSecurityIntegration,
		{http.MethodPost, pathCodeSecurityIntegrationGet}:    opGetCodeSecurityIntegration,
		{http.MethodPost, pathCodeSecurityIntegrationUpdate}: opUpdateCodeSecurityIntegration,
		{http.MethodPost, pathCodeSecurityIntegrationList}:   opListCodeSecurityIntegrations,

		// Code Security Scan Configuration
		{http.MethodPost, pathCodeSecurityScanConfigCreate}:        opCreateCodeSecurityScanConfiguration,
		{http.MethodPost, pathCodeSecurityScanConfigDelete}:        opDeleteCodeSecurityScanConfiguration,
		{http.MethodPost, pathCodeSecurityScanConfigGet}:           opGetCodeSecurityScanConfiguration,
		{http.MethodPost, pathCodeSecurityScanConfigUpdate}:        opUpdateCodeSecurityScanConfiguration,
		{http.MethodPost, pathCodeSecurityScanConfigList}:          opListCodeSecurityScanConfigurations,
		{http.MethodPost, pathCodeSecurityScanConfigBatchAssoc}:    opBatchAssociateCodeSecurityScanConfiguration,
		{http.MethodPost, pathCodeSecurityScanConfigBatchDisassoc}: opBatchDisassociateCodeSecurityScanConfiguration,
		{http.MethodPost, pathCodeSecurityScanConfigAssocList}:     opListCodeSecurityScanConfigurationAssociations,
		{http.MethodPost, pathCodeSecurityScanStart}:               opStartCodeSecurityScan,
		{http.MethodPost, pathCodeSecurityScanGet}:                 opGetCodeSecurityScan,

		// Findings Report
		{http.MethodPost, pathReportingCreate}:    opCreateFindingsReport,
		{http.MethodPost, pathReportingCancel}:    opCancelFindingsReport,
		{http.MethodPost, pathReportingStatusGet}: opGetFindingsReportStatus,

		// SBOM Export
		{http.MethodPost, pathSbomExportCreate}: opCreateSbomExport,
		{http.MethodPost, pathSbomExportCancel}: opCancelSbomExport,
		{http.MethodPost, pathSbomExportGet}:    opGetSbomExport,

		// Coverage
		{http.MethodPost, pathCoverageList}:           opListCoverage,
		{http.MethodPost, pathCoverageStatisticsList}: opListCoverageStatistics,

		// Aggregations / usage / permissions
		{http.MethodPost, pathFindingAggregationList}: opListFindingAggregations,
		{http.MethodPost, pathUsageList}:              opListUsageTotals,
		{http.MethodPost, pathAccountPermissionsList}: opListAccountPermissions,

		// Search
		{http.MethodPost, pathVulnerabilitiesSearch}: opSearchVulnerabilities,

		// Batch ops
		{http.MethodPost, pathCodeSnippetBatchGet}:    opBatchGetCodeSnippet,
		{http.MethodPost, pathFindingDetailsBatchGet}: opBatchGetFindingDetails,
		{http.MethodPost, pathFreeTrialInfoBatchGet}:  opBatchGetFreeTrialInfo,

		// Clusters
		{http.MethodPost, pathClusterGet}: opGetClustersForImage,

		// Connectors
		{http.MethodPost, pathConnectorCreate}:           opCreateConnector,
		{http.MethodPost, pathConnectorUpdate}:           opUpdateConnector,
		{http.MethodPost, pathConnectorDelete}:           opDeleteConnector,
		{http.MethodPost, pathConnectorList}:             opListConnectors,
		{http.MethodPost, pathConnectorScanConfigList}:   opListConnectorScanConfigurations,
		{http.MethodPost, pathConnectorScanConfigUpdate}: opUpdateConnectorScanConfiguration,
	}
})

// classifyExtendedPath maps method+path to its extended operation name (one
// of the 62 ops in extendedOps), or opUnknown if no extended route matches.
func classifyExtendedPath(method, path string) string {
	if op, ok := onceExtendedRoutes()[routeKey{method: method, path: path}]; ok {
		return op
	}

	return opUnknown
}

func (h *Handler) handleExtendedOps(c *echo.Context) (bool, error) {
	op := classifyExtendedPath(c.Request().Method, c.Request().URL.Path)
	if op == opUnknown {
		return false, nil
	}

	fn, ok := h.extendedHandlerMap()[op]
	if !ok {
		return false, nil
	}

	return true, fn(c)
}

func (h *Handler) extendedHandlerMap() map[string]func(*echo.Context) error {
	return map[string]func(*echo.Context) error{
		opAssociateMember:                                h.handleAssociateMember,
		opDisassociateMember:                             h.handleDisassociateMember,
		opGetMember:                                      h.handleGetMember,
		opListMembers:                                    h.handleListMembers,
		opEnableDelegatedAdminAccount:                    h.handleEnableDelegatedAdminAccount,
		opDisableDelegatedAdminAccount:                   h.handleDisableDelegatedAdminAccount,
		opGetDelegatedAdminAccount:                       h.handleGetDelegatedAdminAccount,
		opListDelegatedAdminAccounts:                     h.handleListDelegatedAdminAccounts,
		opDescribeOrganizationConfiguration:              h.handleDescribeOrganizationConfiguration,
		opUpdateOrganizationConfiguration:                h.handleUpdateOrganizationConfiguration,
		opGetEc2DeepInspectionConfiguration:              h.handleGetEc2DeepInspectionConfiguration,
		opUpdateEc2DeepInspectionConfiguration:           h.handleUpdateEc2DeepInspectionConfiguration,
		opUpdateOrgEc2DeepInspectionConfiguration:        h.handleUpdateOrgEc2DeepInspectionConfiguration,
		opBatchGetMemberEc2DeepInspectionStatus:          h.handleBatchGetMemberEc2DeepInspectionStatus,
		opBatchUpdateMemberEc2DeepInspectionStatus:       h.handleBatchUpdateMemberEc2DeepInspectionStatus,
		opGetEncryptionKey:                               h.handleGetEncryptionKey,
		opResetEncryptionKey:                             h.handleResetEncryptionKey,
		opUpdateEncryptionKey:                            h.handleUpdateEncryptionKey,
		opCreateCisScanConfiguration:                     h.handleCreateCisScanConfiguration,
		opDeleteCisScanConfiguration:                     h.handleDeleteCisScanConfiguration,
		opUpdateCisScanConfiguration:                     h.handleUpdateCisScanConfiguration,
		opListCisScanConfigurations:                      h.handleListCisScanConfigurations,
		opStartCisSession:                                h.handleStartCisSession,
		opStopCisSession:                                 h.handleStopCisSession,
		opSendCisSessionHealth:                           h.handleSendCisSessionHealth,
		opSendCisSessionTelemetry:                        h.handleSendCisSessionTelemetry,
		opGetCisScanReport:                               h.handleGetCisScanReport,
		opGetCisScanResultDetails:                        h.handleGetCisScanResultDetails,
		opListCisScans:                                   h.handleListCisScans,
		opListCisScanResultsAggregatedByChecks:           h.handleListCisScanResultsAggregatedByChecks,
		opListCisScanResultsAggregatedByTargetResource:   h.handleListCisScanResultsAggregatedByTargetResource,
		opCreateCodeSecurityIntegration:                  h.handleCreateCodeSecurityIntegration,
		opDeleteCodeSecurityIntegration:                  h.handleDeleteCodeSecurityIntegration,
		opGetCodeSecurityIntegration:                     h.handleGetCodeSecurityIntegration,
		opUpdateCodeSecurityIntegration:                  h.handleUpdateCodeSecurityIntegration,
		opListCodeSecurityIntegrations:                   h.handleListCodeSecurityIntegrations,
		opCreateCodeSecurityScanConfiguration:            h.handleCreateCodeSecurityScanConfiguration,
		opDeleteCodeSecurityScanConfiguration:            h.handleDeleteCodeSecurityScanConfiguration,
		opGetCodeSecurityScanConfiguration:               h.handleGetCodeSecurityScanConfiguration,
		opUpdateCodeSecurityScanConfiguration:            h.handleUpdateCodeSecurityScanConfiguration,
		opListCodeSecurityScanConfigurations:             h.handleListCodeSecurityScanConfigurations,
		opBatchAssociateCodeSecurityScanConfiguration:    h.handleBatchAssociateCodeSecurityScanConfiguration,
		opBatchDisassociateCodeSecurityScanConfiguration: h.handleBatchDisassociateCodeSecurityScanConfiguration,
		opListCodeSecurityScanConfigurationAssociations:  h.handleListCodeSecurityScanConfigurationAssociations,
		opStartCodeSecurityScan:                          h.handleStartCodeSecurityScan,
		opGetCodeSecurityScan:                            h.handleGetCodeSecurityScan,
		opCreateFindingsReport:                           h.handleCreateFindingsReport,
		opCancelFindingsReport:                           h.handleCancelFindingsReport,
		opGetFindingsReportStatus:                        h.handleGetFindingsReportStatus,
		opCreateSbomExport:                               h.handleCreateSbomExport,
		opCancelSbomExport:                               h.handleCancelSbomExport,
		opGetSbomExport:                                  h.handleGetSbomExport,
		opListCoverage:                                   h.handleListCoverage,
		opListCoverageStatistics:                         h.handleListCoverageStatistics,
		opListFindingAggregations:                        h.handleListFindingAggregations,
		opListUsageTotals:                                h.handleListUsageTotals,
		opListAccountPermissions:                         h.handleListAccountPermissions,
		opSearchVulnerabilities:                          h.handleSearchVulnerabilities,
		opBatchGetCodeSnippet:                            h.handleBatchGetCodeSnippet,
		opBatchGetFindingDetails:                         h.handleBatchGetFindingDetails,
		opBatchGetFreeTrialInfo:                          h.handleBatchGetFreeTrialInfo,
		opGetClustersForImage:                            h.handleGetClustersForImage,
		opCreateConnector:                                h.handleCreateConnector,
		opUpdateConnector:                                h.handleUpdateConnector,
		opDeleteConnector:                                h.handleDeleteConnector,
		opListConnectors:                                 h.handleListConnectors,
		opListConnectorScanConfigurations:                h.handleListConnectorScanConfigurations,
		opUpdateConnectorScanConfiguration:               h.handleUpdateConnectorScanConfiguration,
	}
}
