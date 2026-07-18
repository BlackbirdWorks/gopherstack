package inspector2

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// This file holds the routing dispatch table for every Inspector2 operation
// beyond the core set handled directly in handler.go's handleREST switch.
// Each operation/path constant is declared alongside its handler in the
// matching handler_<family>.go file; this table only wires path -> operation
// -> handler func. classifyExtendedPath is a flat, exhaustive path-to-op
// mapping across every family (62 ops) -- genuinely a routing table, not
// logic that decomposes further, so its existing complexity nolints are kept
// (see the func doc below) rather than split.

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
	}
}

//nolint:cyclop,gocyclo // exhaustive path-to-operation mapping for all 62 extended ops
func classifyExtendedPath(method, path string) string { //nolint:gocognit,funlen // existing issue.
	switch {
	// Members
	case method == http.MethodPost && path == pathMembersAssociate:
		return opAssociateMember
	case method == http.MethodPost && path == pathMembersDisassociate:
		return opDisassociateMember
	case method == http.MethodPost && path == pathMembersGet:
		return opGetMember
	case method == http.MethodPost && path == pathMembersList:
		return opListMembers

	// Delegated admin accounts
	case method == http.MethodPost && path == pathDelegatedAdminEnable:
		return opEnableDelegatedAdminAccount
	case method == http.MethodPost && path == pathDelegatedAdminDisable:
		return opDisableDelegatedAdminAccount
	case method == http.MethodPost && path == pathDelegatedAdminGet:
		return opGetDelegatedAdminAccount
	case method == http.MethodPost && path == pathDelegatedAdminList:
		return opListDelegatedAdminAccounts

	// Organization configuration
	case method == http.MethodPost && path == pathOrgConfigDescribe:
		return opDescribeOrganizationConfiguration
	case method == http.MethodPost && path == pathOrgConfigUpdate:
		return opUpdateOrganizationConfiguration

	// EC2 Deep Inspection
	case method == http.MethodPost && path == pathEc2DeepConfigGet:
		return opGetEc2DeepInspectionConfiguration
	case method == http.MethodPost && path == pathEc2DeepConfigUpdate:
		return opUpdateEc2DeepInspectionConfiguration
	case method == http.MethodPost && path == pathEc2DeepOrgUpdate:
		return opUpdateOrgEc2DeepInspectionConfiguration
	case method == http.MethodPost && path == pathEc2MemberBatchGet:
		return opBatchGetMemberEc2DeepInspectionStatus
	case method == http.MethodPost && path == pathEc2MemberBatchUpdate:
		return opBatchUpdateMemberEc2DeepInspectionStatus

	// Encryption Key
	case method == http.MethodGet && path == pathEncryptionKeyGet:
		return opGetEncryptionKey
	case method == http.MethodPut && path == pathEncryptionKeyReset:
		return opResetEncryptionKey
	case method == http.MethodPut && path == pathEncryptionKeyUpdate:
		return opUpdateEncryptionKey

	// CIS Scan Configuration
	case method == http.MethodPost && path == pathCisScanConfigCreate:
		return opCreateCisScanConfiguration
	case method == http.MethodPost && path == pathCisScanConfigDelete:
		return opDeleteCisScanConfiguration
	case method == http.MethodPost && path == pathCisScanConfigUpdate:
		return opUpdateCisScanConfiguration
	case method == http.MethodPost && path == pathCisScanConfigList:
		return opListCisScanConfigurations

	// CIS Session
	case method == http.MethodPut && path == pathCisSessionStart:
		return opStartCisSession
	case method == http.MethodPut && path == pathCisSessionStop:
		return opStopCisSession
	case method == http.MethodPut && path == pathCisSessionHealthSend:
		return opSendCisSessionHealth
	case method == http.MethodPut && path == pathCisSessionTelemetrySend:
		return opSendCisSessionTelemetry
	case method == http.MethodPost && path == pathCisScanReportGet:
		return opGetCisScanReport
	case method == http.MethodPost && path == pathCisScanResultDetailsGet:
		return opGetCisScanResultDetails
	case method == http.MethodPost && path == pathCisScanList:
		return opListCisScans
	case method == http.MethodPost && path == pathCisScanResultCheckList:
		return opListCisScanResultsAggregatedByChecks
	case method == http.MethodPost && path == pathCisScanResultResourceList:
		return opListCisScanResultsAggregatedByTargetResource

	// Code Security Integration
	case method == http.MethodPost && path == pathCodeSecurityIntegrationCreate:
		return opCreateCodeSecurityIntegration
	case method == http.MethodPost && path == pathCodeSecurityIntegrationDelete:
		return opDeleteCodeSecurityIntegration
	case method == http.MethodPost && path == pathCodeSecurityIntegrationGet:
		return opGetCodeSecurityIntegration
	case method == http.MethodPost && path == pathCodeSecurityIntegrationUpdate:
		return opUpdateCodeSecurityIntegration
	case method == http.MethodPost && path == pathCodeSecurityIntegrationList:
		return opListCodeSecurityIntegrations

	// Code Security Scan Configuration
	case method == http.MethodPost && path == pathCodeSecurityScanConfigCreate:
		return opCreateCodeSecurityScanConfiguration
	case method == http.MethodPost && path == pathCodeSecurityScanConfigDelete:
		return opDeleteCodeSecurityScanConfiguration
	case method == http.MethodPost && path == pathCodeSecurityScanConfigGet:
		return opGetCodeSecurityScanConfiguration
	case method == http.MethodPost && path == pathCodeSecurityScanConfigUpdate:
		return opUpdateCodeSecurityScanConfiguration
	case method == http.MethodPost && path == pathCodeSecurityScanConfigList:
		return opListCodeSecurityScanConfigurations
	case method == http.MethodPost && path == pathCodeSecurityScanConfigBatchAssoc:
		return opBatchAssociateCodeSecurityScanConfiguration
	case method == http.MethodPost && path == pathCodeSecurityScanConfigBatchDisassoc:
		return opBatchDisassociateCodeSecurityScanConfiguration
	case method == http.MethodPost && path == pathCodeSecurityScanConfigAssocList:
		return opListCodeSecurityScanConfigurationAssociations
	case method == http.MethodPost && path == pathCodeSecurityScanStart:
		return opStartCodeSecurityScan
	case method == http.MethodPost && path == pathCodeSecurityScanGet:
		return opGetCodeSecurityScan

	// Findings Report
	case method == http.MethodPost && path == pathReportingCreate:
		return opCreateFindingsReport
	case method == http.MethodPost && path == pathReportingCancel:
		return opCancelFindingsReport
	case method == http.MethodPost && path == pathReportingStatusGet:
		return opGetFindingsReportStatus

	// SBOM Export
	case method == http.MethodPost && path == pathSbomExportCreate:
		return opCreateSbomExport
	case method == http.MethodPost && path == pathSbomExportCancel:
		return opCancelSbomExport
	case method == http.MethodPost && path == pathSbomExportGet:
		return opGetSbomExport

	// Coverage
	case method == http.MethodPost && path == pathCoverageList:
		return opListCoverage
	case method == http.MethodPost && path == pathCoverageStatisticsList:
		return opListCoverageStatistics

	// Aggregations / usage / permissions
	case method == http.MethodPost && path == pathFindingAggregationList:
		return opListFindingAggregations
	case method == http.MethodPost && path == pathUsageList:
		return opListUsageTotals
	case method == http.MethodPost && path == pathAccountPermissionsList:
		return opListAccountPermissions

	// Search
	case method == http.MethodPost && path == pathVulnerabilitiesSearch:
		return opSearchVulnerabilities

	// Batch ops
	case method == http.MethodPost && path == pathCodeSnippetBatchGet:
		return opBatchGetCodeSnippet
	case method == http.MethodPost && path == pathFindingDetailsBatchGet:
		return opBatchGetFindingDetails
	case method == http.MethodPost && path == pathFreeTrialInfoBatchGet:
		return opBatchGetFreeTrialInfo

	// Clusters
	case method == http.MethodPost && path == pathClusterGet:
		return opGetClustersForImage
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
	}
}
