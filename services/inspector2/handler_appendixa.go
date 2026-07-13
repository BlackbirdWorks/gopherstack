package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// --- Operation name constants for appendix A ops ---

const (
	opAssociateMember    = "AssociateMember"
	opDisassociateMember = "DisassociateMember"
	opGetMember          = "GetMember"
	opListMembers        = "ListMembers"

	opEnableDelegatedAdminAccount  = "EnableDelegatedAdminAccount"
	opDisableDelegatedAdminAccount = "DisableDelegatedAdminAccount"
	opGetDelegatedAdminAccount     = "GetDelegatedAdminAccount"
	opListDelegatedAdminAccounts   = "ListDelegatedAdminAccounts"

	opDescribeOrganizationConfiguration = "DescribeOrganizationConfiguration"
	opUpdateOrganizationConfiguration   = "UpdateOrganizationConfiguration"

	opGetEc2DeepInspectionConfiguration        = "GetEc2DeepInspectionConfiguration"
	opUpdateEc2DeepInspectionConfiguration     = "UpdateEc2DeepInspectionConfiguration"
	opUpdateOrgEc2DeepInspectionConfiguration  = "UpdateOrgEc2DeepInspectionConfiguration"
	opBatchGetMemberEc2DeepInspectionStatus    = "BatchGetMemberEc2DeepInspectionStatus"
	opBatchUpdateMemberEc2DeepInspectionStatus = "BatchUpdateMemberEc2DeepInspectionStatus"

	opGetEncryptionKey    = "GetEncryptionKey"
	opResetEncryptionKey  = "ResetEncryptionKey"
	opUpdateEncryptionKey = "UpdateEncryptionKey"

	opCreateCisScanConfiguration = "CreateCisScanConfiguration"
	opDeleteCisScanConfiguration = "DeleteCisScanConfiguration"
	opUpdateCisScanConfiguration = "UpdateCisScanConfiguration"
	opListCisScanConfigurations  = "ListCisScanConfigurations"

	opStartCisSession                              = "StartCisSession"
	opStopCisSession                               = "StopCisSession"
	opSendCisSessionHealth                         = "SendCisSessionHealth"
	opSendCisSessionTelemetry                      = "SendCisSessionTelemetry"
	opGetCisScanReport                             = "GetCisScanReport"
	opGetCisScanResultDetails                      = "GetCisScanResultDetails"
	opListCisScans                                 = "ListCisScans"
	opListCisScanResultsAggregatedByChecks         = "ListCisScanResultsAggregatedByChecks"
	opListCisScanResultsAggregatedByTargetResource = "ListCisScanResultsAggregatedByTargetResource"

	opCreateCodeSecurityIntegration = "CreateCodeSecurityIntegration"
	opDeleteCodeSecurityIntegration = "DeleteCodeSecurityIntegration"
	opGetCodeSecurityIntegration    = "GetCodeSecurityIntegration"
	opUpdateCodeSecurityIntegration = "UpdateCodeSecurityIntegration"
	opListCodeSecurityIntegrations  = "ListCodeSecurityIntegrations"

	opCreateCodeSecurityScanConfiguration            = "CreateCodeSecurityScanConfiguration"
	opDeleteCodeSecurityScanConfiguration            = "DeleteCodeSecurityScanConfiguration"
	opGetCodeSecurityScanConfiguration               = "GetCodeSecurityScanConfiguration"
	opUpdateCodeSecurityScanConfiguration            = "UpdateCodeSecurityScanConfiguration"
	opListCodeSecurityScanConfigurations             = "ListCodeSecurityScanConfigurations"
	opBatchAssociateCodeSecurityScanConfiguration    = "BatchAssociateCodeSecurityScanConfiguration"
	opBatchDisassociateCodeSecurityScanConfiguration = "BatchDisassociateCodeSecurityScanConfiguration"
	opListCodeSecurityScanConfigurationAssociations  = "ListCodeSecurityScanConfigurationAssociations"
	opStartCodeSecurityScan                          = "StartCodeSecurityScan"
	opGetCodeSecurityScan                            = "GetCodeSecurityScan"

	opCreateFindingsReport    = "CreateFindingsReport"
	opCancelFindingsReport    = "CancelFindingsReport"
	opGetFindingsReportStatus = "GetFindingsReportStatus"

	opCreateSbomExport = "CreateSbomExport"
	opCancelSbomExport = "CancelSbomExport"
	opGetSbomExport    = "GetSbomExport"

	opListCoverage           = "ListCoverage"
	opListCoverageStatistics = "ListCoverageStatistics"

	opListFindingAggregations = "ListFindingAggregations"
	opListUsageTotals         = "ListUsageTotals"
	opListAccountPermissions  = "ListAccountPermissions"

	opSearchVulnerabilities = "SearchVulnerabilities"

	opBatchGetCodeSnippet    = "BatchGetCodeSnippet"
	opBatchGetFindingDetails = "BatchGetFindingDetails"
	opBatchGetFreeTrialInfo  = "BatchGetFreeTrialInfo"

	opGetClustersForImage = "GetClustersForImage"

	// paths.
	pathMembersAssociate    = "/members/associate"
	pathMembersDisassociate = "/members/disassociate"
	pathMembersGet          = "/members/get"
	pathMembersList         = "/members/list"

	pathDelegatedAdminEnable  = "/delegatedadminaccounts/enable"
	pathDelegatedAdminDisable = "/delegatedadminaccounts/disable"
	pathDelegatedAdminGet     = "/delegatedadminaccounts/get"
	pathDelegatedAdminList    = "/delegatedadminaccounts/list"

	pathOrgConfigDescribe = "/organizationconfiguration/describe"
	pathOrgConfigUpdate   = "/organizationconfiguration/update"

	pathEc2DeepConfigGet     = "/ec2deepinspectionconfiguration/get"
	pathEc2DeepConfigUpdate  = "/ec2deepinspectionconfiguration/update"
	pathEc2DeepOrgUpdate     = "/ec2deepinspectionconfiguration/org/update"
	pathEc2MemberBatchGet    = "/ec2deepinspectionstatus/member/batch/get"
	pathEc2MemberBatchUpdate = "/ec2deepinspectionstatus/member/batch/update"

	pathEncryptionKeyGet    = "/encryptionkey/get"
	pathEncryptionKeyReset  = "/encryptionkey/reset"
	pathEncryptionKeyUpdate = "/encryptionkey/update"

	pathCisScanConfigCreate = "/cis/scan-configuration/create"
	pathCisScanConfigDelete = "/cis/scan-configuration/delete"
	pathCisScanConfigUpdate = "/cis/scan-configuration/update"
	pathCisScanConfigList   = "/cis/scan-configuration/list"

	pathCisSessionStart           = "/cissession/start"
	pathCisSessionStop            = "/cissession/stop"
	pathCisSessionHealthSend      = "/cissession/health/send"
	pathCisSessionTelemetrySend   = "/cissession/telemetry/send"
	pathCisScanReportGet          = "/cis/scan/report/get"
	pathCisScanResultDetailsGet   = "/cis/scan-result/details/get"
	pathCisScanList               = "/cis/scan/list"
	pathCisScanResultCheckList    = "/cis/scan-result/check/list"
	pathCisScanResultResourceList = "/cis/scan-result/resource/list"

	pathCodeSecurityIntegrationCreate = "/codesecurity/integration/create"
	pathCodeSecurityIntegrationDelete = "/codesecurity/integration/delete"
	pathCodeSecurityIntegrationGet    = "/codesecurity/integration/get"
	pathCodeSecurityIntegrationUpdate = "/codesecurity/integration/update"
	pathCodeSecurityIntegrationList   = "/codesecurity/integration/list"

	pathCodeSecurityScanConfigCreate        = "/codesecurity/scan-configuration/create"
	pathCodeSecurityScanConfigDelete        = "/codesecurity/scan-configuration/delete"
	pathCodeSecurityScanConfigGet           = "/codesecurity/scan-configuration/get"
	pathCodeSecurityScanConfigUpdate        = "/codesecurity/scan-configuration/update"
	pathCodeSecurityScanConfigList          = "/codesecurity/scan-configuration/list"
	pathCodeSecurityScanConfigBatchAssoc    = "/codesecurity/scan-configuration/batch/associate"
	pathCodeSecurityScanConfigBatchDisassoc = "/codesecurity/scan-configuration/batch/disassociate"
	pathCodeSecurityScanConfigAssocList     = "/codesecurity/scan-configuration/associations/list"
	pathCodeSecurityScanStart               = "/codesecurity/scan/start"
	pathCodeSecurityScanGet                 = "/codesecurity/scan/get"

	pathReportingCreate    = "/reporting/create"
	pathReportingCancel    = "/reporting/cancel"
	pathReportingStatusGet = "/reporting/status/get"

	pathSbomExportCreate = "/sbomexport/create"
	pathSbomExportCancel = "/sbomexport/cancel"
	pathSbomExportGet    = "/sbomexport/get"

	pathCoverageList           = "/coverage/list"
	pathCoverageStatisticsList = "/coverage/statistics/list"

	pathFindingAggregationList = "/findings/aggregation/list"
	pathUsageList              = "/usage/list"
	pathAccountPermissionsList = "/accountpermissions/list"

	pathVulnerabilitiesSearch = "/vulnerabilities/search"

	pathCodeSnippetBatchGet    = "/codesnippet/batchget"
	pathFindingDetailsBatchGet = "/findings/details/batch/get"
	pathFreeTrialInfoBatchGet  = "/freetrialinfo/batchget"

	pathClusterGet = "/cluster/get"

	keyScanConfigurationArn  = "scanConfigurationArn"
	keyReportID              = "reportId"
	keyDelegatedAdminAccount = "delegatedAdminAccountId"
	keyIntegrationArn        = "integrationArn"
)

// appendixAOps returns all appendix A operation names.
func appendixAOps() []string {
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

//nolint:cyclop,gocyclo // exhaustive path-to-operation mapping for all 62 appendix A ops
func classifyAppendixAPath(method, path string) string { //nolint:gocognit,funlen // existing issue.
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

func (h *Handler) handleAppendixA(c *echo.Context) (bool, error) {
	op := classifyAppendixAPath(c.Request().Method, c.Request().URL.Path)
	if op == opUnknown {
		return false, nil
	}

	fn, ok := h.appendixAHandlerMap()[op]
	if !ok {
		return false, nil
	}

	return true, fn(c)
}

func (h *Handler) appendixAHandlerMap() map[string]func(*echo.Context) error {
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

// --- Handler implementations ---

func (h *Handler) handleAssociateMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountID string `json:"accountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if assocErr := h.Backend.AssociateMember(req.AccountID); assocErr != nil {
		return h.mapError(c, assocErr)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyAccountID: req.AccountID},
	)
}

func (h *Handler) handleDisassociateMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountID string `json:"accountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if disassocErr := h.Backend.DisassociateMember(req.AccountID); disassocErr != nil {
		return h.mapError(c, disassocErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAccountID: req.AccountID})
}

func (h *Handler) handleGetMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountID string `json:"accountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	m, getErr := h.Backend.GetMember(req.AccountID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"member": memberToWire(m)})
}

// memberToWire renders a Member in its Inspector2 wire shape. UpdatedAt is a
// Go time.Time internally, but the restjson1 Member.UpdatedAt member is a
// DateTimeTimestamp (epoch-seconds JSON number, see pkgs/awstime) -- the
// domain struct's default JSON marshaling would emit an RFC3339 string and
// break real SDK clients' GetMember/ListMembers deserializers.
func memberToWire(m *Member) map[string]any {
	return map[string]any{
		keyAccountID:             m.AccountID,
		keyDelegatedAdminAccount: m.DelegatedAdminAccountID,
		"relationshipStatus":     m.RelationshipStatus,
		keyUpdatedAt:             awstime.Epoch(m.UpdatedAt),
	}
}

func (h *Handler) handleListMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		OnlyAssociated bool `json:"onlyAssociated"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	members, listErr := h.Backend.ListMembers(req.OnlyAssociated)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	wire := make([]map[string]any, 0, len(members))
	for _, m := range members {
		wire = append(wire, memberToWire(m))
	}

	return c.JSON(http.StatusOK, map[string]any{"members": wire})
}

func (h *Handler) handleEnableDelegatedAdminAccount(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		DelegatedAdminAccountID string `json:"delegatedAdminAccountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if enableErr := h.Backend.EnableDelegatedAdminAccount(req.DelegatedAdminAccountID); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDelegatedAdminAccount: req.DelegatedAdminAccountID,
		keyStatus:                statusEnabled,
	})
}

func (h *Handler) handleDisableDelegatedAdminAccount(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		DelegatedAdminAccountID string `json:"delegatedAdminAccountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if disErr := h.Backend.DisableDelegatedAdminAccount(req.DelegatedAdminAccountID); disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDelegatedAdminAccount: req.DelegatedAdminAccountID,
		keyStatus:                "DISABLE_IN_PROGRESS",
	})
}

func (h *Handler) handleGetDelegatedAdminAccount(c *echo.Context) error {
	d, err := h.Backend.GetDelegatedAdminAccount()
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"delegatedAdmin": d})
}

func (h *Handler) handleListDelegatedAdminAccounts(c *echo.Context) error {
	accounts, err := h.Backend.ListDelegatedAdminAccounts()
	if err != nil {
		return h.mapError(c, err)
	}

	if accounts == nil {
		accounts = []*DelegatedAdminAccount{}
	}

	return c.JSON(http.StatusOK, map[string]any{"delegatedAdminAccounts": accounts})
}

func (h *Handler) handleDescribeOrganizationConfiguration(c *echo.Context) error {
	cfg := h.Backend.DescribeOrganizationConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		"autoEnable":             cfg.AutoEnable,
		"maxAccountLimitReached": cfg.MaxAccountLimitReached,
	})
}

func (h *Handler) handleUpdateOrganizationConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AutoEnable map[string]bool `json:"autoEnable"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	autoEnable := false
	for _, v := range req.AutoEnable {
		if v {
			autoEnable = true

			break
		}
	}

	if updateErr := h.Backend.UpdateOrganizationConfiguration(
		OrgConfiguration{AutoEnable: autoEnable},
	); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetEc2DeepInspectionConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetEc2DeepInspectionConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		"ec2ScanModeState": map[string]any{
			"scanMode":       "EC2_SSM_AGENT_BASED",
			"scanModeStatus": "SUCCESS",
		},
		keyErrorMessage: cfg.ErrorMessage,
		"packagePaths":  cfg.PackagePaths,
		keyStatus:       cfg.Status,
	})
}

func (h *Handler) handleUpdateEc2DeepInspectionConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		PackagePaths []string `json:"packagePaths"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if updateErr := h.Backend.UpdateEc2DeepInspectionConfiguration(req.PackagePaths); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	cfg := h.Backend.GetEc2DeepInspectionConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		keyErrorMessage: cfg.ErrorMessage,
		"packagePaths":  cfg.PackagePaths,
		keyStatus:       cfg.Status,
	})
}

func (h *Handler) handleUpdateOrgEc2DeepInspectionConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Ec2ScanModeConfig *struct {
			ScanMode string `json:"scanMode"`
		} `json:"ec2ScanModeConfig"`
		OrgPackagePaths []string `json:"orgPackagePaths"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if updateErr := h.Backend.UpdateOrgEc2DeepInspectionConfiguration(req.OrgPackagePaths); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleBatchGetMemberEc2DeepInspectionStatus(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	statuses := h.Backend.BatchGetMemberEc2DeepInspectionStatus(req.AccountIDs)

	return c.JSON(http.StatusOK, map[string]any{
		"members":          statuses,
		"failedAccountIds": []any{},
	})
}

func (h *Handler) handleBatchUpdateMemberEc2DeepInspectionStatus(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountEc2DeepInspectionStatuses []*MemberEc2DeepInspectionStatus `json:"accountEc2DeepInspectionStatuses"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	updated := h.Backend.BatchUpdateMemberEc2DeepInspectionStatus(
		req.AccountEc2DeepInspectionStatuses,
	)

	return c.JSON(http.StatusOK, map[string]any{
		"accounts":         updated,
		"failedAccountIds": []any{},
	})
}

func (h *Handler) handleGetEncryptionKey(c *echo.Context) error {
	resourceType := c.Request().URL.Query().Get("resourceType")
	scanType := c.Request().URL.Query().Get("scanType")

	key, err := h.Backend.GetEncryptionKey(resourceType, scanType)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"kmsKeyId":     key.KmsKeyID,
		"resourceType": key.ResourceType,
		"scanType":     key.ScanType,
	})
}

func (h *Handler) handleResetEncryptionKey(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceType string `json:"resourceType"`
		ScanType     string `json:"scanType"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if resetErr := h.Backend.ResetEncryptionKey(req.ResourceType, req.ScanType); resetErr != nil {
		return h.mapError(c, resetErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateEncryptionKey(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		KmsKeyID     string `json:"kmsKeyId"`
		ResourceType string `json:"resourceType"`
		ScanType     string `json:"scanType"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if updateErr := h.Backend.UpdateEncryptionKey(req.KmsKeyID, req.ResourceType, req.ScanType); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateCisScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Schedule map[string]any    `json:"schedule"`
		Targets  map[string]any    `json:"targets"`
		Tags     map[string]string `json:"tags"`
		ScanName string            `json:"scanName"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, createErr := h.Backend.CreateCisScanConfiguration(
		req.ScanName,
		req.Schedule,
		req.Targets,
		req.Tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyScanConfigurationArn: cfg.Arn},
	)
}

func (h *Handler) handleDeleteCisScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if deleteErr := h.Backend.DeleteCisScanConfiguration(req.ScanConfigurationArn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: req.ScanConfigurationArn})
}

func (h *Handler) handleUpdateCisScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Schedule             map[string]any `json:"schedule"`
		Targets              map[string]any `json:"targets"`
		ScanConfigurationArn string         `json:"scanConfigurationArn"`
		ScanName             string         `json:"scanName"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, updateErr := h.Backend.UpdateCisScanConfiguration(
		req.ScanConfigurationArn,
		req.ScanName,
		req.Schedule,
		req.Targets,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: cfg.Arn})
}

func (h *Handler) handleListCisScanConfigurations(c *echo.Context) error {
	cfgs, err := h.Backend.ListCisScanConfigurations()
	if err != nil {
		return h.mapError(c, err)
	}

	if cfgs == nil {
		cfgs = []*CisScanConfiguration{}
	}

	return c.JSON(http.StatusOK, map[string]any{"scanConfigurations": cfgs})
}

func (h *Handler) handleStartCisSession(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Message   map[string]any `json:"message"`
		ScanJobID string         `json:"scanJobId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	sessionToken := ""
	if req.Message != nil {
		if st, ok := req.Message["sessionToken"].(string); ok {
			sessionToken = st
		}
	}

	if _, startErr := h.Backend.StartCisSession(req.ScanJobID, sessionToken); startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStopCisSession(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanJobID string `json:"scanJobId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if stopErr := h.Backend.StopCisSession(req.ScanJobID); stopErr != nil {
		return h.mapError(c, stopErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleSendCisSessionHealth(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanJobID string `json:"scanJobId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if healthErr := h.Backend.SendCisSessionHealth(req.ScanJobID); healthErr != nil {
		return h.mapError(c, healthErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleSendCisSessionTelemetry(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Messages  map[string]any `json:"messages"`
		ScanJobID string         `json:"scanJobId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	if telErr := h.Backend.SendCisSessionTelemetry(req.ScanJobID, req.Messages); telErr != nil {
		return h.mapError(c, telErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetCisScanReport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	report, err := h.Backend.GetCisScanReport(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, report)
}

func (h *Handler) handleGetCisScanResultDetails(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	details, err := h.Backend.GetCisScanResultDetails(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, details)
}

func (h *Handler) handleListCisScans(c *echo.Context) error {
	scans, err := h.Backend.ListCisScans()
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"cisScans": scans})
}

func (h *Handler) handleListCisScanResultsAggregatedByChecks(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	results, err := h.Backend.ListCisScanResultsAggregatedByChecks(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"checkAggregations": results})
}

func (h *Handler) handleListCisScanResultsAggregatedByTargetResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanArn string `json:"scanArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	results, err := h.Backend.ListCisScanResultsAggregatedByTargetResource(req.ScanArn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"targetResourceAggregations": results})
}

func (h *Handler) handleCreateCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Tags    map[string]string `json:"tags"`
		Details map[string]any    `json:"details"`
		Name    string            `json:"name"`
		Type    string            `json:"type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	integ, createErr := h.Backend.CreateCodeSecurityIntegration(
		req.Name,
		req.Type,
		req.Tags,
		req.Details,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyIntegrationArn: integ.IntegrationArn,
		keyStatus:         integ.Status,
	})
}

func (h *Handler) handleDeleteCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		IntegrationArn string `json:"integrationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if deleteErr := h.Backend.DeleteCodeSecurityIntegration(req.IntegrationArn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		IntegrationArn string `json:"integrationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	integ, getErr := h.Backend.GetCodeSecurityIntegration(req.IntegrationArn)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, codeSecurityIntegrationToWire(integ))
}

// codeSecurityIntegrationToWire renders a CodeSecurityIntegration in its
// Inspector2 wire shape. Get/ListCodeSecurityIntegrations both use
// createdOn/lastUpdateOn (epoch-seconds DateTimeTimestamp members, see
// pkgs/awstime) rather than the domain struct's internal createdAt/updatedAt
// field names -- marshaling the struct directly would both use the wrong
// keys and emit RFC3339 strings, either of which leaves the real fields
// unpopulated on the client.
func codeSecurityIntegrationToWire(integ *CodeSecurityIntegration) map[string]any {
	return map[string]any{
		keyIntegrationArn: integ.IntegrationArn,
		keyName:           integ.Name,
		keyStatus:         integ.Status,
		"createdOn":       awstime.Epoch(integ.CreatedAt),
		"lastUpdateOn":    awstime.Epoch(integ.UpdatedAt),
	}
}

func (h *Handler) handleUpdateCodeSecurityIntegration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Details        map[string]any `json:"details"`
		IntegrationArn string         `json:"integrationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	integ, updateErr := h.Backend.UpdateCodeSecurityIntegration(req.IntegrationArn, req.Details)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyIntegrationArn: integ.IntegrationArn,
		keyStatus:         integ.Status,
	})
}

func (h *Handler) handleListCodeSecurityIntegrations(c *echo.Context) error {
	integrations, err := h.Backend.ListCodeSecurityIntegrations()
	if err != nil {
		return h.mapError(c, err)
	}

	wire := make([]map[string]any, 0, len(integrations))
	for _, integ := range integrations {
		wire = append(wire, codeSecurityIntegrationToWire(integ))
	}

	return c.JSON(http.StatusOK, map[string]any{"integrations": wire})
}

func (h *Handler) handleCreateCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScopeSettings             map[string]any    `json:"scopeSettings"`
		PeriodicScanConfiguration map[string]any    `json:"periodicScanConfiguration"`
		Tags                      map[string]string `json:"tags"`
		Name                      string            `json:"name"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, createErr := h.Backend.CreateCodeSecurityScanConfiguration(
		req.Name, req.ScopeSettings, req.PeriodicScanConfiguration, req.Tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: cfg.Arn})
}

func (h *Handler) handleDeleteCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if deleteErr := h.Backend.DeleteCodeSecurityScanConfiguration(req.ScanConfigurationArn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, getErr := h.Backend.GetCodeSecurityScanConfiguration(req.ScanConfigurationArn)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, codeSecurityScanConfigToWire(cfg))
}

// codeSecurityScanConfigToWire renders a CodeSecurityScanConfiguration in its
// Inspector2 wire shape. GetCodeSecurityScanConfiguration's real
// createdAt/lastUpdatedAt members are epoch-seconds DateTimeTimestamps (see
// pkgs/awstime); the domain struct's own "updatedAt" JSON tag additionally
// disagrees with the real "lastUpdatedAt" key name, so both must be
// converted here rather than marshaling the struct directly.
func codeSecurityScanConfigToWire(cfg *CodeSecurityScanConfiguration) map[string]any {
	entry := map[string]any{
		keyScanConfigurationArn: cfg.Arn,
		keyName:                 cfg.Name,
		keyStatus:               cfg.Status,
		"createdAt":             awstime.Epoch(cfg.CreatedAt),
		"lastUpdatedAt":         awstime.Epoch(cfg.UpdatedAt),
	}

	if cfg.ScopeSettings != nil {
		entry["scopeSettings"] = cfg.ScopeSettings
	}

	if cfg.PeriodicScanConfig != nil {
		entry["periodicScanConfiguration"] = cfg.PeriodicScanConfig
	}

	if cfg.IntegrationArn != "" {
		entry[keyIntegrationArn] = cfg.IntegrationArn
	}

	if len(cfg.Tags) > 0 {
		entry["tags"] = cfg.Tags
	}

	return entry
}

func (h *Handler) handleUpdateCodeSecurityScanConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScopeSettings             map[string]any `json:"scopeSettings"`
		PeriodicScanConfiguration map[string]any `json:"periodicScanConfiguration"`
		ScanConfigurationArn      string         `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	cfg, updateErr := h.Backend.UpdateCodeSecurityScanConfiguration(
		req.ScanConfigurationArn, req.ScopeSettings, req.PeriodicScanConfiguration,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScanConfigurationArn: cfg.Arn})
}

func (h *Handler) handleListCodeSecurityScanConfigurations(c *echo.Context) error {
	cfgs, err := h.Backend.ListCodeSecurityScanConfigurations()
	if err != nil {
		return h.mapError(c, err)
	}

	wire := make([]map[string]any, 0, len(cfgs))
	for _, cfg := range cfgs {
		wire = append(wire, codeSecurityScanConfigToWire(cfg))
	}

	return c.JSON(http.StatusOK, map[string]any{"scanConfigurations": wire})
}

func (h *Handler) handleBatchAssociateCodeSecurityScanConfiguration( //nolint:dupl // existing issue.
	c *echo.Context,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn           string           `json:"scanConfigurationArn"`
		AssociateConfigurationRequests []map[string]any `json:"associateConfigurationRequests"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	resources := make([]string, 0, len(req.AssociateConfigurationRequests))
	for _, r := range req.AssociateConfigurationRequests {
		if res, ok := r["resource"].(string); ok {
			resources = append(resources, res)
		}
	}

	failed, assocErr := h.Backend.BatchAssociateCodeSecurityScanConfiguration(
		req.ScanConfigurationArn,
		resources,
	)
	if assocErr != nil {
		return h.mapError(c, assocErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"failedAssociations": failed})
}

func (h *Handler) handleBatchDisassociateCodeSecurityScanConfiguration( //nolint:dupl // existing issue.
	c *echo.Context,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn              string           `json:"scanConfigurationArn"`
		DisassociateConfigurationRequests []map[string]any `json:"disassociateConfigurationRequests"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	resources := make([]string, 0, len(req.DisassociateConfigurationRequests))
	for _, r := range req.DisassociateConfigurationRequests {
		if res, ok := r["resource"].(string); ok {
			resources = append(resources, res)
		}
	}

	failed, disErr := h.Backend.BatchDisassociateCodeSecurityScanConfiguration(
		req.ScanConfigurationArn,
		resources,
	)
	if disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"failedDisassociations": failed})
}

func (h *Handler) handleListCodeSecurityScanConfigurationAssociations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanConfigurationArn string `json:"scanConfigurationArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	assocs, listErr := h.Backend.ListCodeSecurityScanConfigurationAssociations(
		req.ScanConfigurationArn,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	if assocs == nil {
		assocs = []*CodeSecurityScanConfigurationAssociation{}
	}

	return c.JSON(http.StatusOK, map[string]any{"associations": assocs})
}

func (h *Handler) handleStartCodeSecurityScan(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceID string `json:"resourceId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, startErr := h.Backend.StartCodeSecurityScan(req.ResourceID)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleGetCodeSecurityScan(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ScanID string `json:"scanId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	scan, getErr := h.Backend.GetCodeSecurityScan(req.ScanID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, scan)
}

func (h *Handler) handleCreateFindingsReport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		S3Destination map[string]any `json:"s3Destination"`
		ReportFormat  string         `json:"reportFormat"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	report, createErr := h.Backend.CreateFindingsReport(req.S3Destination)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyReportID: report.ReportID},
	)
}

func (h *Handler) handleCancelFindingsReport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if cancelErr := h.Backend.CancelFindingsReport(req.ReportID); cancelErr != nil {
		return h.mapError(c, cancelErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyReportID: req.ReportID})
}

func (h *Handler) handleGetFindingsReportStatus(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	report, getErr := h.Backend.GetFindingsReportStatus(req.ReportID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportID:  report.ReportID,
		keyStatus:    report.Status,
		keyErrorCode: report.ErrorCode,
	})
}

func (h *Handler) handleCreateSbomExport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		S3Destination map[string]any `json:"s3Destination"`
		SbomFormat    string         `json:"sbomFormat"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	export, createErr := h.Backend.CreateSbomExport(req.S3Destination)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyReportID: export.ReportID})
}

func (h *Handler) handleCancelSbomExport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if cancelErr := h.Backend.CancelSbomExport(req.ReportID); cancelErr != nil {
		return h.mapError(c, cancelErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyReportID: req.ReportID})
}

func (h *Handler) handleGetSbomExport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	export, getErr := h.Backend.GetSbomExport(req.ReportID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportID:  export.ReportID,
		keyStatus:    export.Status,
		keyErrorCode: export.ErrorCode,
	})
}

func (h *Handler) handleListCoverage(c *echo.Context) error {
	req, ok := decodeFilterListRequest(c)
	if !ok {
		return nil
	}

	entries, nextToken, listErr := h.Backend.ListCoverage(req.FilterCriteria, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	resp := map[string]any{"coveredResources": entries}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListCoverageStatistics(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	stats, statsErr := h.Backend.ListCoverageStatistics(req.FilterCriteria)
	if statsErr != nil {
		return h.mapError(c, statsErr)
	}

	return c.JSON(http.StatusOK, stats)
}

func (h *Handler) handleListFindingAggregations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AggregationRequest map[string]any `json:"aggregationRequest"`
		AggregationType    string         `json:"aggregationType"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	result, aggErr := h.Backend.ListFindingAggregations(req.AggregationType, req.AggregationRequest)
	if aggErr != nil {
		return h.mapError(c, aggErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleListUsageTotals(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	totals, usageErr := h.Backend.ListUsageTotals(req.AccountIDs)
	if usageErr != nil {
		return h.mapError(c, usageErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"totals": totals})
}

func (h *Handler) handleListAccountPermissions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Service string `json:"service"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	perms, listErr := h.Backend.ListAccountPermissions(req.Service)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	if perms == nil {
		perms = []*AccountPermission{}
	}

	return c.JSON(http.StatusOK, map[string]any{"permissions": perms})
}

func (h *Handler) handleSearchVulnerabilities(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
		NextToken      string         `json:"nextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	vulns, nextToken, searchErr := h.Backend.SearchVulnerabilities(
		req.FilterCriteria,
		req.NextToken,
	)
	if searchErr != nil {
		return h.mapError(c, searchErr)
	}

	resp := map[string]any{"vulnerabilities": vulns}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchGetCodeSnippet(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FindingArns []string `json:"findingArns"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, getErr := h.Backend.BatchGetCodeSnippet(req.FindingArns)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleBatchGetFindingDetails(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FindingArns []map[string]any `json:"findingArns"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, getErr := h.Backend.BatchGetFindingDetails(req.FindingArns)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleBatchGetFreeTrialInfo(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, getErr := h.Backend.BatchGetFreeTrialInfo(req.AccountIDs)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleGetClustersForImage(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	result, getErr := h.Backend.GetClustersForImage(req.FilterCriteria)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}
