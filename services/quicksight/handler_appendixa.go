package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchNew handles operations added in Appendix A that are not yet
// fully implemented. It returns a "not implemented" error for all ops.
func (h *Handler) dispatchNew(c *echo.Context, op string) error {
	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// classifyAccountSubscriptionPaths routes /account/{accountId} paths.
func classifyAccountSubscriptionPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyNamespaceSingularPaths routes /accounts/{id}/namespace/{ns}/... paths.
func classifyNamespaceSingularPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyFolderPaths routes /accounts/{id}/folders/... paths.
func classifyFolderPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyTemplatePaths routes /accounts/{id}/templates/... paths.
func classifyTemplatePaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyThemePaths routes /accounts/{id}/themes/... paths.
func classifyThemePaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyTopicPaths routes /accounts/{id}/topics/... paths.
func classifyTopicPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyVPCConnectionPaths routes /accounts/{id}/vpc-connections/... paths.
func classifyVPCConnectionPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyActionConnectorPaths routes /accounts/{id}/action-connectors/... paths.
func classifyActionConnectorPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyBrandPaths routes /accounts/{id}/brands/... paths.
func classifyBrandPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyBrandAssignmentPaths routes /accounts/{id}/brandassignments/... paths.
func classifyBrandAssignmentPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyCustomPermissionsPaths routes /accounts/{id}/custom-permissions/... paths.
func classifyCustomPermissionsPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyOAuthAppPaths routes /accounts/{id}/oauth-client-applications/... paths.
func classifyOAuthAppPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyIdentityPropagationPaths routes /accounts/{id}/identity-propagation-config/... paths.
func classifyIdentityPropagationPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyAssetBundleExportPaths routes /accounts/{id}/asset-bundle-export-jobs/... paths.
func classifyAssetBundleExportPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyAssetBundleImportPaths routes /accounts/{id}/asset-bundle-import-jobs/... paths.
func classifyAssetBundleImportPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyAutomationPaths routes /accounts/{id}/automation-groups/... paths.
func classifyAutomationPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyFlowPaths routes /accounts/{id}/flows/... paths.
func classifyFlowPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyResourceFoldersPaths routes /accounts/{id}/resource/{id}/folders/... paths.
func classifyResourceFoldersPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyCustomizationPaths routes /accounts/{id}/customizations/... paths.
func classifyCustomizationPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyAccountCustomPermissionPaths routes /accounts/{id}/custom-permission/... paths.
func classifyAccountCustomPermissionPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyAccountSettingsPaths routes /accounts/{id}/settings/... paths.
func classifyAccountSettingsPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyDashboardsQAPaths routes /accounts/{id}/dashboards-qa-configuration/... paths.
func classifyDashboardsQAPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyDefaultQBizPaths routes /accounts/{id}/default-qbusiness-application/... paths.
func classifyDefaultQBizPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyIPRestrictionPaths routes /accounts/{id}/ip-restriction/... paths.
func classifyIPRestrictionPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyKeyRegistrationPaths routes /accounts/{id}/key-registration/... paths.
func classifyKeyRegistrationPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyQPersonalizationPaths routes /accounts/{id}/q-personalization-configuration/... paths.
func classifyQPersonalizationPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyQSearchConfigPaths routes /accounts/{id}/quicksight-q-search-configuration/... paths.
func classifyQSearchConfigPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}

// classifyEmbedUrlPaths routes /accounts/{id}/embed-url/... paths.
func classifyEmbedUrlPaths(_ string, _ []string, _ int) (string, string) {
	return opUnknown, ""
}
