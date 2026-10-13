package quicksight

import (
	"errors"
	"strings"
)

type sentinelMessage struct {
	err error
	msg string
}

func sentinelMessages() []sentinelMessage {
	return []sentinelMessage{
		{ErrNamespaceNotFound, "The specified namespace was not found."},
		{ErrNamespaceAlreadyExists, "The specified namespace already exists."},
		{ErrGroupNotFound, "The specified group was not found."},
		{ErrGroupAlreadyExists, "The specified group already exists."},
		{ErrGroupMemberNotFound, "The specified group member was not found."},
		{ErrGroupMemberAlreadyExists, "The specified group member already exists."},
		{ErrUserNotFound, "The specified user was not found."},
		{ErrUserAlreadyExists, "The specified user already exists."},
		{ErrDataSourceNotFound, "The specified data source was not found."},
		{ErrDataSourceAlreadyExists, "The specified data source already exists."},
		{ErrDataSetNotFound, "The specified dataset was not found."},
		{ErrDataSetAlreadyExists, "The specified dataset already exists."},
		{ErrIngestionNotFound, "The specified ingestion was not found."},
		{ErrIngestionAlreadyExists, "The specified ingestion already exists."},
		{ErrDashboardNotFound, "The specified dashboard was not found."},
		{ErrDashboardAlreadyExists, "The specified dashboard already exists."},
		{ErrAnalysisNotFound, "The specified analysis was not found."},
		{ErrAnalysisAlreadyExists, "The specified analysis already exists."},
		{ErrFolderNotFound, "The specified folder was not found."},
		{ErrFolderAlreadyExists, "The specified folder already exists."},
		{ErrFolderMemberNotFound, "The specified folder member was not found."},
		{ErrTemplateNotFound, "The specified template was not found."},
		{ErrTemplateAlreadyExists, "The specified template already exists."},
		{ErrTemplateAliasNotFound, "The specified template alias was not found."},
		{ErrTemplateAliasAlreadyExists, "The specified template alias already exists."},
		{ErrThemeNotFound, "The specified theme was not found."},
		{ErrThemeAlreadyExists, "The specified theme already exists."},
		{ErrThemeAliasNotFound, "The specified theme alias was not found."},
		{ErrThemeAliasAlreadyExists, "The specified theme alias already exists."},
		{ErrTopicNotFound, "The specified topic was not found."},
		{ErrTopicAlreadyExists, "The specified topic already exists."},
		{ErrTopicRefreshScheduleNotFound, "The specified topic refresh schedule was not found."},
		{ErrTopicRefreshScheduleAlreadyExists, "The specified topic refresh schedule already exists."},
		{ErrVPCConnectionNotFound, "The specified VPCConnection was not found."},
		{ErrVPCConnectionAlreadyExists, "The specified VPCConnection already exists."},
		{ErrIAMPolicyAssignmentNotFound, "The specified IAMPolicy Assignment was not found."},
		{ErrIAMPolicyAssignmentAlreadyExists, "The specified IAMPolicy Assignment already exists."},
		{ErrAccountSubscriptionNotFound, "The specified account subscription was not found."},
		{ErrAccountSubscriptionAlreadyExists, "The specified account subscription already exists."},
		{ErrAccountCustomizationNotFound, "The specified account customization was not found."},
		{ErrAccountCustomizationAlreadyExists, "The specified account customization already exists."},
		{ErrAccountCustomPermissionNotFound, "The specified account custom permission was not found."},
		{ErrDefaultQBusinessApplicationNotFound, "The specified Default QBusiness Application was not found."},
		{ErrBrandNotFound, "The specified brand was not found."},
		{ErrBrandAlreadyExists, "The specified brand already exists."},
		{ErrBrandVersionNotFound, "The specified brand version was not found."},
		{ErrCustomPermissionsNotFound, "The specified custom permissions was not found."},
		{ErrCustomPermissionsAlreadyExists, "The specified custom permissions already exists."},
		{ErrRoleCustomPermissionNotFound, "The specified role custom permission was not found."},
		{ErrRoleMembershipAlreadyExists, "The specified role membership already exists."},
		{ErrRoleMembershipNotFound, "The specified role membership was not found."},
		{ErrUserCustomPermissionNotFound, "The specified user custom permission was not found."},
		{ErrOAuthClientAppNotFound, "The specified OAuth Client App was not found."},
		{ErrOAuthClientAppAlreadyExists, "The specified OAuth Client App already exists."},
		{ErrIdentityPropagationConfigNotFound, "The specified identity propagation config was not found."},
		{ErrAssetBundleExportJobNotFound, "The specified asset bundle export job was not found."},
		{ErrAssetBundleImportJobNotFound, "The specified asset bundle import job was not found."},
		{ErrDashboardSnapshotJobNotFound, "The specified dashboard snapshot job was not found."},
		{ErrRefreshScheduleNotFound, "The specified refresh schedule was not found."},
		{ErrRefreshScheduleAlreadyExists, "The specified refresh schedule already exists."},
		{ErrDataSetRefreshPropertiesNotFound, "The specified dataset refresh properties was not found."},
		{ErrActionConnectorNotFound, "The specified action connector was not found."},
		{ErrActionConnectorAlreadyExists, "The specified action connector already exists."},
		{ErrAutomationJobNotFound, "The specified automation job was not found."},
		{ErrFlowNotFound, "The specified flow was not found."},
		{ErrDashboardVersionNotFound, "The specified dashboard version was not found."},
		{ErrSelfUpgradeRequestNotFound, "The specified self upgrade request was not found."},
		{ErrTaggableResourceNotFound, "The specified taggable resource was not found."},
		{ErrAgentNotFound, "The specified agent was not found."},
		{ErrAgentAlreadyExists, "The specified agent already exists."},
		{ErrKnowledgeBaseNotFound, "The specified knowledge base was not found."},
		{ErrKnowledgeBaseAlreadyExists, "The specified knowledge base already exists."},
		{ErrSpaceNotFound, "The specified space was not found."},
		{ErrSpaceAlreadyExists, "The specified space already exists."},
		{ErrApprovalPolicyNotFound, "The specified approval policy was not found."},
		{ErrApprovalPolicyAlreadyExists, "The specified approval policy already exists."},
		{ErrDlpSettingNotFound, "The specified dlp setting was not found."},
		{ErrDlpSettingAlreadyExists, "The specified dlp setting already exists."},
		{ErrLimitsProfileNotFound, "The specified limits profile was not found."},
		{ErrAppNotFound, "The specified app was not found."},
	}
}

func errCodePrefixes() []string {
	return []string{
		errResourceNotFound, errResourceExists, errConflictException, errValidation, errSessionLifetimeInvalid,
	}
}

// errMessage strips the leading error code and fills in a readable message for bare-code sentinels.
func errMessage(err error) string {
	msg := err.Error()

	for _, code := range errCodePrefixes() {
		if rest, ok := strings.CutPrefix(msg, code+": "); ok {
			return rest
		}
	}

	for _, code := range errCodePrefixes() {
		if msg != code {
			continue
		}

		for _, m := range sentinelMessages() {
			if errors.Is(err, m.err) {
				return m.msg
			}
		}
	}

	return msg
}
