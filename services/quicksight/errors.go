package quicksight

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Package-level sentinel errors for QuickSight backend operations.

const (
	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errResourceExists    = "ResourceExistsException"
	errValidation        = "InvalidParameterValueException"
)

var (
	// ErrNamespaceNotFound is returned when a namespace does not exist.
	ErrNamespaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrNamespaceAlreadyExists is returned when a namespace already exists.
	ErrNamespaceAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrGroupNotFound is returned when a group does not exist.
	ErrGroupNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrGroupAlreadyExists is returned when a group already exists.
	ErrGroupAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrGroupMemberNotFound is returned when a group member does not exist.
	ErrGroupMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrGroupMemberAlreadyExists is returned when a group member already exists.
	ErrGroupMemberAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a user already exists.
	ErrUserAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrDataSourceNotFound is returned when a data source does not exist.
	ErrDataSourceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDataSourceAlreadyExists is returned when a data source already exists.
	ErrDataSourceAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrDataSetNotFound is returned when a dataset does not exist.
	ErrDataSetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDataSetAlreadyExists is returned when a dataset already exists.
	ErrDataSetAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrIngestionNotFound is returned when an ingestion does not exist.
	ErrIngestionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIngestionAlreadyExists is returned when an ingestion already exists.
	ErrIngestionAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrDashboardNotFound is returned when a dashboard does not exist.
	ErrDashboardNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDashboardAlreadyExists is returned when a dashboard already exists.
	ErrDashboardAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrAnalysisNotFound is returned when an analysis does not exist.
	ErrAnalysisNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAnalysisAlreadyExists is returned when an analysis already exists.
	ErrAnalysisAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrFolderNotFound is returned when a folder does not exist.
	ErrFolderNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFolderAlreadyExists is returned when a folder already exists.
	ErrFolderAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrFolderMemberNotFound is returned when a folder membership does not exist.
	ErrFolderMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTemplateNotFound is returned when a template (or template version) does not exist.
	ErrTemplateNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTemplateAlreadyExists is returned when a template already exists.
	ErrTemplateAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrTemplateAliasNotFound is returned when a template alias does not exist.
	ErrTemplateAliasNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTemplateAliasAlreadyExists is returned when a template alias already exists.
	ErrTemplateAliasAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrThemeNotFound is returned when a theme (or theme version) does not exist.
	ErrThemeNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrThemeAlreadyExists is returned when a theme already exists.
	ErrThemeAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrThemeAliasNotFound is returned when a theme alias does not exist.
	ErrThemeAliasNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrThemeAliasAlreadyExists is returned when a theme alias already exists.
	ErrThemeAliasAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrTopicNotFound is returned when a topic does not exist.
	ErrTopicNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTopicAlreadyExists is returned when a topic already exists.
	ErrTopicAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrTopicRefreshScheduleNotFound is returned when a topic refresh schedule does not exist.
	ErrTopicRefreshScheduleNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTopicRefreshScheduleAlreadyExists is returned when a topic refresh schedule already exists.
	ErrTopicRefreshScheduleAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrVPCConnectionNotFound is returned when a VPC connection does not exist.
	ErrVPCConnectionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrVPCConnectionAlreadyExists is returned when a VPC connection already exists.
	ErrVPCConnectionAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrIAMPolicyAssignmentNotFound is returned when an IAM policy assignment does not exist.
	ErrIAMPolicyAssignmentNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIAMPolicyAssignmentAlreadyExists is returned when an IAM policy assignment already exists.
	ErrIAMPolicyAssignmentAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrAccountSubscriptionNotFound is returned when an account has no QuickSight subscription.
	ErrAccountSubscriptionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAccountSubscriptionAlreadyExists is returned when an account is already subscribed.
	ErrAccountSubscriptionAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrAccountCustomizationNotFound is returned when an account (or namespace) customization
	// does not exist.
	ErrAccountCustomizationNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAccountCustomizationAlreadyExists is returned when an account (or namespace)
	// customization already exists.
	ErrAccountCustomizationAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrAccountCustomPermissionNotFound is returned when an account has no custom
	// permissions profile applied.
	ErrAccountCustomPermissionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDefaultQBusinessApplicationNotFound is returned when no default Q Business
	// application is configured for an account.
	ErrDefaultQBusinessApplicationNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrBrandNotFound is returned when a brand does not exist.
	ErrBrandNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrBrandAlreadyExists is returned when a brand already exists.
	ErrBrandAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrBrandVersionNotFound is returned when a brand version does not exist.
	ErrBrandVersionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrBrandInUse is returned when a brand cannot be deleted because it is assigned.
	ErrBrandInUse = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrCustomPermissionsNotFound is returned when a custom permissions profile does
	// not exist.
	ErrCustomPermissionsNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCustomPermissionsAlreadyExists is returned when a custom permissions profile
	// already exists.
	ErrCustomPermissionsAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrCustomPermissionsInUse is returned when a custom permissions profile cannot
	// be deleted because it is assigned to a role or user.
	ErrCustomPermissionsInUse = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrRoleCustomPermissionNotFound is returned when a role has no custom
	// permissions assigned.
	ErrRoleCustomPermissionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrRoleMembershipAlreadyExists is returned when a group is already a member of
	// a role.
	ErrRoleMembershipAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrRoleMembershipNotFound is returned when a group is not a member of a role.
	ErrRoleMembershipNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrUserCustomPermissionNotFound is returned when a user has no custom
	// permissions assigned.
	ErrUserCustomPermissionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrOAuthClientAppNotFound is returned when an OAuth client application does not
	// exist.
	ErrOAuthClientAppNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrOAuthClientAppAlreadyExists is returned when an OAuth client application
	// already exists.
	ErrOAuthClientAppAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrIdentityPropagationConfigNotFound is returned when an identity propagation
	// configuration does not exist for the given service.
	ErrIdentityPropagationConfigNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAssetBundleExportJobNotFound is returned when an asset bundle export job does
	// not exist.
	ErrAssetBundleExportJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAssetBundleImportJobNotFound is returned when an asset bundle import job does
	// not exist.
	ErrAssetBundleImportJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDashboardSnapshotJobNotFound is returned when a dashboard snapshot job does
	// not exist.
	ErrDashboardSnapshotJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrRefreshScheduleNotFound is returned when a dataset refresh schedule does not
	// exist.
	ErrRefreshScheduleNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrRefreshScheduleAlreadyExists is returned when a dataset refresh schedule
	// already exists.
	ErrRefreshScheduleAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrDataSetRefreshPropertiesNotFound is returned when a dataset has no refresh
	// properties configured.
	ErrDataSetRefreshPropertiesNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
	// ErrUnknownOperation is returned when the requested operation is not implemented.
	ErrUnknownOperation = errors.New("unknown operation")
	// ErrActionConnectorNotFound is returned when an action connector does not exist.
	ErrActionConnectorNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrActionConnectorAlreadyExists is returned when an action connector already exists.
	ErrActionConnectorAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrAutomationJobNotFound is returned when an automation job does not exist.
	ErrAutomationJobNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFlowNotFound is returned when a flow does not exist.
	ErrFlowNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDashboardVersionNotFound is returned when a dashboard version does not exist.
	ErrDashboardVersionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrSelfUpgradeRequestNotFound is returned when a self-upgrade request does not exist.
	ErrSelfUpgradeRequestNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTaggableResourceNotFound is returned by TagResource/UntagResource/
	// ListTagsForResource when the given ARN doesn't identify a resource
	// this backend actually holds.
	ErrTaggableResourceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIngestionNotCancellable is returned by CancelIngestion when the
	// target ingestion is already in a terminal state (COMPLETED, FAILED, or
	// CANCELLED) and so can no longer be cancelled.
	ErrIngestionNotCancellable = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrAgentNotFound is returned when an agent does not exist.
	ErrAgentNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAgentAlreadyExists is returned when an agent already exists.
	ErrAgentAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrKnowledgeBaseNotFound is returned when a knowledge base does not exist.
	ErrKnowledgeBaseNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrKnowledgeBaseAlreadyExists is returned when a knowledge base already exists.
	ErrKnowledgeBaseAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrSpaceNotFound is returned when a space does not exist.
	ErrSpaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrSpaceAlreadyExists is returned when a space already exists.
	ErrSpaceAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
)
