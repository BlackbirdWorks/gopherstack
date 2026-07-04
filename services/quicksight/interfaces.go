package quicksight

import "time"

// StorageBackend is the interface for QuickSight storage operations.
type StorageBackend interface {
	// Namespaces
	CreateNamespace(accountID, namespace, capacityRegion string) (*Namespace, error)
	DescribeNamespace(accountID, namespace string) (*Namespace, error)
	DeleteNamespace(accountID, namespace string) error
	ListNamespaces(accountID string, maxResults int32, nextToken string) ([]*Namespace, string, error)

	// Groups
	CreateGroup(accountID, namespace, groupName, description string) (*Group, error)
	DescribeGroup(accountID, namespace, groupName string) (*Group, error)
	UpdateGroup(accountID, namespace, groupName, description string) (*Group, error)
	DeleteGroup(accountID, namespace, groupName string) error
	ListGroups(accountID, namespace string, maxResults int32, nextToken string) ([]*Group, string, error)
	SearchGroups(accountID, namespace, query string, maxResults int32, nextToken string) ([]*Group, string, error)

	// Group Memberships
	CreateGroupMembership(accountID, namespace, groupName, memberName string) (*GroupMember, error)
	DescribeGroupMembership(accountID, namespace, groupName, memberName string) (*GroupMember, error)
	DeleteGroupMembership(accountID, namespace, groupName, memberName string) error
	ListGroupMemberships(
		accountID, namespace, groupName string,
		maxResults int32,
		nextToken string,
	) ([]*GroupMember, string, error)

	// Users
	RegisterUser(accountID, namespace, userName, email, role, identityType, sessionName string) (*User, error)
	DescribeUser(accountID, namespace, userName string) (*User, error)
	UpdateUser(accountID, namespace, userName, email, role string) (*User, error)
	DeleteUser(accountID, namespace, userName string) error
	DeleteUserByPrincipalID(accountID, namespace, principalID string) error
	ListUsers(accountID, namespace string, maxResults int32, nextToken string) ([]*User, string, error)
	ListUserGroups(accountID, namespace, userName string, maxResults int32, nextToken string) ([]*Group, string, error)

	// DataSources
	CreateDataSource(
		accountID, dataSourceID, name, dsType string,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*DataSource, error)
	DescribeDataSource(accountID, dataSourceID string) (*DataSource, error)
	UpdateDataSource(accountID, dataSourceID, name string) (*DataSource, error)
	DeleteDataSource(accountID, dataSourceID string) error
	ListDataSources(accountID string, maxResults int32, nextToken string) ([]*DataSource, string, error)
	SearchDataSources(
		accountID string,
		filters []SearchFilter,
		maxResults int32,
		nextToken string,
	) ([]*DataSource, string, error)
	DescribeDataSourcePermissions(accountID, dataSourceID string) (*DataSource, []ResourcePermission, error)
	UpdateDataSourcePermissions(
		accountID, dataSourceID string,
		grant, revoke []ResourcePermission,
	) (*DataSource, []ResourcePermission, error)

	// DataSets
	CreateDataSet(
		accountID, dataSetID, name, importMode string,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*DataSet, error)
	DescribeDataSet(accountID, dataSetID string) (*DataSet, error)
	UpdateDataSet(accountID, dataSetID, name, importMode string) (*DataSet, error)
	DeleteDataSet(accountID, dataSetID string) error
	ListDataSets(accountID string, maxResults int32, nextToken string) ([]*DataSet, string, error)
	SearchDataSets(
		accountID string,
		filters []SearchFilter,
		maxResults int32,
		nextToken string,
	) ([]*DataSet, string, error)
	DescribeDataSetPermissions(accountID, dataSetID string) (*DataSet, []ResourcePermission, error)
	UpdateDataSetPermissions(
		accountID, dataSetID string,
		grant, revoke []ResourcePermission,
	) (*DataSet, []ResourcePermission, error)

	// Ingestions
	CreateIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error)
	DescribeIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error)
	CancelIngestion(accountID, dataSetID, ingestionID string) error
	ListIngestions(accountID, dataSetID string, maxResults int32, nextToken string) ([]*Ingestion, string, error)

	// Dashboards
	CreateDashboard(
		accountID, dashboardID, name string,
		definition map[string]any,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*Dashboard, error)
	DescribeDashboard(accountID, dashboardID string) (*Dashboard, error)
	UpdateDashboard(accountID, dashboardID, name string, definition map[string]any) (*Dashboard, error)
	DeleteDashboard(accountID, dashboardID string) error
	ListDashboards(accountID string, maxResults int32, nextToken string) ([]*Dashboard, string, error)
	ListDashboardVersions(
		accountID, dashboardID string,
		maxResults int32,
		nextToken string,
	) ([]*DashboardVersion, string, error)
	SearchDashboards(
		accountID string,
		filters []SearchFilter,
		maxResults int32,
		nextToken string,
	) ([]*Dashboard, string, error)
	DescribeDashboardPermissions(accountID, dashboardID string) (*Dashboard, []ResourcePermission, error)
	UpdateDashboardPermissions(
		accountID, dashboardID string,
		grant, revoke []ResourcePermission,
	) (*Dashboard, []ResourcePermission, error)

	// Analyses
	CreateAnalysis(
		accountID, analysisID, name string,
		definition map[string]any,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*Analysis, error)
	DescribeAnalysis(accountID, analysisID string) (*Analysis, error)
	UpdateAnalysis(accountID, analysisID, name string, definition map[string]any) (*Analysis, error)
	DeleteAnalysis(accountID, analysisID string, forceDeleteWithoutRecovery bool) error
	ListAnalyses(accountID string, maxResults int32, nextToken string) ([]*Analysis, string, error)
	RestoreAnalysis(accountID, analysisID string) (*Analysis, error)
	SearchAnalyses(
		accountID string,
		filters []SearchFilter,
		maxResults int32,
		nextToken string,
	) ([]*Analysis, string, error)
	DescribeAnalysisPermissions(accountID, analysisID string) (*Analysis, []ResourcePermission, error)
	UpdateAnalysisPermissions(
		accountID, analysisID string,
		grant, revoke []ResourcePermission,
	) (*Analysis, []ResourcePermission, error)

	// Tags
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Folders
	CreateFolder(
		accountID, folderID, name, folderType, parentFolderArn string,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*Folder, error)
	DescribeFolder(accountID, folderID string) (*Folder, error)
	UpdateFolder(accountID, folderID, name string) (*Folder, error)
	DeleteFolder(accountID, folderID string) error
	ListFolders(accountID string, maxResults int32, nextToken string) ([]*Folder, string, error)
	SearchFolders(
		accountID string,
		filters []FolderSearchFilter,
		maxResults int32,
		nextToken string,
	) ([]*Folder, string, error)

	// Folder memberships
	CreateFolderMembership(accountID, folderID, memberID, memberType string) (*FolderMember, error)
	DeleteFolderMembership(accountID, folderID, memberID, memberType string) error
	ListFolderMembers(
		accountID, folderID string,
		maxResults int32,
		nextToken string,
	) ([]*FolderMember, string, error)

	// Folder permissions
	DescribeFolderPermissions(accountID, folderID string) ([]ResourcePermission, error)
	UpdateFolderPermissions(
		accountID, folderID string,
		grant, revoke []ResourcePermission,
	) ([]ResourcePermission, error)
	DescribeFolderResolvedPermissions(accountID, folderID string) ([]ResourcePermission, error)

	// Folders-for-resource
	ListFoldersForResource(
		accountID, resourceArn string,
		maxResults int32,
		nextToken string,
	) ([]string, string, error)

	// Templates
	CreateTemplate(
		accountID, templateID, name, sourceEntityArn string,
		definition map[string]any,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*Template, error)
	DescribeTemplate(accountID, templateID string, versionNumber int64) (*Template, error)
	UpdateTemplate(
		accountID, templateID, name, sourceEntityArn string,
		definition map[string]any,
	) (*Template, error)
	DeleteTemplate(accountID, templateID string, versionNumber int64) error
	ListTemplates(accountID string, maxResults int32, nextToken string) ([]*Template, string, error)
	ListTemplateVersions(
		accountID, templateID string,
		maxResults int32,
		nextToken string,
	) ([]*TemplateVersion, string, error)
	DescribeTemplatePermissions(accountID, templateID string) (*Template, []ResourcePermission, error)
	UpdateTemplatePermissions(
		accountID, templateID string,
		grant, revoke []ResourcePermission,
	) (*Template, []ResourcePermission, error)

	// Template aliases
	CreateTemplateAlias(accountID, templateID, aliasName string, versionNumber int64) (*TemplateAlias, error)
	DescribeTemplateAlias(accountID, templateID, aliasName string) (*TemplateAlias, error)
	UpdateTemplateAlias(accountID, templateID, aliasName string, versionNumber int64) (*TemplateAlias, error)
	DeleteTemplateAlias(accountID, templateID, aliasName string) error
	ListTemplateAliases(
		accountID, templateID string,
		maxResults int32,
		nextToken string,
	) ([]*TemplateAlias, string, error)

	// Themes
	CreateTheme(
		accountID, themeID, name, baseThemeID string,
		configuration map[string]any,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*Theme, error)
	DescribeTheme(accountID, themeID string, versionNumber int64) (*Theme, error)
	UpdateTheme(
		accountID, themeID, name, baseThemeID string,
		configuration map[string]any,
	) (*Theme, error)
	DeleteTheme(accountID, themeID string, versionNumber int64) error
	ListThemes(accountID string, maxResults int32, nextToken string) ([]*Theme, string, error)
	ListThemeVersions(
		accountID, themeID string,
		maxResults int32,
		nextToken string,
	) ([]*ThemeVersion, string, error)
	DescribeThemePermissions(accountID, themeID string) (*Theme, []ResourcePermission, error)
	UpdateThemePermissions(
		accountID, themeID string,
		grant, revoke []ResourcePermission,
	) (*Theme, []ResourcePermission, error)

	// Theme aliases
	CreateThemeAlias(accountID, themeID, aliasName string, versionNumber int64) (*ThemeAlias, error)
	DescribeThemeAlias(accountID, themeID, aliasName string) (*ThemeAlias, error)
	UpdateThemeAlias(accountID, themeID, aliasName string, versionNumber int64) (*ThemeAlias, error)
	DeleteThemeAlias(accountID, themeID, aliasName string) error
	ListThemeAliases(
		accountID, themeID string,
		maxResults int32,
		nextToken string,
	) ([]*ThemeAlias, string, error)

	// Topics
	CreateTopic(
		accountID, topicID, name, description, userExperienceVersion string,
		dataSets []map[string]any,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*Topic, error)
	DescribeTopic(accountID, topicID string) (*Topic, error)
	UpdateTopic(
		accountID, topicID, name, description, userExperienceVersion string,
		dataSets []map[string]any,
	) (*Topic, error)
	DeleteTopic(accountID, topicID string) error
	ListTopics(accountID string, maxResults int32, nextToken string) ([]*Topic, string, error)

	// Topic permissions
	DescribeTopicPermissions(accountID, topicID string) (*Topic, []ResourcePermission, error)
	UpdateTopicPermissions(
		accountID, topicID string,
		grant, revoke []ResourcePermission,
	) (*Topic, []ResourcePermission, error)

	// Topic refresh
	DescribeTopicRefresh(accountID, topicID, refreshID string) (*TopicRefreshDetails, error)

	// Topic refresh schedules
	CreateTopicRefreshSchedule(
		accountID, topicID, datasetID, datasetArn, refreshType string,
		isEnabled bool,
		scheduleConfig map[string]any,
	) (*TopicRefreshSchedule, error)
	DescribeTopicRefreshSchedule(accountID, topicID, datasetID string) (*TopicRefreshSchedule, error)
	UpdateTopicRefreshSchedule(
		accountID, topicID, datasetID, refreshType string,
		isEnabled *bool,
		scheduleConfig map[string]any,
	) (*TopicRefreshSchedule, error)
	DeleteTopicRefreshSchedule(accountID, topicID, datasetID string) error
	ListTopicRefreshSchedules(accountID, topicID string) ([]*TopicRefreshSchedule, error)

	// Topic reviewed answers
	BatchCreateTopicReviewedAnswer(
		accountID, topicID string,
		answers []map[string]any,
	) ([]*TopicReviewedAnswer, []TopicAnswerError, error)
	BatchDeleteTopicReviewedAnswer(
		accountID, topicID string,
		answerIDs []string,
	) ([]string, []TopicAnswerError, error)
	ListTopicReviewedAnswers(accountID, topicID string) ([]*TopicReviewedAnswer, error)

	// VPC connections
	CreateVPCConnection(
		accountID, vpcConnectionID, name, vpcID string,
		subnetIDs, securityGroupIDs, dnsResolvers []string,
		roleArn string,
		tags map[string]string,
	) (*VPCConnection, error)
	DescribeVPCConnection(accountID, vpcConnectionID string) (*VPCConnection, error)
	UpdateVPCConnection(
		accountID, vpcConnectionID, name string,
		subnetIDs, securityGroupIDs, dnsResolvers []string,
		roleArn string,
	) (*VPCConnection, error)
	DeleteVPCConnection(accountID, vpcConnectionID string) error
	ListVPCConnections(accountID string, maxResults int32, nextToken string) ([]*VPCConnection, string, error)

	// IAM policy assignments
	CreateIAMPolicyAssignment(
		accountID, namespace, assignmentName, assignmentStatus, policyArn string,
		identities map[string][]string,
	) (*IAMPolicyAssignment, error)
	DescribeIAMPolicyAssignment(accountID, namespace, assignmentName string) (*IAMPolicyAssignment, error)
	UpdateIAMPolicyAssignment(
		accountID, namespace, assignmentName, assignmentStatus, policyArn string,
		identities map[string][]string,
	) (*IAMPolicyAssignment, error)
	DeleteIAMPolicyAssignment(accountID, namespace, assignmentName string) error
	ListIAMPolicyAssignments(
		accountID, namespace, statusFilter string,
		maxResults int32,
		nextToken string,
	) ([]*IAMPolicyAssignment, string, error)
	ListIAMPolicyAssignmentsForUser(
		accountID, namespace, userName string,
		maxResults int32,
		nextToken string,
	) ([]*IAMPolicyAssignment, string, error)

	// Account settings
	DescribeAccountSettings(accountID string) (*AccountSettings, error)
	UpdateAccountSettings(
		accountID, defaultNamespace, notificationEmail string,
		terminationProtectionEnabled *bool,
	) (*AccountSettings, error)

	// Account subscription
	CreateAccountSubscription(
		accountID, accountName, edition, authenticationMethod, notificationEmail string,
	) (*AccountSubscription, error)
	DescribeAccountSubscription(accountID string) (*AccountSubscription, error)
	DeleteAccountSubscription(accountID string) error

	// Account customization
	CreateAccountCustomization(
		accountID, namespace, defaultTheme, defaultEmailCustomizationTemplate string,
	) (*AccountCustomization, error)
	DescribeAccountCustomization(accountID, namespace string) (*AccountCustomization, error)
	UpdateAccountCustomization(
		accountID, namespace, defaultTheme, defaultEmailCustomizationTemplate string,
	) (*AccountCustomization, error)
	DeleteAccountCustomization(accountID, namespace string) error

	// Account custom permission
	DescribeAccountCustomPermission(accountID string) (string, error)
	UpdateAccountCustomPermission(accountID, customPermissionsName string) error
	DeleteAccountCustomPermission(accountID string) error

	// IP restriction
	DescribeIPRestriction(accountID string) (*IPRestriction, error)
	UpdateIPRestriction(
		accountID string,
		ruleMap, vpcIDRuleMap, vpcEndpointIDRuleMap map[string]string,
		enabled *bool,
	) (*IPRestriction, error)

	// Public sharing
	UpdatePublicSharingSettings(accountID string, enabled bool) error

	// Key registration
	DescribeKeyRegistration(accountID string) ([]RegisteredCustomerManagedKey, error)
	UpdateKeyRegistration(
		accountID string,
		keys []RegisteredCustomerManagedKey,
	) ([]RegisteredCustomerManagedKey, error)

	// Default Q Business Application
	DescribeDefaultQBusinessApplication(accountID, namespace string) (*DefaultQBusinessApplication, error)
	UpdateDefaultQBusinessApplication(
		accountID, applicationID, namespace string,
	) (*DefaultQBusinessApplication, error)
	DeleteDefaultQBusinessApplication(accountID, namespace string) error

	// Q personalization
	DescribeQPersonalizationConfiguration(accountID string) (string, error)
	UpdateQPersonalizationConfiguration(accountID, mode string) (string, error)

	// Q Search configuration
	DescribeQuickSightQSearchConfiguration(accountID string) (string, error)
	UpdateQuickSightQSearchConfiguration(accountID, status string) (string, error)

	// Dashboards Q&A configuration
	DescribeDashboardsQAConfiguration(accountID string) (string, error)
	UpdateDashboardsQAConfiguration(accountID, status string) (string, error)

	// Brands
	CreateBrand(accountID, brandID string, definition map[string]any) (*Brand, error)
	DescribeBrand(accountID, brandID, versionID string) (*Brand, error)
	UpdateBrand(accountID, brandID string, definition map[string]any) (*Brand, error)
	DeleteBrand(accountID, brandID string) error
	ListBrands(accountID string, maxResults int32, nextToken string) ([]*Brand, string, error)
	DescribeBrandPublishedVersion(accountID, brandID string) (*Brand, error)
	UpdateBrandPublishedVersion(accountID, brandID, versionID string) error
	DescribeBrandAssignment(accountID string) (string, error)
	UpdateBrandAssignment(accountID, brandArn string) (string, error)
	DeleteBrandAssignment(accountID string) error

	// Custom permissions
	CreateCustomPermissions(
		accountID, name string,
		capabilities map[string]any,
		tags map[string]string,
	) (*CustomPermissions, error)
	DescribeCustomPermissions(accountID, name string) (*CustomPermissions, error)
	UpdateCustomPermissions(accountID, name string, capabilities map[string]any) (*CustomPermissions, error)
	DeleteCustomPermissions(accountID, name string) error
	ListCustomPermissions(accountID string, maxResults int32, nextToken string) ([]*CustomPermissions, string, error)

	// Role custom permission
	UpdateRoleCustomPermission(accountID, namespace, role, customPermissionsName string) error
	DescribeRoleCustomPermission(accountID, namespace, role string) (string, error)
	DeleteRoleCustomPermission(accountID, namespace, role string) error

	// Role memberships
	CreateRoleMembership(accountID, namespace, role, memberName string) error
	DeleteRoleMembership(accountID, namespace, role, memberName string) error
	ListRoleMemberships(
		accountID, namespace, role string,
		maxResults int32,
		nextToken string,
	) ([]string, string, error)

	// User custom permission
	UpdateUserCustomPermission(accountID, namespace, userName, customPermissionsName string) error
	DeleteUserCustomPermission(accountID, namespace, userName string) error

	// OAuth client applications
	CreateOAuthClientApplication(
		accountID, clientID, name string,
		fields map[string]any,
	) (*OAuthClientApplication, error)
	DescribeOAuthClientApplication(accountID, clientID string) (*OAuthClientApplication, error)
	UpdateOAuthClientApplication(
		accountID, clientID, name string,
		fields map[string]any,
	) (*OAuthClientApplication, error)
	DeleteOAuthClientApplication(accountID, clientID string) (*OAuthClientApplication, error)
	ListOAuthClientApplications(
		accountID string,
		maxResults int32,
		nextToken string,
	) ([]*OAuthClientApplication, string, error)

	// Identity propagation configuration
	UpdateIdentityPropagationConfig(accountID, service string, authorizedTargets []string) error
	DeleteIdentityPropagationConfig(accountID, service string) error
	ListIdentityPropagationConfigs(accountID string) ([]*IdentityPropagationConfig, error)

	// Asset bundle export jobs
	StartAssetBundleExportJob(
		accountID, jobID, exportFormat string,
		resourceArns []string,
		includeAllDependencies bool,
	) (*AssetBundleExportJob, error)
	DescribeAssetBundleExportJob(accountID, jobID string) (*AssetBundleExportJob, error)
	ListAssetBundleExportJobs(
		accountID string,
		maxResults int32,
		nextToken string,
	) ([]*AssetBundleExportJob, string, error)

	// Asset bundle import jobs
	StartAssetBundleImportJob(accountID, jobID, failureAction string) (*AssetBundleImportJob, error)
	DescribeAssetBundleImportJob(accountID, jobID string) (*AssetBundleImportJob, error)
	ListAssetBundleImportJobs(
		accountID string,
		maxResults int32,
		nextToken string,
	) ([]*AssetBundleImportJob, string, error)

	// Dashboard snapshot jobs
	StartDashboardSnapshotJob(
		accountID, dashboardID, jobID string,
		snapshotConfiguration map[string]any,
	) (*DashboardSnapshotJob, error)
	DescribeDashboardSnapshotJob(accountID, dashboardID, jobID string) (*DashboardSnapshotJob, error)
	DescribeDashboardSnapshotJobResult(accountID, dashboardID, jobID string) (*DashboardSnapshotJob, error)

	// DataSet refresh schedules
	CreateRefreshSchedule(
		accountID, datasetID, scheduleID, refreshType, startAfterDateTime string,
		scheduleFrequency map[string]any,
	) (*RefreshSchedule, error)
	DescribeRefreshSchedule(accountID, datasetID, scheduleID string) (*RefreshSchedule, error)
	UpdateRefreshSchedule(
		accountID, datasetID, scheduleID, refreshType, startAfterDateTime string,
		scheduleFrequency map[string]any,
	) (*RefreshSchedule, error)
	DeleteRefreshSchedule(accountID, datasetID, scheduleID string) error
	ListRefreshSchedules(accountID, datasetID string) ([]*RefreshSchedule, error)

	// DataSet refresh properties
	PutDataSetRefreshProperties(
		accountID, datasetID string,
		refreshConfiguration, failureConfiguration map[string]any,
	) (*DataSetRefreshProperties, error)
	DescribeDataSetRefreshProperties(accountID, datasetID string) (*DataSetRefreshProperties, error)
	DeleteDataSetRefreshProperties(accountID, datasetID string) error

	// Embed URLs
	GenerateEmbedURLForAnonymousUser(
		accountID, namespace string,
		authorizedResourceArns []string,
		experienceConfiguration map[string]any,
	) (embedURL, anonymousUserArn string, err error)
	GenerateEmbedURLForRegisteredUser(
		accountID, userArn string,
		experienceConfiguration map[string]any,
	) (string, error)
	GenerateEmbedURLForRegisteredUserWithIdentity(
		accountID string,
		experienceConfiguration map[string]any,
	) (string, error)
	GetDashboardEmbedURL(accountID, dashboardID, identityType string) (string, error)
	GetSessionEmbedURL(accountID, entryPoint string) (string, error)

	// Action connectors
	CreateActionConnector(
		accountID, actionConnectorID, name, connectorType, description, vpcConnectionArn string,
		authenticationConfig map[string]any,
		permissions []ResourcePermission,
		tags map[string]string,
	) (*ActionConnector, error)
	DescribeActionConnector(accountID, actionConnectorID string) (*ActionConnector, error)
	UpdateActionConnector(
		accountID, actionConnectorID, name, description, vpcConnectionArn string,
		authenticationConfig map[string]any,
	) (*ActionConnector, error)
	DeleteActionConnector(accountID, actionConnectorID string) (*ActionConnector, error)
	ListActionConnectors(accountID string, maxResults int32, nextToken string) ([]*ActionConnector, string, error)
	SearchActionConnectors(
		accountID string,
		filters []SearchFilter,
		maxResults int32,
		nextToken string,
	) ([]*ActionConnector, string, error)
	DescribeActionConnectorPermissions(
		accountID, actionConnectorID string,
	) (*ActionConnector, []ResourcePermission, error)
	UpdateActionConnectorPermissions(
		accountID, actionConnectorID string,
		grant, revoke []ResourcePermission,
	) (*ActionConnector, []ResourcePermission, error)

	// Automation jobs
	StartAutomationJob(accountID, automationGroupID, automationID, inputPayload string) (*AutomationJob, error)
	DescribeAutomationJob(accountID, automationGroupID, automationID, jobID string) (*AutomationJob, error)

	// SPICE capacity configuration
	UpdateSPICECapacityConfiguration(accountID, purchaseMode string) error

	// Flows (QuickSight exposes no CreateFlow API; flows are authored via the
	// console/Quick Suite and only read/searched/permissioned through the API).
	ListFlows(accountID string, maxResults int32, nextToken string) ([]*Flow, string, error)
	SearchFlows(
		accountID string,
		filters []SearchFilter,
		maxResults int32,
		nextToken string,
	) ([]*Flow, string, error)
	GetFlowMetadata(accountID, flowID string) (*Flow, error)
	GetFlowPermissions(accountID, flowID string) (*Flow, []ResourcePermission, error)
	UpdateFlowPermissions(
		accountID, flowID string,
		grant, revoke []ResourcePermission,
	) (*Flow, []ResourcePermission, error)

	AccountID() string
	Region() string
	Reset()
}

// Namespace represents a QuickSight namespace.
type Namespace struct {
	CreationStatus string
	Name           string
	Arn            string
	CapacityRegion string
	IdentityStore  string
}

// Group represents a QuickSight group.
type Group struct {
	GroupName   string
	Arn         string
	Description string
	Namespace   string
	PrincipalID string
}

// GroupMember represents a QuickSight group member.
type GroupMember struct {
	MemberName string
	Arn        string
}

// User represents a QuickSight user.
type User struct {
	UserName     string
	Arn          string
	Email        string
	Role         string
	IdentityType string
	Namespace    string
	PrincipalID  string
	SessionName  string
	Active       bool
}

// DataSource represents a QuickSight data source.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type DataSource struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	DataSourceID    string
	Arn             string
	Name            string
	Type            string
	Status          string
	Permissions     []ResourcePermission
}

// DataSet represents a QuickSight dataset.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type DataSet struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	DataSetID       string
	Arn             string
	Name            string
	ImportMode      string
	Permissions     []ResourcePermission
}

// Ingestion represents a QuickSight ingestion.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type Ingestion struct {
	CreatedTime     time.Time
	IngestionID     string
	Arn             string
	DataSetID       string
	IngestionStatus string
}

// Dashboard represents a QuickSight dashboard.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type Dashboard struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	Definition      map[string]any
	DashboardID     string
	Arn             string
	Name            string
	Status          string
	Permissions     []ResourcePermission
	VersionNumber   int64
}

// DashboardVersion represents a version of a QuickSight dashboard.
type DashboardVersion struct {
	CreatedTime   time.Time
	Arn           string
	Status        string
	VersionNumber int64
}

// Analysis represents a QuickSight analysis.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type Analysis struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	AnalysisID      string
	Arn             string
	Name            string
	Status          string
	Definition      map[string]any
	Permissions     []ResourcePermission
}

// ResourcePermission represents a QuickSight principal + allowed actions grant.
type ResourcePermission struct {
	Principal string   `json:"principal"`
	Actions   []string `json:"actions"`
}

// FolderMember represents a QuickSight folder membership (a dashboard, analysis,
// dataset, or other asset that belongs to a folder).
type FolderMember struct {
	MemberID   string
	MemberType string
}

// FolderSearchFilter represents a single SearchFolders filter criterion.
type FolderSearchFilter struct {
	Operator string
	Name     string
	Value    string
}

// SearchFilter is a generic Name/Operator/Value search filter, shared by the
// Search{Analyses,Dashboards,DataSets,DataSources} operations. It has the same
// shape as [FolderSearchFilter] (SearchFolders' filter type).
type SearchFilter = FolderSearchFilter

// Folder represents a QuickSight folder.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type Folder struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	FolderID        string
	Arn             string
	Name            string
	FolderType      string
	ParentFolderArn string
	Permissions     []ResourcePermission
}

// TemplateVersion represents one version of a QuickSight template.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type TemplateVersion struct {
	CreatedTime     time.Time
	Definition      map[string]any
	Arn             string
	Status          string
	SourceEntityArn string
	Description     string
	VersionNumber   int64
}

// Template represents a QuickSight template (a versioned, reusable dashboard/analysis
// layout definition).
type Template struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	TemplateID      string
	Arn             string
	Name            string
	Version         *TemplateVersion
	Permissions     []ResourcePermission
}

// TemplateAlias represents a named pointer to a specific template version.
type TemplateAlias struct {
	AliasName             string
	Arn                   string
	TemplateVersionNumber int64
}

// ThemeVersion represents one version of a QuickSight theme.
// CreatedTime first: non-pointer prefix reduces GC pointer bytes.
type ThemeVersion struct {
	CreatedTime   time.Time
	Configuration map[string]any
	Arn           string
	Status        string
	BaseThemeID   string
	Description   string
	VersionNumber int64
}

// Theme represents a QuickSight theme (a versioned set of visual style settings).
type Theme struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	ThemeID         string
	Arn             string
	Name            string
	Type            string
	Version         *ThemeVersion
	Permissions     []ResourcePermission
}

// ThemeAlias represents a named pointer to a specific theme version.
type ThemeAlias struct {
	AliasName          string
	Arn                string
	ThemeVersionNumber int64
}

// Topic represents a QuickSight topic (a natural-language Q&A data source).
type Topic struct {
	CreatedTime           time.Time
	LastUpdatedTime       time.Time
	TopicID               string
	Arn                   string
	Name                  string
	Description           string
	UserExperienceVersion string
	DataSets              []map[string]any
	Permissions           []ResourcePermission
}

// TopicRefreshSchedule represents one refresh schedule for a topic's dataset,
// keyed by DatasetId.
type TopicRefreshSchedule struct {
	ScheduleConfig map[string]any
	DatasetID      string
	DatasetArn     string
	RefreshType    string
	IsEnabled      bool
}

// TopicRefreshDetails represents the status of a single topic refresh execution.
type TopicRefreshDetails struct {
	RefreshID     string
	RefreshStatus string
}

// TopicReviewedAnswer represents one human-reviewed answer attached to a topic.
type TopicReviewedAnswer struct {
	PrimaryVisual map[string]any
	Template      map[string]any
	AnswerID      string
	DatasetArn    string
	Question      string
	Mode          string
}

// TopicAnswerError represents a single failed entry in a batch reviewed-answer
// create/delete operation.
type TopicAnswerError struct {
	AnswerID string
	Message  string
}

// VPCConnection represents a QuickSight VPC connection.
type VPCConnection struct {
	CreatedTime        time.Time
	LastUpdatedTime    time.Time
	VPCConnectionID    string
	Arn                string
	Name               string
	VPCID              string
	RoleArn            string
	Status             string
	AvailabilityStatus string
	SubnetIDs          []string
	SecurityGroupIDs   []string
	DNSResolvers       []string
}

// IAMPolicyAssignment represents a QuickSight IAM policy assignment, scoped by namespace.
type IAMPolicyAssignment struct {
	Identities       map[string][]string
	AssignmentID     string
	AssignmentName   string
	AssignmentStatus string
	PolicyArn        string
	Namespace        string
}

// AccountSettings represents a QuickSight account's account-wide settings.
type AccountSettings struct {
	AccountName                  string
	Edition                      string
	DefaultNamespace             string
	NotificationEmail            string
	PublicSharingEnabled         bool
	TerminationProtectionEnabled bool
}

// AccountSubscription represents a QuickSight account subscription.
type AccountSubscription struct {
	AccountName               string
	Edition                   string
	NotificationEmail         string
	AuthenticationType        string
	AccountSubscriptionStatus string
}

// AccountCustomization represents a QuickSight account's (or namespace's) branding customization.
type AccountCustomization struct {
	Namespace                         string
	DefaultTheme                      string
	DefaultEmailCustomizationTemplate string
}

// IPRestriction represents a QuickSight account's IP/VPC access restriction rules.
type IPRestriction struct {
	RuleMap              map[string]string
	VPCIDRuleMap         map[string]string
	VPCEndpointIDRuleMap map[string]string
	Enabled              bool
}

// RegisteredCustomerManagedKey represents a customer-managed KMS key registered with QuickSight.
type RegisteredCustomerManagedKey struct {
	KeyArn     string
	DefaultKey bool
}

// DefaultQBusinessApplication represents the default Amazon Q Business application
// linked to a QuickSight account.
type DefaultQBusinessApplication struct {
	ApplicationID string
	Namespace     string
}

// Brand represents a QuickSight brand, a versioned set of visual identity
// customizations (logo, theme, name) applied to the console/embedded experiences.
type Brand struct {
	CreatedTime        time.Time
	LastUpdatedTime    time.Time
	Definition         map[string]any
	BrandID            string
	Arn                string
	Status             string
	CurrentVersionID   string
	CurrentVersionStat string
	PublishedVersionID string
}

// CustomPermissions represents a named, reusable set of QuickSight capability
// overrides that can be attached to roles or users.
type CustomPermissions struct {
	Capabilities map[string]any
	Name         string
	Arn          string
}

// OAuthClientApplication represents a QuickSight OAuth 2.0 client application used
// to connect to a data source's identity provider.
type OAuthClientApplication struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	Extra           map[string]any
	ClientID        string
	Arn             string
	Name            string
	Status          string
}

// IdentityPropagationConfig represents the authorized targets for one downstream
// Amazon Web Services service that QuickSight can propagate an end user's identity to.
type IdentityPropagationConfig struct {
	Service           string
	AuthorizedTargets []string
}

// AssetBundleExportJob represents an asynchronous asset-bundle export job.
type AssetBundleExportJob struct {
	CreatedTime            time.Time
	JobID                  string
	Arn                    string
	Status                 string
	ExportFormat           string
	DownloadURL            string
	ResourceArns           []string
	IncludeAllDependencies bool
}

// AssetBundleImportJob represents an asynchronous asset-bundle import job.
type AssetBundleImportJob struct {
	CreatedTime   time.Time
	JobID         string
	Arn           string
	Status        string
	FailureAction string
}

// DashboardSnapshotJob represents an asynchronous dashboard snapshot ("export to
// PDF/CSV/Excel") job.
type DashboardSnapshotJob struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	SnapshotConfig  map[string]any
	JobID           string
	Arn             string
	DashboardID     string
	Status          string
	S3URI           string
}

// RefreshSchedule represents a QuickSight dataset SPICE refresh schedule.
type RefreshSchedule struct {
	ScheduleFrequency  map[string]any
	ScheduleID         string
	Arn                string
	RefreshType        string
	StartAfterDateTime string
}

// DataSetRefreshProperties represents a QuickSight dataset's SPICE refresh
// configuration (incremental refresh window, failure notifications, etc.).
type DataSetRefreshProperties struct {
	RefreshConfiguration map[string]any
	FailureConfiguration map[string]any
}

// ActionConnector represents a QuickSight action connector: a configured
// integration (Salesforce, Jira, generic HTTP, etc.) that QuickSight agents
// and automations can invoke to perform actions against an external service.
type ActionConnector struct {
	CreatedTime          time.Time
	LastUpdatedTime      time.Time
	AuthenticationConfig map[string]any
	ActionConnectorID    string
	Arn                  string
	Name                 string
	Type                 string
	Description          string
	VPCConnectionArn     string
	Status               string
	Permissions          []ResourcePermission
}

// AutomationJob represents one run of a QuickSight automation (a
// console-authored workflow scoped to an automation group/automation).
type AutomationJob struct {
	CreatedAt         time.Time
	StartedAt         time.Time
	EndedAt           time.Time
	AutomationGroupID string
	AutomationID      string
	JobID             string
	Arn               string
	Status            string
	InputPayload      string
	OutputPayload     string
}

// Flow represents a QuickSight flow. QuickSight's API exposes no CreateFlow
// operation (flows are authored via the console/Quick Suite); only
// list/search/describe/permission operations are available.
type Flow struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	LastPublishedAt time.Time
	Description     string
	Arn             string
	Name            string
	FlowID          string
	CreatedBy       string
	LastPublishedBy string
	LastUpdatedBy   string
	PublishState    string
	Permissions     []ResourcePermission
	RunCount        int32
	UserCount       int32
}

var _ StorageBackend = (*InMemoryBackend)(nil)
