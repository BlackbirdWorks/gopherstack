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
	CreateDataSource(accountID, dataSourceID, name, dsType string, tags map[string]string) (*DataSource, error)
	DescribeDataSource(accountID, dataSourceID string) (*DataSource, error)
	UpdateDataSource(accountID, dataSourceID, name string) (*DataSource, error)
	DeleteDataSource(accountID, dataSourceID string) error
	ListDataSources(accountID string, maxResults int32, nextToken string) ([]*DataSource, string, error)

	// DataSets
	CreateDataSet(accountID, dataSetID, name, importMode string, tags map[string]string) (*DataSet, error)
	DescribeDataSet(accountID, dataSetID string) (*DataSet, error)
	UpdateDataSet(accountID, dataSetID, name, importMode string) (*DataSet, error)
	DeleteDataSet(accountID, dataSetID string) error
	ListDataSets(accountID string, maxResults int32, nextToken string) ([]*DataSet, string, error)

	// Ingestions
	CreateIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error)
	DescribeIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error)
	CancelIngestion(accountID, dataSetID, ingestionID string) error
	ListIngestions(accountID, dataSetID string, maxResults int32, nextToken string) ([]*Ingestion, string, error)

	// Dashboards
	CreateDashboard(accountID, dashboardID, name string, tags map[string]string) (*Dashboard, error)
	DescribeDashboard(accountID, dashboardID string) (*Dashboard, error)
	UpdateDashboard(accountID, dashboardID, name string) (*Dashboard, error)
	DeleteDashboard(accountID, dashboardID string) error
	ListDashboards(accountID string, maxResults int32, nextToken string) ([]*Dashboard, string, error)
	ListDashboardVersions(
		accountID, dashboardID string,
		maxResults int32,
		nextToken string,
	) ([]*DashboardVersion, string, error)

	// Analyses
	CreateAnalysis(accountID, analysisID, name string, tags map[string]string) (*Analysis, error)
	DescribeAnalysis(accountID, analysisID string) (*Analysis, error)
	UpdateAnalysis(accountID, analysisID, name string) (*Analysis, error)
	DeleteAnalysis(accountID, analysisID string, forceDeleteWithoutRecovery bool) error
	ListAnalyses(accountID string, maxResults int32, nextToken string) ([]*Analysis, string, error)
	RestoreAnalysis(accountID, analysisID string) (*Analysis, error)

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
	DashboardID     string
	Arn             string
	Name            string
	Status          string
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

var _ StorageBackend = (*InMemoryBackend)(nil)
