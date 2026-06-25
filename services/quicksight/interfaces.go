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

	// Folders
	CreateFolder(accountID, folderID, name, folderType string) (*Folder, error)
	DescribeFolder(accountID, folderID string) (*Folder, error)
	UpdateFolder(accountID, folderID, name string) (*Folder, error)
	DeleteFolder(accountID, folderID string) error
	ListFolders(accountID string, maxResults int32, nextToken string) ([]*Folder, string, error)

	// Templates
	CreateTemplate(accountID, templateID, name string) (*Template, error)
	DescribeTemplate(accountID, templateID string) (*Template, error)
	UpdateTemplate(accountID, templateID, name string) (*Template, error)
	DeleteTemplate(accountID, templateID string) error
	ListTemplates(accountID string, maxResults int32, nextToken string) ([]*Template, string, error)

	// Themes
	CreateTheme(accountID, themeID, name string) (*Theme, error)
	DescribeTheme(accountID, themeID string) (*Theme, error)
	UpdateTheme(accountID, themeID, name string) (*Theme, error)
	DeleteTheme(accountID, themeID string) error
	ListThemes(accountID string, maxResults int32, nextToken string) ([]*Theme, string, error)

	// VPC Connections
	CreateVPCConnection(accountID, vpcConnectionID, name string) (*VPCConnection, error)
	DescribeVPCConnection(accountID, vpcConnectionID string) (*VPCConnection, error)
	UpdateVPCConnection(accountID, vpcConnectionID, name string) (*VPCConnection, error)
	DeleteVPCConnection(accountID, vpcConnectionID string) (*VPCConnection, error)
	ListVPCConnections(accountID string, maxResults int32, nextToken string) ([]*VPCConnection, string, error)

	// Brands
	CreateBrand(accountID, brandID, name string) (*Brand, error)
	DescribeBrand(accountID, brandID string) (*Brand, error)
	UpdateBrand(accountID, brandID, name string) (*Brand, error)
	DeleteBrand(accountID, brandID string) error
	ListBrands(accountID string, maxResults int32, nextToken string) ([]*Brand, string, error)

	// Tags
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
}

// Folder represents a QuickSight folder.
type Folder struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	FolderID        string
	Arn             string
	Name            string
	FolderType      string
}

// Template represents a QuickSight template.
type Template struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	TemplateID      string
	Arn             string
	Name            string
	Status          string
	VersionNumber   int64
}

// Theme represents a QuickSight theme.
type Theme struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	ThemeID         string
	Arn             string
	Name            string
	Status          string
	VersionNumber   int64
}

// VPCConnection represents a QuickSight VPC connection.
type VPCConnection struct {
	CreatedTime       time.Time
	LastUpdatedTime   time.Time
	VPCConnectionID   string
	Arn               string
	Name              string
	Status            string
	AvailabilityState string
}

// Brand represents a QuickSight brand.
type Brand struct {
	CreatedTime     time.Time
	LastUpdatedTime time.Time
	BrandID         string
	Arn             string
	Name            string
	Status          string
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

var _ StorageBackend = (*InMemoryBackend)(nil)
