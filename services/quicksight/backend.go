package quicksight

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
	errResourceExists    = "ResourceExistsException"
	errValidation        = "InvalidParameterValueException"

	defaultNamespace         = "default"
	identityStoreQuickSight  = "QUICKSIGHT"
	statusCreationSuccessful = "CREATION_SUCCESSFUL"
	statusCreationInProgress = "CREATION_IN_PROGRESS"
	statusUpdateSuccessful   = "UPDATE_SUCCESSFUL"
	statusCreated            = "CREATED"
	statusDeleted            = "DELETED"
	statusRunning            = "RUNNING"
	statusCompleted          = "COMPLETED"
	statusCancelled          = "CANCELLED"

	defaultMaxResults = 100
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
)

type storedNamespace struct {
	Name           string `json:"name"`
	Arn            string `json:"arn"`
	CapacityRegion string `json:"capacityRegion"`
	Status         string `json:"status"`
	IdentityStore  string `json:"identityStore"`
}

func (n *storedNamespace) toNamespace() *Namespace {
	return &Namespace{
		Name:           n.Name,
		Arn:            n.Arn,
		CapacityRegion: n.CapacityRegion,
		CreationStatus: n.Status,
		IdentityStore:  n.IdentityStore,
	}
}

type storedGroup struct {
	GroupName   string `json:"groupName"`
	Arn         string `json:"arn"`
	Description string `json:"description"`
	Namespace   string `json:"namespace"`
	PrincipalID string `json:"principalId"`
}

func (g *storedGroup) toGroup() *Group {
	return &Group{
		GroupName:   g.GroupName,
		Arn:         g.Arn,
		Description: g.Description,
		Namespace:   g.Namespace,
		PrincipalID: g.PrincipalID,
	}
}

type storedUser struct {
	UserName     string `json:"userName"`
	Arn          string `json:"arn"`
	Email        string `json:"email"`
	Role         string `json:"role"`
	IdentityType string `json:"identityType"`
	Namespace    string `json:"namespace"`
	PrincipalID  string `json:"principalId"`
	SessionName  string `json:"sessionName"`
	Active       bool   `json:"active"`
}

func (u *storedUser) toUser() *User {
	return &User{
		UserName:     u.UserName,
		Arn:          u.Arn,
		Email:        u.Email,
		Role:         u.Role,
		IdentityType: u.IdentityType,
		Namespace:    u.Namespace,
		PrincipalID:  u.PrincipalID,
		SessionName:  u.SessionName,
		Active:       u.Active,
	}
}

type storedDataSource struct {
	CreatedTime     time.Time            `json:"createdTime"`
	LastUpdatedTime time.Time            `json:"lastUpdatedTime"`
	DataSourceID    string               `json:"dataSourceId"`
	Arn             string               `json:"arn"`
	Name            string               `json:"name"`
	Type            string               `json:"type"`
	Status          string               `json:"status"`
	Permissions     []ResourcePermission `json:"permissions,omitempty"`
}

func (d *storedDataSource) toDataSource() *DataSource {
	return &DataSource{
		CreatedTime:     d.CreatedTime,
		LastUpdatedTime: d.LastUpdatedTime,
		DataSourceID:    d.DataSourceID,
		Arn:             d.Arn,
		Name:            d.Name,
		Type:            d.Type,
		Status:          d.Status,
		Permissions:     clonePermissions(d.Permissions),
	}
}

type storedDataSet struct {
	CreatedTime       time.Time                         `json:"createdTime"`
	LastUpdatedTime   time.Time                         `json:"lastUpdatedTime"`
	RefreshSchedules  map[string]*storedRefreshSchedule `json:"refreshSchedules,omitempty"`
	RefreshProperties *storedDataSetRefreshProperties   `json:"refreshProperties,omitempty"`
	DataSetID         string                            `json:"dataSetId"`
	Arn               string                            `json:"arn"`
	Name              string                            `json:"name"`
	ImportMode        string                            `json:"importMode"`
	Permissions       []ResourcePermission              `json:"permissions,omitempty"`
}

func (d *storedDataSet) toDataSet() *DataSet {
	return &DataSet{
		CreatedTime:     d.CreatedTime,
		LastUpdatedTime: d.LastUpdatedTime,
		DataSetID:       d.DataSetID,
		Arn:             d.Arn,
		Name:            d.Name,
		ImportMode:      d.ImportMode,
		Permissions:     clonePermissions(d.Permissions),
	}
}

type storedIngestion struct {
	CreatedTime     time.Time `json:"createdTime"`
	IngestionID     string    `json:"ingestionId"`
	Arn             string    `json:"arn"`
	DataSetID       string    `json:"dataSetId"`
	IngestionStatus string    `json:"ingestionStatus"`
}

func (i *storedIngestion) toIngestion() *Ingestion {
	return &Ingestion{
		CreatedTime:     i.CreatedTime,
		IngestionID:     i.IngestionID,
		Arn:             i.Arn,
		DataSetID:       i.DataSetID,
		IngestionStatus: i.IngestionStatus,
	}
}

type storedDashboard struct {
	CreatedTime            time.Time            `json:"createdTime"`
	LastUpdatedTime        time.Time            `json:"lastUpdatedTime"`
	Definition             map[string]any       `json:"definition,omitempty"`
	DashboardID            string               `json:"dashboardId"`
	Arn                    string               `json:"arn"`
	Name                   string               `json:"name"`
	Status                 string               `json:"status"`
	Permissions            []ResourcePermission `json:"permissions,omitempty"`
	LinkEntities           []string             `json:"linkEntities,omitempty"`
	VersionNumber          int64                `json:"versionNumber"`
	PublishedVersionNumber int64                `json:"publishedVersionNumber"`
}

func (d *storedDashboard) toDashboard() *Dashboard {
	return &Dashboard{
		CreatedTime:            d.CreatedTime,
		LastUpdatedTime:        d.LastUpdatedTime,
		DashboardID:            d.DashboardID,
		Arn:                    d.Arn,
		Name:                   d.Name,
		Status:                 d.Status,
		VersionNumber:          d.VersionNumber,
		PublishedVersionNumber: d.PublishedVersionNumber,
		Definition:             d.Definition,
		Permissions:            clonePermissions(d.Permissions),
		LinkEntities:           append([]string(nil), d.LinkEntities...),
	}
}

// storedRefreshSchedule is the persisted representation of one dataset SPICE
// refresh schedule, keyed by ScheduleId.
type storedRefreshSchedule struct {
	ScheduleFrequency  map[string]any `json:"scheduleFrequency,omitempty"`
	ScheduleID         string         `json:"scheduleId"`
	Arn                string         `json:"arn"`
	RefreshType        string         `json:"refreshType"`
	StartAfterDateTime string         `json:"startAfterDateTime,omitempty"`
}

func (s *storedRefreshSchedule) toRefreshSchedule() *RefreshSchedule {
	return &RefreshSchedule{
		ScheduleID:         s.ScheduleID,
		Arn:                s.Arn,
		RefreshType:        s.RefreshType,
		StartAfterDateTime: s.StartAfterDateTime,
		ScheduleFrequency:  s.ScheduleFrequency,
	}
}

// storedDataSetRefreshProperties is the persisted representation of a dataset's
// SPICE refresh configuration.
type storedDataSetRefreshProperties struct {
	RefreshConfiguration map[string]any `json:"refreshConfiguration,omitempty"`
	FailureConfiguration map[string]any `json:"failureConfiguration,omitempty"`
}

func (p *storedDataSetRefreshProperties) toDataSetRefreshProperties() *DataSetRefreshProperties {
	return &DataSetRefreshProperties{
		RefreshConfiguration: p.RefreshConfiguration,
		FailureConfiguration: p.FailureConfiguration,
	}
}

type storedAnalysis struct {
	CreatedTime     time.Time            `json:"createdTime"`
	LastUpdatedTime time.Time            `json:"lastUpdatedTime"`
	Definition      map[string]any       `json:"definition,omitempty"`
	AnalysisID      string               `json:"analysisId"`
	Arn             string               `json:"arn"`
	Name            string               `json:"name"`
	Status          string               `json:"status"`
	Permissions     []ResourcePermission `json:"permissions,omitempty"`
}

func (a *storedAnalysis) toAnalysis() *Analysis {
	return &Analysis{
		CreatedTime:     a.CreatedTime,
		LastUpdatedTime: a.LastUpdatedTime,
		AnalysisID:      a.AnalysisID,
		Arn:             a.Arn,
		Name:            a.Name,
		Status:          a.Status,
		Definition:      a.Definition,
		Permissions:     clonePermissions(a.Permissions),
	}
}

// state is the serializable snapshot of the backend.
type state struct {
	Namespaces           map[string]*storedNamespace           `json:"namespaces"`
	Groups               map[string]*storedGroup               `json:"groups"`
	GroupMembers         map[string]bool                       `json:"groupMembers"`
	Users                map[string]*storedUser                `json:"users"`
	DataSources          map[string]*storedDataSource          `json:"dataSources"`
	DataSets             map[string]*storedDataSet             `json:"dataSets"`
	Ingestions           map[string]*storedIngestion           `json:"ingestions"`
	Dashboards           map[string]*storedDashboard           `json:"dashboards"`
	Analyses             map[string]*storedAnalysis            `json:"analyses"`
	Tags                 map[string]map[string]string          `json:"tags"`
	Folders              map[string]*storedFolder              `json:"folders"`
	FolderMembers        map[string]*storedFolderMember        `json:"folderMembers"`
	Templates            map[string]*storedTemplate            `json:"templates"`
	Themes               map[string]*storedTheme               `json:"themes"`
	Topics               map[string]*storedTopic               `json:"topics"`
	VPCConnections       map[string]*storedVPCConnection       `json:"vpcConnections"`
	IAMPolicyAssignments map[string]*storedIAMPolicyAssignment `json:"iamPolicyAssignments"`

	AccountSettings          map[string]*storedAccountSettings             `json:"accountSettings"`
	AccountSubscriptions     map[string]*storedAccountSubscription         `json:"accountSubscriptions"`
	AccountCustomizations    map[string]*storedAccountCustomization        `json:"accountCustomizations"`
	AccountCustomPermissions map[string]string                             `json:"accountCustomPermissions"`
	IPRestrictions           map[string]*storedIPRestriction               `json:"ipRestrictions"`
	PublicSharing            map[string]bool                               `json:"publicSharing"`
	KeyRegistrations         map[string][]storedRegisteredKey              `json:"keyRegistrations"`
	DefaultQBusinessApps     map[string]*storedDefaultQBusinessApplication `json:"defaultQBusinessApps"`
	QPersonalization         map[string]string                             `json:"qPersonalization"`
	QSearchConfig            map[string]string                             `json:"qSearchConfig"`
	DashboardsQAConfig       map[string]string                             `json:"dashboardsQAConfig"`

	Brands                     map[string]*storedBrand                     `json:"brands"`
	BrandAssignments           map[string]string                           `json:"brandAssignments"`
	CustomPermissions          map[string]*storedCustomPermissions         `json:"customPermissions"`
	RoleCustomPermissions      map[string]string                           `json:"roleCustomPermissions"`
	RoleMemberships            map[string]bool                             `json:"roleMemberships"`
	UserCustomPermissions      map[string]string                           `json:"userCustomPermissions"`
	OAuthClientApps            map[string]*storedOAuthApp                  `json:"oauthClientApps"`
	IdentityPropagationConfigs map[string]*storedIdentityPropagationConfig `json:"identityPropagationConfigs"`
	AssetBundleExportJobs      map[string]*storedAssetBundleExportJob      `json:"assetBundleExportJobs"`
	AssetBundleImportJobs      map[string]*storedAssetBundleImportJob      `json:"assetBundleImportJobs"`
	DashboardSnapshotJobs      map[string]*storedDashboardSnapshotJob      `json:"dashboardSnapshotJobs"`

	ActionConnectors map[string]*storedActionConnector `json:"actionConnectors"`
	AutomationJobs   map[string]*storedAutomationJob   `json:"automationJobs"`
	Flows            map[string]*storedFlow            `json:"flows"`
	SPICECapacity    map[string]string                 `json:"spiceCapacity"`

	SelfUpgradeConfig   map[string]string                    `json:"selfUpgradeConfig"`
	SelfUpgradeRequests map[string]*storedSelfUpgradeRequest `json:"selfUpgradeRequests"`
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu                   *lockmetrics.RWMutex
	namespaces           map[string]*storedNamespace
	groups               map[string]*storedGroup
	groupMembers         map[string]bool
	users                map[string]*storedUser
	dataSources          map[string]*storedDataSource
	dataSets             map[string]*storedDataSet
	ingestions           map[string]*storedIngestion
	dashboards           map[string]*storedDashboard
	analyses             map[string]*storedAnalysis
	tags                 map[string]map[string]string
	folders              map[string]*storedFolder
	folderMembers        map[string]*storedFolderMember
	templates            map[string]*storedTemplate
	themes               map[string]*storedTheme
	topics               map[string]*storedTopic
	vpcConnections       map[string]*storedVPCConnection
	iamPolicyAssignments map[string]*storedIAMPolicyAssignment

	accountSettings          map[string]*storedAccountSettings
	accountSubscriptions     map[string]*storedAccountSubscription
	accountCustomizations    map[string]*storedAccountCustomization
	accountCustomPermissions map[string]string
	ipRestrictions           map[string]*storedIPRestriction
	publicSharing            map[string]bool
	keyRegistrations         map[string][]storedRegisteredKey
	defaultQBusinessApps     map[string]*storedDefaultQBusinessApplication
	qPersonalization         map[string]string
	qSearchConfig            map[string]string
	dashboardsQAConfig       map[string]string

	brands                     map[string]*storedBrand
	brandAssignments           map[string]string
	customPermissions          map[string]*storedCustomPermissions
	roleCustomPermissions      map[string]string
	roleMemberships            map[string]bool
	userCustomPermissions      map[string]string
	oauthClientApps            map[string]*storedOAuthApp
	identityPropagationConfigs map[string]*storedIdentityPropagationConfig
	assetBundleExportJobs      map[string]*storedAssetBundleExportJob
	assetBundleImportJobs      map[string]*storedAssetBundleImportJob
	dashboardSnapshotJobs      map[string]*storedDashboardSnapshotJob

	actionConnectors map[string]*storedActionConnector
	automationJobs   map[string]*storedAutomationJob
	flows            map[string]*storedFlow
	spiceCapacity    map[string]string

	selfUpgradeConfig   map[string]string
	selfUpgradeRequests map[string]*storedSelfUpgradeRequest

	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:            accountID,
		region:               region,
		namespaces:           make(map[string]*storedNamespace),
		groups:               make(map[string]*storedGroup),
		groupMembers:         make(map[string]bool),
		users:                make(map[string]*storedUser),
		dataSources:          make(map[string]*storedDataSource),
		dataSets:             make(map[string]*storedDataSet),
		ingestions:           make(map[string]*storedIngestion),
		dashboards:           make(map[string]*storedDashboard),
		analyses:             make(map[string]*storedAnalysis),
		tags:                 make(map[string]map[string]string),
		folders:              make(map[string]*storedFolder),
		folderMembers:        make(map[string]*storedFolderMember),
		templates:            make(map[string]*storedTemplate),
		themes:               make(map[string]*storedTheme),
		topics:               make(map[string]*storedTopic),
		vpcConnections:       make(map[string]*storedVPCConnection),
		iamPolicyAssignments: make(map[string]*storedIAMPolicyAssignment),

		accountSettings:       make(map[string]*storedAccountSettings),
		accountSubscriptions:  make(map[string]*storedAccountSubscription),
		accountCustomizations: make(map[string]*storedAccountCustomization),
		ipRestrictions:        make(map[string]*storedIPRestriction),
		publicSharing:         make(map[string]bool),
		keyRegistrations:      make(map[string][]storedRegisteredKey),
		defaultQBusinessApps:  make(map[string]*storedDefaultQBusinessApplication),
		qPersonalization:      make(map[string]string),
		qSearchConfig:         make(map[string]string),
		dashboardsQAConfig:    make(map[string]string),

		brands:                     make(map[string]*storedBrand),
		brandAssignments:           make(map[string]string),
		customPermissions:          make(map[string]*storedCustomPermissions),
		roleCustomPermissions:      make(map[string]string),
		roleMemberships:            make(map[string]bool),
		userCustomPermissions:      make(map[string]string),
		oauthClientApps:            make(map[string]*storedOAuthApp),
		identityPropagationConfigs: make(map[string]*storedIdentityPropagationConfig),
		assetBundleExportJobs:      make(map[string]*storedAssetBundleExportJob),
		assetBundleImportJobs:      make(map[string]*storedAssetBundleImportJob),
		dashboardSnapshotJobs:      make(map[string]*storedDashboardSnapshotJob),

		actionConnectors: make(map[string]*storedActionConnector),
		automationJobs:   make(map[string]*storedAutomationJob),
		flows:            make(map[string]*storedFlow),
		spiceCapacity:    make(map[string]string),

		selfUpgradeConfig:   make(map[string]string),
		selfUpgradeRequests: make(map[string]*storedSelfUpgradeRequest),
	}
	b.mu = lockmetrics.New("quicksight")

	// Pre-create the default namespace so basic operations work without explicit setup.
	b.namespaces[nsKey(accountID, defaultNamespace)] = &storedNamespace{
		Name:           defaultNamespace,
		Arn:            b.buildARN("namespace", defaultNamespace),
		CapacityRegion: region,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	}

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.namespaces = make(map[string]*storedNamespace)
	b.groups = make(map[string]*storedGroup)
	b.groupMembers = make(map[string]bool)
	b.users = make(map[string]*storedUser)
	b.dataSources = make(map[string]*storedDataSource)
	b.dataSets = make(map[string]*storedDataSet)
	b.ingestions = make(map[string]*storedIngestion)
	b.dashboards = make(map[string]*storedDashboard)
	b.analyses = make(map[string]*storedAnalysis)
	b.tags = make(map[string]map[string]string)
	b.folders = make(map[string]*storedFolder)
	b.folderMembers = make(map[string]*storedFolderMember)
	b.templates = make(map[string]*storedTemplate)
	b.themes = make(map[string]*storedTheme)
	b.topics = make(map[string]*storedTopic)
	b.vpcConnections = make(map[string]*storedVPCConnection)
	b.iamPolicyAssignments = make(map[string]*storedIAMPolicyAssignment)

	b.accountSettings = make(map[string]*storedAccountSettings)
	b.accountSubscriptions = make(map[string]*storedAccountSubscription)
	b.accountCustomizations = make(map[string]*storedAccountCustomization)
	b.accountCustomPermissions = make(map[string]string)
	b.ipRestrictions = make(map[string]*storedIPRestriction)
	b.publicSharing = make(map[string]bool)
	b.keyRegistrations = make(map[string][]storedRegisteredKey)
	b.defaultQBusinessApps = make(map[string]*storedDefaultQBusinessApplication)
	b.qPersonalization = make(map[string]string)
	b.qSearchConfig = make(map[string]string)
	b.dashboardsQAConfig = make(map[string]string)

	b.brands = make(map[string]*storedBrand)
	b.brandAssignments = make(map[string]string)
	b.customPermissions = make(map[string]*storedCustomPermissions)
	b.roleCustomPermissions = make(map[string]string)
	b.roleMemberships = make(map[string]bool)
	b.userCustomPermissions = make(map[string]string)
	b.oauthClientApps = make(map[string]*storedOAuthApp)
	b.identityPropagationConfigs = make(map[string]*storedIdentityPropagationConfig)
	b.assetBundleExportJobs = make(map[string]*storedAssetBundleExportJob)
	b.assetBundleImportJobs = make(map[string]*storedAssetBundleImportJob)
	b.dashboardSnapshotJobs = make(map[string]*storedDashboardSnapshotJob)

	b.actionConnectors = make(map[string]*storedActionConnector)
	b.automationJobs = make(map[string]*storedAutomationJob)
	b.flows = make(map[string]*storedFlow)
	b.spiceCapacity = make(map[string]string)

	b.selfUpgradeConfig = make(map[string]string)
	b.selfUpgradeRequests = make(map[string]*storedSelfUpgradeRequest)

	b.namespaces[nsKey(b.accountID, defaultNamespace)] = &storedNamespace{
		Name:           defaultNamespace,
		Arn:            b.buildARN("namespace", defaultNamespace),
		CapacityRegion: b.region,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	}
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	s := state{
		Namespaces:           b.namespaces,
		Groups:               b.groups,
		GroupMembers:         b.groupMembers,
		Users:                b.users,
		DataSources:          b.dataSources,
		DataSets:             b.dataSets,
		Ingestions:           b.ingestions,
		Dashboards:           b.dashboards,
		Analyses:             b.analyses,
		Tags:                 b.tags,
		Folders:              b.folders,
		FolderMembers:        b.folderMembers,
		Templates:            b.templates,
		Themes:               b.themes,
		Topics:               b.topics,
		VPCConnections:       b.vpcConnections,
		IAMPolicyAssignments: b.iamPolicyAssignments,

		AccountSettings:          b.accountSettings,
		AccountSubscriptions:     b.accountSubscriptions,
		AccountCustomizations:    b.accountCustomizations,
		AccountCustomPermissions: b.accountCustomPermissions,
		IPRestrictions:           b.ipRestrictions,
		PublicSharing:            b.publicSharing,
		KeyRegistrations:         b.keyRegistrations,
		DefaultQBusinessApps:     b.defaultQBusinessApps,
		QPersonalization:         b.qPersonalization,
		QSearchConfig:            b.qSearchConfig,
		DashboardsQAConfig:       b.dashboardsQAConfig,

		Brands:                     b.brands,
		BrandAssignments:           b.brandAssignments,
		CustomPermissions:          b.customPermissions,
		RoleCustomPermissions:      b.roleCustomPermissions,
		RoleMemberships:            b.roleMemberships,
		UserCustomPermissions:      b.userCustomPermissions,
		OAuthClientApps:            b.oauthClientApps,
		IdentityPropagationConfigs: b.identityPropagationConfigs,
		AssetBundleExportJobs:      b.assetBundleExportJobs,
		AssetBundleImportJobs:      b.assetBundleImportJobs,
		DashboardSnapshotJobs:      b.dashboardSnapshotJobs,

		ActionConnectors: b.actionConnectors,
		AutomationJobs:   b.automationJobs,
		Flows:            b.flows,
		SPICECapacity:    b.spiceCapacity,

		SelfUpgradeConfig:   b.selfUpgradeConfig,
		SelfUpgradeRequests: b.selfUpgradeRequests,
	}

	data, _ := json.Marshal(s)

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("quicksight: restore: %w", err)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.namespaces = s.Namespaces
	b.groups = s.Groups
	b.groupMembers = s.GroupMembers
	b.users = s.Users
	b.dataSources = s.DataSources
	b.dataSets = s.DataSets
	b.ingestions = s.Ingestions
	b.dashboards = s.Dashboards
	b.analyses = s.Analyses
	b.tags = s.Tags
	b.folders = s.Folders
	b.folderMembers = s.FolderMembers
	b.templates = s.Templates
	b.themes = s.Themes
	b.topics = s.Topics
	b.vpcConnections = s.VPCConnections
	b.iamPolicyAssignments = s.IAMPolicyAssignments

	b.accountSettings = s.AccountSettings
	b.accountSubscriptions = s.AccountSubscriptions
	b.accountCustomizations = s.AccountCustomizations
	b.accountCustomPermissions = s.AccountCustomPermissions
	b.ipRestrictions = s.IPRestrictions
	b.publicSharing = s.PublicSharing
	b.keyRegistrations = s.KeyRegistrations
	b.defaultQBusinessApps = s.DefaultQBusinessApps
	b.qPersonalization = s.QPersonalization
	b.qSearchConfig = s.QSearchConfig
	b.dashboardsQAConfig = s.DashboardsQAConfig

	b.restoreAppendixBatchFields(s)
	b.restoreFinalStubFields(s)
	b.ensureLegacyResourceMaps()
	b.ensureAccountConfigMaps()
	b.ensureFinalStubMaps()

	return nil
}

// ensureLegacyResourceMaps re-initializes any pre-Appendix-A resource maps
// left nil after Restore (e.g. snapshots taken before those maps existed).
// Split out of Restore purely to keep Restore's statement count in budget.
func (b *InMemoryBackend) ensureLegacyResourceMaps() {
	if b.folders == nil {
		b.folders = make(map[string]*storedFolder)
	}
	if b.folderMembers == nil {
		b.folderMembers = make(map[string]*storedFolderMember)
	}
	if b.templates == nil {
		b.templates = make(map[string]*storedTemplate)
	}
	if b.themes == nil {
		b.themes = make(map[string]*storedTheme)
	}
	if b.topics == nil {
		b.topics = make(map[string]*storedTopic)
	}
	if b.vpcConnections == nil {
		b.vpcConnections = make(map[string]*storedVPCConnection)
	}
	if b.iamPolicyAssignments == nil {
		b.iamPolicyAssignments = make(map[string]*storedIAMPolicyAssignment)
	}
}

// ensureFinalStubMaps re-initializes any maps introduced by the final
// canned-stub batch (action connectors, automation jobs, flows, SPICE
// capacity) or the parity-sweep batch (self-upgrade config/requests) left nil
// after Restore (e.g. snapshots taken before those maps existed).
func (b *InMemoryBackend) ensureFinalStubMaps() {
	if b.actionConnectors == nil {
		b.actionConnectors = make(map[string]*storedActionConnector)
	}
	if b.automationJobs == nil {
		b.automationJobs = make(map[string]*storedAutomationJob)
	}
	if b.flows == nil {
		b.flows = make(map[string]*storedFlow)
	}
	if b.spiceCapacity == nil {
		b.spiceCapacity = make(map[string]string)
	}
	if b.selfUpgradeConfig == nil {
		b.selfUpgradeConfig = make(map[string]string)
	}
	if b.selfUpgradeRequests == nil {
		b.selfUpgradeRequests = make(map[string]*storedSelfUpgradeRequest)
	}
}

// ensureAccountConfigMaps re-initializes any account/config-cluster maps left nil
// after Restore (e.g. snapshots taken before those maps existed).
func (b *InMemoryBackend) ensureAccountConfigMaps() {
	if b.accountSettings == nil {
		b.accountSettings = make(map[string]*storedAccountSettings)
	}
	if b.accountSubscriptions == nil {
		b.accountSubscriptions = make(map[string]*storedAccountSubscription)
	}
	if b.accountCustomizations == nil {
		b.accountCustomizations = make(map[string]*storedAccountCustomization)
	}
	if b.accountCustomPermissions == nil {
		b.accountCustomPermissions = make(map[string]string)
	}
	if b.ipRestrictions == nil {
		b.ipRestrictions = make(map[string]*storedIPRestriction)
	}
	if b.publicSharing == nil {
		b.publicSharing = make(map[string]bool)
	}
	if b.keyRegistrations == nil {
		b.keyRegistrations = make(map[string][]storedRegisteredKey)
	}
	if b.defaultQBusinessApps == nil {
		b.defaultQBusinessApps = make(map[string]*storedDefaultQBusinessApplication)
	}
	if b.qPersonalization == nil {
		b.qPersonalization = make(map[string]string)
	}
	if b.qSearchConfig == nil {
		b.qSearchConfig = make(map[string]string)
	}
	if b.dashboardsQAConfig == nil {
		b.dashboardsQAConfig = make(map[string]string)
	}
	b.ensureAppendixBatchMaps()
}

// restoreAppendixBatchFields copies the final Appendix-A batch's fields
// (brands, custom permissions, OAuth apps, identity propagation, asset
// bundle/snapshot jobs, refresh schedules) from a deserialized snapshot onto b.
// Split out of Restore purely to keep Restore's statement count in budget.
func (b *InMemoryBackend) restoreAppendixBatchFields(s state) {
	b.brands = s.Brands
	b.brandAssignments = s.BrandAssignments
	b.customPermissions = s.CustomPermissions
	b.roleCustomPermissions = s.RoleCustomPermissions
	b.roleMemberships = s.RoleMemberships
	b.userCustomPermissions = s.UserCustomPermissions
	b.oauthClientApps = s.OAuthClientApps
	b.identityPropagationConfigs = s.IdentityPropagationConfigs
	b.assetBundleExportJobs = s.AssetBundleExportJobs
	b.assetBundleImportJobs = s.AssetBundleImportJobs
	b.dashboardSnapshotJobs = s.DashboardSnapshotJobs
}

// restoreFinalStubFields copies the final canned-stub batch's fields (action
// connectors, automation jobs, flows, SPICE capacity) from a deserialized
// snapshot onto b. Split out of Restore purely to keep Restore's statement
// count in budget.
func (b *InMemoryBackend) restoreFinalStubFields(s state) {
	b.actionConnectors = s.ActionConnectors
	b.automationJobs = s.AutomationJobs
	b.flows = s.Flows
	b.spiceCapacity = s.SPICECapacity
	b.selfUpgradeConfig = s.SelfUpgradeConfig
	b.selfUpgradeRequests = s.SelfUpgradeRequests
}

// ensureAppendixBatchMaps re-initializes any maps introduced by the final
// Appendix-A batch (brands, custom permissions, OAuth apps, identity
// propagation, asset bundle/snapshot jobs, refresh schedules) left nil after
// Restore (e.g. snapshots taken before those maps existed).
func (b *InMemoryBackend) ensureAppendixBatchMaps() {
	if b.brands == nil {
		b.brands = make(map[string]*storedBrand)
	}
	if b.brandAssignments == nil {
		b.brandAssignments = make(map[string]string)
	}
	if b.customPermissions == nil {
		b.customPermissions = make(map[string]*storedCustomPermissions)
	}
	if b.roleCustomPermissions == nil {
		b.roleCustomPermissions = make(map[string]string)
	}
	if b.roleMemberships == nil {
		b.roleMemberships = make(map[string]bool)
	}
	if b.userCustomPermissions == nil {
		b.userCustomPermissions = make(map[string]string)
	}
	if b.oauthClientApps == nil {
		b.oauthClientApps = make(map[string]*storedOAuthApp)
	}
	if b.identityPropagationConfigs == nil {
		b.identityPropagationConfigs = make(map[string]*storedIdentityPropagationConfig)
	}
	if b.assetBundleExportJobs == nil {
		b.assetBundleExportJobs = make(map[string]*storedAssetBundleExportJob)
	}
	if b.assetBundleImportJobs == nil {
		b.assetBundleImportJobs = make(map[string]*storedAssetBundleImportJob)
	}
	if b.dashboardSnapshotJobs == nil {
		b.dashboardSnapshotJobs = make(map[string]*storedDashboardSnapshotJob)
	}
}

// ---- key helpers ----

func nsKey(accountID, namespace string) string {
	return accountID + "/" + namespace
}

func groupKey(accountID, namespace, groupName string) string {
	return accountID + "/" + namespace + "/" + groupName
}

func groupMemberKey(accountID, namespace, groupName, memberName string) string {
	return accountID + "/" + namespace + "/" + groupName + "/" + memberName
}

func userKey(accountID, namespace, userName string) string {
	return accountID + "/" + namespace + "/" + userName
}

func dataSourceKey(accountID, dataSourceID string) string {
	return accountID + "/" + dataSourceID
}

func dataSetKey(accountID, dataSetID string) string {
	return accountID + "/" + dataSetID
}

func ingestionKey(accountID, dataSetID, ingestionID string) string {
	return accountID + "/" + dataSetID + "/" + ingestionID
}

func dashboardKey(accountID, dashboardID string) string {
	return accountID + "/" + dashboardID
}

func analysisKey(accountID, analysisID string) string {
	return accountID + "/" + analysisID
}

func folderKey(accountID, folderID string) string {
	return accountID + "/" + folderID
}

func folderMemberKey(accountID, folderID, memberType, memberID string) string {
	return accountID + "/" + folderID + "/" + memberType + "/" + memberID
}

func templateKey(accountID, templateID string) string {
	return accountID + "/" + templateID
}

func themeKey(accountID, themeID string) string {
	return accountID + "/" + themeID
}

func brandKey(accountID, brandID string) string {
	return accountID + "/" + brandID
}

func customPermissionsKey(accountID, name string) string {
	return accountID + "/" + name
}

func roleCustomPermissionKey(accountID, namespace, role string) string {
	return accountID + "/" + namespace + "/" + role
}

func roleMembershipKey(accountID, namespace, role, memberName string) string {
	return accountID + "/" + namespace + "/" + role + "/" + memberName
}

func userCustomPermissionKey(accountID, namespace, userName string) string {
	return accountID + "/" + namespace + "/" + userName
}

func oauthAppKey(accountID, clientID string) string {
	return accountID + "/" + clientID
}

func identityPropagationKey(accountID, service string) string {
	return accountID + "/" + service
}

func assetBundleJobKey(accountID, jobID string) string {
	return accountID + "/" + jobID
}

func dashboardSnapshotJobKey(accountID, dashboardID, jobID string) string {
	return accountID + "/" + dashboardID + "/" + jobID
}

// ---- ARN builder ----

func (b *InMemoryBackend) buildARN(resourceType, resourceID string) string {
	return fmt.Sprintf("arn:aws:quicksight:%s:%s:%s/%s", b.region, b.accountID, resourceType, resourceID)
}

// ---- Namespaces ----

func (b *InMemoryBackend) CreateNamespace(accountID, namespace, capacityRegion string) (*Namespace, error) {
	if namespace == "" {
		return nil, ErrValidation
	}

	if capacityRegion == "" {
		capacityRegion = b.region
	}

	b.mu.Lock("CreateNamespace")
	defer b.mu.Unlock()

	key := nsKey(accountID, namespace)
	if _, exists := b.namespaces[key]; exists {
		return nil, ErrNamespaceAlreadyExists
	}

	ns := &storedNamespace{
		Name:           namespace,
		Arn:            fmt.Sprintf("arn:aws:quicksight:%s:%s:namespace/%s", b.region, accountID, namespace),
		CapacityRegion: capacityRegion,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	}
	b.namespaces[key] = ns

	return ns.toNamespace(), nil
}

func (b *InMemoryBackend) DescribeNamespace(accountID, namespace string) (*Namespace, error) {
	b.mu.RLock("DescribeNamespace")
	defer b.mu.RUnlock()

	ns, ok := b.namespaces[nsKey(accountID, namespace)]
	if !ok {
		return nil, ErrNamespaceNotFound
	}

	return ns.toNamespace(), nil
}

func (b *InMemoryBackend) DeleteNamespace(accountID, namespace string) error {
	if namespace == defaultNamespace {
		return ErrValidation
	}

	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	key := nsKey(accountID, namespace)
	if _, ok := b.namespaces[key]; !ok {
		return ErrNamespaceNotFound
	}

	delete(b.namespaces, key)

	return nil
}

func (b *InMemoryBackend) ListNamespaces(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Namespace, string, error) {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	var all []*storedNamespace
	prefix := accountID + "/"
	for k, ns := range b.namespaces {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ns)
		}
	}

	result, next := paginateNamespaces(all, maxResults, nextToken)

	return result, next, nil
}

func paginateNamespaces(all []*storedNamespace, maxResults int32, nextToken string) ([]*Namespace, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ns := range all {
			if ns.Name == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].Name
	} else {
		end = len(all)
	}

	result := make([]*Namespace, 0, end-start)
	for _, ns := range all[start:end] {
		result = append(result, ns.toNamespace())
	}

	return result, next
}

// ---- Groups ----

func (b *InMemoryBackend) CreateGroup(accountID, namespace, groupName, description string) (*Group, error) {
	if groupName == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return nil, ErrNamespaceNotFound
	}

	key := groupKey(accountID, namespace, groupName)
	if _, exists := b.groups[key]; exists {
		return nil, ErrGroupAlreadyExists
	}

	g := &storedGroup{
		GroupName:   groupName,
		Arn:         fmt.Sprintf("arn:aws:quicksight:%s:%s:group/%s/%s", b.region, accountID, namespace, groupName),
		Description: description,
		Namespace:   namespace,
		PrincipalID: uuid.New().String(),
	}
	b.groups[key] = g

	return g.toGroup(), nil
}

func (b *InMemoryBackend) DescribeGroup(accountID, namespace, groupName string) (*Group, error) {
	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups[groupKey(accountID, namespace, groupName)]
	if !ok {
		return nil, ErrGroupNotFound
	}

	return g.toGroup(), nil
}

func (b *InMemoryBackend) UpdateGroup(accountID, namespace, groupName, description string) (*Group, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	key := groupKey(accountID, namespace, groupName)
	g, ok := b.groups[key]
	if !ok {
		return nil, ErrGroupNotFound
	}

	g.Description = description

	return g.toGroup(), nil
}

func (b *InMemoryBackend) DeleteGroup(accountID, namespace, groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	key := groupKey(accountID, namespace, groupName)
	if _, ok := b.groups[key]; !ok {
		return ErrGroupNotFound
	}

	delete(b.groups, key)

	// Remove all memberships for this group.
	prefix := groupKey(accountID, namespace, groupName) + "/"
	for k := range b.groupMembers {
		if strings.HasPrefix(k, prefix) {
			delete(b.groupMembers, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListGroups(
	accountID, namespace string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	prefix := groupKey(accountID, namespace, "") + "/"
	// prefix for groups in this namespace: "accountID/namespace/"
	nsPrefix := accountID + "/" + namespace + "/"
	var all []*storedGroup
	for k, g := range b.groups {
		if strings.HasPrefix(k, nsPrefix) && !strings.Contains(k[len(nsPrefix):], "/") {
			_ = prefix
			all = append(all, g)
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchGroups(
	accountID, namespace, query string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("SearchGroups")
	defer b.mu.RUnlock()

	nsPrefix := accountID + "/" + namespace + "/"
	var all []*storedGroup
	for k, g := range b.groups {
		if strings.HasPrefix(k, nsPrefix) && !strings.Contains(k[len(nsPrefix):], "/") {
			if query == "" || strings.Contains(strings.ToLower(g.GroupName), strings.ToLower(query)) {
				all = append(all, g)
			}
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

func paginateGroups(all []*storedGroup, maxResults int32, nextToken string) ([]*Group, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, g := range all {
			if g.GroupName == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].GroupName
	} else {
		end = len(all)
	}

	result := make([]*Group, 0, end-start)
	for _, g := range all[start:end] {
		result = append(result, g.toGroup())
	}

	return result, next
}

// ---- Group Memberships ----

func (b *InMemoryBackend) CreateGroupMembership(
	accountID, namespace, groupName, memberName string,
) (*GroupMember, error) {
	b.mu.Lock("CreateGroupMembership")
	defer b.mu.Unlock()

	if _, ok := b.groups[groupKey(accountID, namespace, groupName)]; !ok {
		return nil, ErrGroupNotFound
	}

	key := groupMemberKey(accountID, namespace, groupName, memberName)
	if b.groupMembers[key] {
		return nil, ErrGroupMemberAlreadyExists
	}

	b.groupMembers[key] = true

	return &GroupMember{
		MemberName: memberName,
		Arn:        fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, memberName),
	}, nil
}

func (b *InMemoryBackend) DescribeGroupMembership(
	accountID, namespace, groupName, memberName string,
) (*GroupMember, error) {
	b.mu.RLock("DescribeGroupMembership")
	defer b.mu.RUnlock()

	if !b.groupMembers[groupMemberKey(accountID, namespace, groupName, memberName)] {
		return nil, ErrGroupMemberNotFound
	}

	return &GroupMember{
		MemberName: memberName,
		Arn:        fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, memberName),
	}, nil
}

func (b *InMemoryBackend) DeleteGroupMembership(accountID, namespace, groupName, memberName string) error {
	b.mu.Lock("DeleteGroupMembership")
	defer b.mu.Unlock()

	key := groupMemberKey(accountID, namespace, groupName, memberName)
	if !b.groupMembers[key] {
		return ErrGroupMemberNotFound
	}

	delete(b.groupMembers, key)

	return nil
}

func (b *InMemoryBackend) ListGroupMemberships(
	accountID, namespace, groupName string,
	maxResults int32,
	nextToken string,
) ([]*GroupMember, string, error) {
	b.mu.RLock("ListGroupMemberships")
	defer b.mu.RUnlock()

	if _, ok := b.groups[groupKey(accountID, namespace, groupName)]; !ok {
		return nil, "", ErrGroupNotFound
	}

	prefix := groupMemberKey(accountID, namespace, groupName, "") + "/"
	_ = prefix
	fullPrefix := accountID + "/" + namespace + "/" + groupName + "/"
	var members []string
	for k := range b.groupMembers {
		if member, ok := strings.CutPrefix(k, fullPrefix); ok {
			members = append(members, member)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, m := range members {
			if m == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(members) {
		next = members[end]
	} else {
		end = len(members)
	}

	result := make([]*GroupMember, 0, end-start)
	for _, m := range members[start:end] {
		result = append(result, &GroupMember{
			MemberName: m,
			Arn:        fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, m),
		})
	}

	return result, next, nil
}

// ---- Users ----

func (b *InMemoryBackend) RegisterUser(
	accountID, namespace, userName, email, role, identityType, sessionName string,
) (*User, error) {
	if userName == "" || email == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("RegisterUser")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return nil, ErrNamespaceNotFound
	}

	key := userKey(accountID, namespace, userName)
	if _, exists := b.users[key]; exists {
		return nil, ErrUserAlreadyExists
	}

	if role == "" {
		role = "READER"
	}
	if identityType == "" {
		identityType = identityStoreQuickSight
	}

	u := &storedUser{
		UserName:     userName,
		Arn:          fmt.Sprintf("arn:aws:quicksight:%s:%s:user/%s/%s", b.region, accountID, namespace, userName),
		Email:        email,
		Role:         role,
		IdentityType: identityType,
		Namespace:    namespace,
		PrincipalID:  uuid.New().String(),
		SessionName:  sessionName,
		Active:       true,
	}
	b.users[key] = u

	return u.toUser(), nil
}

func (b *InMemoryBackend) DescribeUser(accountID, namespace, userName string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	u, ok := b.users[userKey(accountID, namespace, userName)]
	if !ok {
		return nil, ErrUserNotFound
	}

	return u.toUser(), nil
}

func (b *InMemoryBackend) UpdateUser(accountID, namespace, userName, email, role string) (*User, error) {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	key := userKey(accountID, namespace, userName)
	u, ok := b.users[key]
	if !ok {
		return nil, ErrUserNotFound
	}

	if email != "" {
		u.Email = email
	}
	if role != "" {
		u.Role = role
	}

	return u.toUser(), nil
}

func (b *InMemoryBackend) DeleteUser(accountID, namespace, userName string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	key := userKey(accountID, namespace, userName)
	if _, ok := b.users[key]; !ok {
		return ErrUserNotFound
	}

	delete(b.users, key)

	return nil
}

func (b *InMemoryBackend) DeleteUserByPrincipalID(accountID, namespace, principalID string) error {
	b.mu.Lock("DeleteUserByPrincipalID")
	defer b.mu.Unlock()

	prefix := accountID + "/" + namespace + "/"
	for k, u := range b.users {
		if strings.HasPrefix(k, prefix) && u.PrincipalID == principalID {
			delete(b.users, k)

			return nil
		}
	}

	return ErrUserNotFound
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListUsers(
	accountID, namespace string,
	maxResults int32,
	nextToken string,
) ([]*User, string, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	prefix := accountID + "/" + namespace + "/"
	var all []*storedUser
	for k, u := range b.users {
		if strings.HasPrefix(k, prefix) {
			all = append(all, u)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, u := range all {
			if u.UserName == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].UserName
	} else {
		end = len(all)
	}

	result := make([]*User, 0, end-start)
	for _, u := range all[start:end] {
		result = append(result, u.toUser())
	}

	return result, next, nil
}

func (b *InMemoryBackend) ListUserGroups(
	accountID, namespace, userName string,
	maxResults int32,
	nextToken string,
) ([]*Group, string, error) {
	b.mu.RLock("ListUserGroups")
	defer b.mu.RUnlock()

	if _, ok := b.users[userKey(accountID, namespace, userName)]; !ok {
		return nil, "", ErrUserNotFound
	}

	nsPrefix := accountID + "/" + namespace + "/"
	var all []*storedGroup
	for gKey, g := range b.groups {
		if !strings.HasPrefix(gKey, nsPrefix) {
			continue
		}
		memberKey := groupMemberKey(accountID, namespace, g.GroupName, userName)
		if b.groupMembers[memberKey] {
			all = append(all, g)
		}
	}

	result, next := paginateGroups(all, maxResults, nextToken)

	return result, next, nil
}

// ---- DataSources ----

func (b *InMemoryBackend) CreateDataSource(
	accountID, dataSourceID, name, dsType string,
	permissions []ResourcePermission,
	tags map[string]string,
) (*DataSource, error) {
	if dataSourceID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	if _, exists := b.dataSources[key]; exists {
		return nil, ErrDataSourceAlreadyExists
	}

	now := time.Now().UTC()
	ds := &storedDataSource{
		CreatedTime:     now,
		LastUpdatedTime: now,
		DataSourceID:    dataSourceID,
		Arn:             fmt.Sprintf("arn:aws:quicksight:%s:%s:datasource/%s", b.region, accountID, dataSourceID),
		Name:            name,
		Type:            dsType,
		Status:          statusCreationSuccessful,
		Permissions:     clonePermissions(permissions),
	}
	b.dataSources[key] = ds

	if len(tags) > 0 {
		b.tags[ds.Arn] = maps.Clone(tags)
	}

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) DescribeDataSource(accountID, dataSourceID string) (*DataSource, error) {
	b.mu.RLock("DescribeDataSource")
	defer b.mu.RUnlock()

	ds, ok := b.dataSources[dataSourceKey(accountID, dataSourceID)]
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) UpdateDataSource(accountID, dataSourceID, name string) (*DataSource, error) {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	ds, ok := b.dataSources[key]
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	if name != "" {
		ds.Name = name
	}
	ds.LastUpdatedTime = time.Now().UTC()
	ds.Status = statusUpdateSuccessful

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) DeleteDataSource(accountID, dataSourceID string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	ds, ok := b.dataSources[key]
	if !ok {
		return ErrDataSourceNotFound
	}

	delete(b.tags, ds.Arn)
	delete(b.dataSources, key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDataSources(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*DataSource, string, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedDataSource
	for k, ds := range b.dataSources {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ds)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range all {
			if ds.DataSourceID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DataSourceID
	} else {
		end = len(all)
	}

	result := make([]*DataSource, 0, end-start)
	for _, ds := range all[start:end] {
		result = append(result, ds.toDataSource())
	}

	return result, next, nil
}

// SearchDataSources searches data sources by name (filter Name ==
// filterDataSourceName); any other filter Name is an ownership-related filter
// that this in-memory backend doesn't track and is treated as a pass-through
// match, mirroring folderMatchesFilter's permissive default.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchDataSources(
	accountID string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*DataSource, string, error) {
	b.mu.RLock("SearchDataSources")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var filtered []*storedDataSource
	for k, ds := range b.dataSources {
		if strings.HasPrefix(k, prefix) && matchesAllNameFilters(ds.Name, filters, filterDataSourceName) {
			filtered = append(filtered, ds)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].DataSourceID < filtered[j].DataSourceID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range filtered {
			if ds.DataSourceID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].DataSourceID
	} else {
		end = len(filtered)
	}

	result := make([]*DataSource, 0, end-start)
	for _, ds := range filtered[start:end] {
		result = append(result, ds.toDataSource())
	}

	return result, next, nil
}

// ---- DataSource permissions ----

func (b *InMemoryBackend) DescribeDataSourcePermissions(
	accountID, dataSourceID string,
) (*DataSource, []ResourcePermission, error) {
	b.mu.RLock("DescribeDataSourcePermissions")
	defer b.mu.RUnlock()

	ds, ok := b.dataSources[dataSourceKey(accountID, dataSourceID)]
	if !ok {
		return nil, nil, ErrDataSourceNotFound
	}

	return ds.toDataSource(), clonePermissions(ds.Permissions), nil
}

func (b *InMemoryBackend) UpdateDataSourcePermissions(
	accountID, dataSourceID string,
	grant, revoke []ResourcePermission,
) (*DataSource, []ResourcePermission, error) {
	b.mu.Lock("UpdateDataSourcePermissions")
	defer b.mu.Unlock()

	ds, ok := b.dataSources[dataSourceKey(accountID, dataSourceID)]
	if !ok {
		return nil, nil, ErrDataSourceNotFound
	}

	ds.Permissions = applyGrantRevoke(ds.Permissions, grant, revoke)
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.toDataSource(), clonePermissions(ds.Permissions), nil
}

// ---- DataSets ----

func (b *InMemoryBackend) CreateDataSet(
	accountID, dataSetID, name, importMode string,
	permissions []ResourcePermission,
	tags map[string]string,
) (*DataSet, error) {
	if dataSetID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	if _, exists := b.dataSets[key]; exists {
		return nil, ErrDataSetAlreadyExists
	}

	if importMode == "" {
		importMode = "SPICE"
	}

	now := time.Now().UTC()
	ds := &storedDataSet{
		CreatedTime:      now,
		LastUpdatedTime:  now,
		DataSetID:        dataSetID,
		Arn:              fmt.Sprintf("arn:aws:quicksight:%s:%s:dataset/%s", b.region, accountID, dataSetID),
		Name:             name,
		ImportMode:       importMode,
		RefreshSchedules: make(map[string]*storedRefreshSchedule),
		Permissions:      clonePermissions(permissions),
	}
	b.dataSets[key] = ds

	if len(tags) > 0 {
		b.tags[ds.Arn] = maps.Clone(tags)
	}

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) DescribeDataSet(accountID, dataSetID string) (*DataSet, error) {
	b.mu.RLock("DescribeDataSet")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets[dataSetKey(accountID, dataSetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) UpdateDataSet(accountID, dataSetID, name, importMode string) (*DataSet, error) {
	b.mu.Lock("UpdateDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	ds, ok := b.dataSets[key]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	if name != "" {
		ds.Name = name
	}
	if importMode != "" {
		ds.ImportMode = importMode
	}
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) DeleteDataSet(accountID, dataSetID string) error {
	b.mu.Lock("DeleteDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	ds, ok := b.dataSets[key]
	if !ok {
		return ErrDataSetNotFound
	}

	delete(b.tags, ds.Arn)
	delete(b.dataSets, key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDataSets(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*DataSet, string, error) {
	b.mu.RLock("ListDataSets")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedDataSet
	for k, ds := range b.dataSets {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ds)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range all {
			if ds.DataSetID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DataSetID
	} else {
		end = len(all)
	}

	result := make([]*DataSet, 0, end-start)
	for _, ds := range all[start:end] {
		result = append(result, ds.toDataSet())
	}

	return result, next, nil
}

// SearchDataSets searches data sets by name (filter Name == filterDataSetName);
// any other filter Name is an ownership-related filter that this in-memory
// backend doesn't track and is treated as a pass-through match.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchDataSets(
	accountID string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*DataSet, string, error) {
	b.mu.RLock("SearchDataSets")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var filtered []*storedDataSet
	for k, ds := range b.dataSets {
		if strings.HasPrefix(k, prefix) && matchesAllNameFilters(ds.Name, filters, filterDataSetName) {
			filtered = append(filtered, ds)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].DataSetID < filtered[j].DataSetID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range filtered {
			if ds.DataSetID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].DataSetID
	} else {
		end = len(filtered)
	}

	result := make([]*DataSet, 0, end-start)
	for _, ds := range filtered[start:end] {
		result = append(result, ds.toDataSet())
	}

	return result, next, nil
}

// ---- DataSet permissions ----

func (b *InMemoryBackend) DescribeDataSetPermissions(
	accountID, dataSetID string,
) (*DataSet, []ResourcePermission, error) {
	b.mu.RLock("DescribeDataSetPermissions")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets[dataSetKey(accountID, dataSetID)]
	if !ok {
		return nil, nil, ErrDataSetNotFound
	}

	return ds.toDataSet(), clonePermissions(ds.Permissions), nil
}

func (b *InMemoryBackend) UpdateDataSetPermissions(
	accountID, dataSetID string,
	grant, revoke []ResourcePermission,
) (*DataSet, []ResourcePermission, error) {
	b.mu.Lock("UpdateDataSetPermissions")
	defer b.mu.Unlock()

	ds, ok := b.dataSets[dataSetKey(accountID, dataSetID)]
	if !ok {
		return nil, nil, ErrDataSetNotFound
	}

	ds.Permissions = applyGrantRevoke(ds.Permissions, grant, revoke)
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.toDataSet(), clonePermissions(ds.Permissions), nil
}

// ---- Ingestions ----

func (b *InMemoryBackend) CreateIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error) {
	b.mu.Lock("CreateIngestion")
	defer b.mu.Unlock()

	if _, ok := b.dataSets[dataSetKey(accountID, dataSetID)]; !ok {
		return nil, ErrDataSetNotFound
	}

	key := ingestionKey(accountID, dataSetID, ingestionID)
	if _, exists := b.ingestions[key]; exists {
		return nil, ErrIngestionAlreadyExists
	}

	ing := &storedIngestion{
		CreatedTime: time.Now().UTC(),
		IngestionID: ingestionID,
		Arn: fmt.Sprintf(
			"arn:aws:quicksight:%s:%s:dataset/%s/ingestion/%s",
			b.region,
			accountID,
			dataSetID,
			ingestionID,
		),
		DataSetID:       dataSetID,
		IngestionStatus: statusRunning,
	}
	b.ingestions[key] = ing

	return ing.toIngestion(), nil
}

func (b *InMemoryBackend) DescribeIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error) {
	b.mu.RLock("DescribeIngestion")
	defer b.mu.RUnlock()

	ing, ok := b.ingestions[ingestionKey(accountID, dataSetID, ingestionID)]
	if !ok {
		return nil, ErrIngestionNotFound
	}

	return ing.toIngestion(), nil
}

func (b *InMemoryBackend) CancelIngestion(accountID, dataSetID, ingestionID string) error {
	b.mu.Lock("CancelIngestion")
	defer b.mu.Unlock()

	key := ingestionKey(accountID, dataSetID, ingestionID)
	ing, ok := b.ingestions[key]
	if !ok {
		return ErrIngestionNotFound
	}

	ing.IngestionStatus = statusCancelled

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListIngestions(
	accountID, dataSetID string,
	maxResults int32,
	nextToken string,
) ([]*Ingestion, string, error) {
	b.mu.RLock("ListIngestions")
	defer b.mu.RUnlock()

	prefix := accountID + "/" + dataSetID + "/"
	var all []*storedIngestion
	for k, ing := range b.ingestions {
		if strings.HasPrefix(k, prefix) {
			all = append(all, ing)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ing := range all {
			if ing.IngestionID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].IngestionID
	} else {
		end = len(all)
	}

	result := make([]*Ingestion, 0, end-start)
	for _, ing := range all[start:end] {
		result = append(result, ing.toIngestion())
	}

	return result, next, nil
}

// ---- Dashboards ----

func (b *InMemoryBackend) CreateDashboard(
	accountID, dashboardID, name string,
	definition map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Dashboard, error) {
	if dashboardID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDashboard")
	defer b.mu.Unlock()

	key := dashboardKey(accountID, dashboardID)
	if _, exists := b.dashboards[key]; exists {
		return nil, ErrDashboardAlreadyExists
	}

	now := time.Now().UTC()
	d := &storedDashboard{
		CreatedTime:            now,
		LastUpdatedTime:        now,
		DashboardID:            dashboardID,
		Arn:                    fmt.Sprintf("arn:aws:quicksight:%s:%s:dashboard/%s", b.region, accountID, dashboardID),
		Name:                   name,
		Status:                 statusCreated,
		VersionNumber:          1,
		PublishedVersionNumber: 1,
		Definition:             definition,
		Permissions:            clonePermissions(permissions),
	}
	b.dashboards[key] = d

	if len(tags) > 0 {
		b.tags[d.Arn] = maps.Clone(tags)
	}

	return d.toDashboard(), nil
}

func (b *InMemoryBackend) DescribeDashboard(accountID, dashboardID string) (*Dashboard, error) {
	b.mu.RLock("DescribeDashboard")
	defer b.mu.RUnlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, ErrDashboardNotFound
	}

	return d.toDashboard(), nil
}

func (b *InMemoryBackend) UpdateDashboard(
	accountID, dashboardID, name string,
	definition map[string]any,
) (*Dashboard, error) {
	b.mu.Lock("UpdateDashboard")
	defer b.mu.Unlock()

	key := dashboardKey(accountID, dashboardID)
	d, ok := b.dashboards[key]
	if !ok {
		return nil, ErrDashboardNotFound
	}

	if name != "" {
		d.Name = name
	}
	if definition != nil {
		d.Definition = definition
	}
	d.LastUpdatedTime = time.Now().UTC()
	d.VersionNumber++

	return d.toDashboard(), nil
}

func (b *InMemoryBackend) DeleteDashboard(accountID, dashboardID string) error {
	b.mu.Lock("DeleteDashboard")
	defer b.mu.Unlock()

	key := dashboardKey(accountID, dashboardID)
	d, ok := b.dashboards[key]
	if !ok {
		return ErrDashboardNotFound
	}

	delete(b.tags, d.Arn)
	delete(b.dashboards, key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDashboards(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Dashboard, string, error) {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedDashboard
	for k, d := range b.dashboards {
		if strings.HasPrefix(k, prefix) {
			all = append(all, d)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, d := range all {
			if d.DashboardID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DashboardID
	} else {
		end = len(all)
	}

	result := make([]*Dashboard, 0, end-start)
	for _, d := range all[start:end] {
		result = append(result, d.toDashboard())
	}

	return result, next, nil
}

func (b *InMemoryBackend) ListDashboardVersions(
	accountID, dashboardID string,
	_ int32,
	_ string,
) ([]*DashboardVersion, string, error) {
	b.mu.RLock("ListDashboardVersions")
	defer b.mu.RUnlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, "", ErrDashboardNotFound
	}

	versions := make([]*DashboardVersion, 0, d.VersionNumber)
	for i := int64(1); i <= d.VersionNumber; i++ {
		versions = append(versions, &DashboardVersion{
			CreatedTime:   d.CreatedTime,
			Arn:           fmt.Sprintf("%s/version/%d", d.Arn, i),
			Status:        statusCreated,
			VersionNumber: i,
		})
	}

	return versions, "", nil
}

// SearchDashboards searches dashboards by name (filter Name ==
// filterDashboardName); any other filter Name is an ownership-related filter
// that this in-memory backend doesn't track and is treated as a pass-through
// match.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchDashboards(
	accountID string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Dashboard, string, error) {
	b.mu.RLock("SearchDashboards")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var filtered []*storedDashboard
	for k, d := range b.dashboards {
		if strings.HasPrefix(k, prefix) && matchesAllNameFilters(d.Name, filters, filterDashboardName) {
			filtered = append(filtered, d)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].DashboardID < filtered[j].DashboardID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, d := range filtered {
			if d.DashboardID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].DashboardID
	} else {
		end = len(filtered)
	}

	result := make([]*Dashboard, 0, end-start)
	for _, d := range filtered[start:end] {
		result = append(result, d.toDashboard())
	}

	return result, next, nil
}

// UpdateDashboardPublishedVersion flips which stored version of a dashboard is
// the published one. versionNumber must name a version that actually exists
// (i.e. be in [1, VersionNumber], matching the versions ListDashboardVersions
// synthesizes), else ErrDashboardVersionNotFound.
func (b *InMemoryBackend) UpdateDashboardPublishedVersion(
	accountID, dashboardID string,
	versionNumber int64,
) (*Dashboard, error) {
	b.mu.Lock("UpdateDashboardPublishedVersion")
	defer b.mu.Unlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, ErrDashboardNotFound
	}

	if versionNumber < 1 || versionNumber > d.VersionNumber {
		return nil, ErrDashboardVersionNotFound
	}

	d.PublishedVersionNumber = versionNumber
	d.LastUpdatedTime = time.Now().UTC()

	return d.toDashboard(), nil
}

// UpdateDashboardLinks replaces the set of analysis ARNs linked to a dashboard.
func (b *InMemoryBackend) UpdateDashboardLinks(
	accountID, dashboardID string,
	linkEntities []string,
) (*Dashboard, error) {
	b.mu.Lock("UpdateDashboardLinks")
	defer b.mu.Unlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, ErrDashboardNotFound
	}

	d.LinkEntities = linkEntities
	d.LastUpdatedTime = time.Now().UTC()

	return d.toDashboard(), nil
}

// ---- Dashboard permissions ----

func (b *InMemoryBackend) DescribeDashboardPermissions(
	accountID, dashboardID string,
) (*Dashboard, []ResourcePermission, error) {
	b.mu.RLock("DescribeDashboardPermissions")
	defer b.mu.RUnlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, nil, ErrDashboardNotFound
	}

	return d.toDashboard(), clonePermissions(d.Permissions), nil
}

func (b *InMemoryBackend) UpdateDashboardPermissions(
	accountID, dashboardID string,
	grant, revoke []ResourcePermission,
) (*Dashboard, []ResourcePermission, error) {
	b.mu.Lock("UpdateDashboardPermissions")
	defer b.mu.Unlock()

	d, ok := b.dashboards[dashboardKey(accountID, dashboardID)]
	if !ok {
		return nil, nil, ErrDashboardNotFound
	}

	d.Permissions = applyGrantRevoke(d.Permissions, grant, revoke)
	d.LastUpdatedTime = time.Now().UTC()

	return d.toDashboard(), clonePermissions(d.Permissions), nil
}

// ---- Analyses ----

func (b *InMemoryBackend) CreateAnalysis(
	accountID, analysisID, name string,
	definition map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Analysis, error) {
	if analysisID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	if _, exists := b.analyses[key]; exists {
		return nil, ErrAnalysisAlreadyExists
	}

	now := time.Now().UTC()
	a := &storedAnalysis{
		CreatedTime:     now,
		LastUpdatedTime: now,
		AnalysisID:      analysisID,
		Arn:             fmt.Sprintf("arn:aws:quicksight:%s:%s:analysis/%s", b.region, accountID, analysisID),
		Name:            name,
		Status:          statusCreationSuccessful,
		Definition:      definition,
		Permissions:     clonePermissions(permissions),
	}
	b.analyses[key] = a

	if len(tags) > 0 {
		b.tags[a.Arn] = maps.Clone(tags)
	}

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) DescribeAnalysis(accountID, analysisID string) (*Analysis, error) {
	b.mu.RLock("DescribeAnalysis")
	defer b.mu.RUnlock()

	a, ok := b.analyses[analysisKey(accountID, analysisID)]
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) UpdateAnalysis(
	accountID, analysisID, name string,
	definition map[string]any,
) (*Analysis, error) {
	b.mu.Lock("UpdateAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses[key]
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	if name != "" {
		a.Name = name
	}
	if definition != nil {
		a.Definition = definition
	}
	a.LastUpdatedTime = time.Now().UTC()
	a.Status = statusUpdateSuccessful

	return a.toAnalysis(), nil
}

func (b *InMemoryBackend) DeleteAnalysis(accountID, analysisID string, forceDeleteWithoutRecovery bool) error {
	b.mu.Lock("DeleteAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses[key]
	if !ok {
		return ErrAnalysisNotFound
	}

	if forceDeleteWithoutRecovery {
		delete(b.tags, a.Arn)
		delete(b.analyses, key)
	} else {
		a.Status = statusDeleted
	}

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListAnalyses(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Analysis, string, error) {
	b.mu.RLock("ListAnalyses")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var all []*storedAnalysis
	for k, a := range b.analyses {
		if strings.HasPrefix(k, prefix) {
			all = append(all, a)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, a := range all {
			if a.AnalysisID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].AnalysisID
	} else {
		end = len(all)
	}

	result := make([]*Analysis, 0, end-start)
	for _, a := range all[start:end] {
		result = append(result, a.toAnalysis())
	}

	return result, next, nil
}

func (b *InMemoryBackend) RestoreAnalysis(accountID, analysisID string) (*Analysis, error) {
	b.mu.Lock("RestoreAnalysis")
	defer b.mu.Unlock()

	key := analysisKey(accountID, analysisID)
	a, ok := b.analyses[key]
	if !ok {
		return nil, ErrAnalysisNotFound
	}

	a.Status = statusCreationSuccessful
	a.LastUpdatedTime = time.Now().UTC()

	return a.toAnalysis(), nil
}

// SearchAnalyses searches analyses by name (filter Name == filterAnalysisName);
// any other filter Name is an ownership-related filter that this in-memory
// backend doesn't track and is treated as a pass-through match.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchAnalyses(
	accountID string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Analysis, string, error) {
	b.mu.RLock("SearchAnalyses")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	var filtered []*storedAnalysis
	for k, a := range b.analyses {
		if strings.HasPrefix(k, prefix) && matchesAllNameFilters(a.Name, filters, filterAnalysisName) {
			filtered = append(filtered, a)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].AnalysisID < filtered[j].AnalysisID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, a := range filtered {
			if a.AnalysisID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].AnalysisID
	} else {
		end = len(filtered)
	}

	result := make([]*Analysis, 0, end-start)
	for _, a := range filtered[start:end] {
		result = append(result, a.toAnalysis())
	}

	return result, next, nil
}

// ---- Analysis permissions ----

func (b *InMemoryBackend) DescribeAnalysisPermissions(
	accountID, analysisID string,
) (*Analysis, []ResourcePermission, error) {
	b.mu.RLock("DescribeAnalysisPermissions")
	defer b.mu.RUnlock()

	a, ok := b.analyses[analysisKey(accountID, analysisID)]
	if !ok {
		return nil, nil, ErrAnalysisNotFound
	}

	return a.toAnalysis(), clonePermissions(a.Permissions), nil
}

func (b *InMemoryBackend) UpdateAnalysisPermissions(
	accountID, analysisID string,
	grant, revoke []ResourcePermission,
) (*Analysis, []ResourcePermission, error) {
	b.mu.Lock("UpdateAnalysisPermissions")
	defer b.mu.Unlock()

	a, ok := b.analyses[analysisKey(accountID, analysisID)]
	if !ok {
		return nil, nil, ErrAnalysisNotFound
	}

	a.Permissions = applyGrantRevoke(a.Permissions, grant, revoke)
	a.LastUpdatedTime = time.Now().UTC()

	return a.toAnalysis(), clonePermissions(a.Permissions), nil
}

// ---- Tags ----

func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		return nil
	}
	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string)
	if t := b.tags[resourceARN]; t != nil {
		maps.Copy(result, t)
	}

	return result, nil
}
