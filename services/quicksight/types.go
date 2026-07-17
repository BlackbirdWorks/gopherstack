package quicksight

import "time"

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
	CreatedTime            time.Time
	LastUpdatedTime        time.Time
	Definition             map[string]any
	DashboardID            string
	Arn                    string
	Name                   string
	Status                 string
	Permissions            []ResourcePermission
	LinkEntities           []string
	VersionNumber          int64
	PublishedVersionNumber int64
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

// QAResult represents a single Predict QA result: either a generated
// natural-language answer referencing a real Topic in the account, or an
// explicit "no answer" result when no topic matches the query text.
type QAResult struct {
	AnswerID     string
	AnswerStatus string
	QuestionID   string
	QuestionText string
	TopicID      string
	TopicName    string
	ResultType   string
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

// SelfUpgradeRequestDetail represents one namespace self-upgrade request (a
// user's request to be upgraded to a different UserRole).
type SelfUpgradeRequestDetail struct {
	LastUpdateFailureReason string
	OriginalRole            string
	RequestNote             string
	RequestStatus           string
	RequestedRole           string
	UpgradeRequestID        string
	CreationTime            int64
	LastUpdateAttemptTime   int64
}

var _ StorageBackend = (*InMemoryBackend)(nil)
