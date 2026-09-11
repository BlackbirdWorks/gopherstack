package appstream

import (
	"context"
	"time"
)

// StorageBackend is the interface for AppStream 2.0 storage operations.
type StorageBackend interface {
	// Stacks
	CreateStack(name string, opts CreateStackOptions) (*Stack, error)
	DescribeStacks(names []string) ([]*Stack, error)
	UpdateStack(name string, opts UpdateStackOptions) (*Stack, error)
	DeleteStack(name string) error

	// Fleets
	CreateFleet(name string, opts CreateFleetOptions) (*Fleet, error)
	DescribeFleets(names []string) ([]*Fleet, error)
	UpdateFleet(name string, opts UpdateFleetOptions) (*Fleet, error)
	DeleteFleet(name string) error
	StartFleet(name string) error
	StopFleet(name string) error

	// Fleet associations
	AssociateFleet(fleetName, stackName string) error
	DisassociateFleet(fleetName, stackName string) error
	ListAssociatedFleets(stackName string) ([]string, error)
	ListAssociatedStacks(fleetName string) ([]string, error)

	// Tags
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
	ListTagsForResource(arn string) (map[string]string, error)

	// AppBlocks
	CreateAppBlock(name, description string, opts CreateAppBlockOptions) (*AppBlock, error)
	DeleteAppBlock(name string) error
	DescribeAppBlocks(arns []string) ([]*AppBlock, error)

	// AppBlockBuilders
	CreateAppBlockBuilder(
		name, description, platform, instanceType string,
		vpcConfig VpcConfig,
		tags map[string]string,
	) (*AppBlockBuilder, error)
	DeleteAppBlockBuilder(name string) error
	DescribeAppBlockBuilders(names []string) ([]*AppBlockBuilder, error)
	StartAppBlockBuilder(name string) error
	StopAppBlockBuilder(name string) error
	UpdateAppBlockBuilder(name, description, instanceType string, vpcConfig *VpcConfig) (*AppBlockBuilder, error)
	CreateAppBlockBuilderStreamingURL(name string, validitySeconds int64) (string, time.Time, error)

	// AppBlockBuilder-AppBlock associations. appBlockID accepts either the
	// app block Name or its Arn (real AWS's request carries AppBlockArn).
	AssociateAppBlockBuilderAppBlock(builderName, appBlockID string) (*AppBlockBuilderAppBlockAssociation, error)
	DisassociateAppBlockBuilderAppBlock(builderName, appBlockID string) error
	DescribeAppBlockBuilderAppBlockAssociations(
		builderName, appBlockID string,
	) ([]*AppBlockBuilderAppBlockAssociation, error)

	// Applications
	CreateApplication(name, displayName, description, launchPath, appBlockArn string,
		platforms []string, iconS3Location S3Location, instanceFamilies []string,
		tags map[string]string, launchParameters, workingDirectory string) (*Application, error)
	DeleteApplication(name string) error
	DescribeApplications(arns []string) ([]*Application, error)
	UpdateApplication(name, displayName, description, launchPath, launchParameters, workingDirectory string,
	) (*Application, error)
	DescribeAppLicenseUsage() ([]map[string]string, error)

	// Application-Fleet associations. appID accepts either the application
	// Name or its Arn (real AWS's request carries ApplicationArn).
	AssociateApplicationFleet(appID, fleetName string) (*ApplicationFleetAssociation, error)
	DisassociateApplicationFleet(appID, fleetName string) error
	DescribeApplicationFleetAssociations(appID, fleetName string) ([]*ApplicationFleetAssociation, error)

	// Entitlements
	CreateEntitlement(name, stackName, description, appVisibility string,
		attributes []EntitlementAttribute) (*Entitlement, error)
	DeleteEntitlement(name, stackName string) error
	DescribeEntitlements(name, stackName string) ([]*Entitlement, error)
	UpdateEntitlement(name, stackName, description, appVisibility string,
		attributes []EntitlementAttribute) (*Entitlement, error)
	AssociateApplicationToEntitlement(appID, entitlementName, stackName string) error
	DisassociateApplicationFromEntitlement(appID, entitlementName, stackName string) error
	ListEntitledApplications(entitlementName, stackName string) ([]*EntitledApplication, error)

	// DirectoryConfigs
	CreateDirectoryConfig(
		name string,
		ouDNs []string, //nolint:revive,staticcheck // existing issue.
		cred ServiceAccountCredentials,
		certAuth CertificateBasedAuthProperties,
	) (*DirectoryConfig, error)

	DeleteDirectoryConfig(name string) error
	DescribeDirectoryConfigs(names []string) ([]*DirectoryConfig, error)
	UpdateDirectoryConfig(
		name string,
		ouDNs []string, //nolint:revive,staticcheck // existing issue.
		cred ServiceAccountCredentials,
		certAuth CertificateBasedAuthProperties,
	) (*DirectoryConfig, error)

	// Images
	CopyImage(sourceName, destName, destRegion, description string) (*Image, error)
	CreateImportedImage(name, description string, tags map[string]string) (*Image, error)
	CreateUpdatedImage(imageName, newImageName, description string) (*Image, error)
	DeleteImage(name string) (*Image, error)
	DescribeImages(names []string, visibilityType string) ([]*Image, error)
	UpdateImagePermissions(imageName, accountID string, allowFleet, allowImageBuilder bool) error
	DeleteImagePermissions(imageName, accountID string) error
	DescribeImagePermissions(imageName string, sharedAwsAccountIDs []string) ([]*SharedImagePermissions, error)

	// ImageBuilders
	CreateImageBuilder(name, description, platform, instanceType string,
		opts CreateImageBuilderOptions) (*ImageBuilder, error)
	DeleteImageBuilder(name string) (*ImageBuilder, error)
	DescribeImageBuilders(names []string) ([]*ImageBuilder, error)
	StartImageBuilder(name, appstreamAgentVersion string) error
	StopImageBuilder(name string) (*ImageBuilder, error)
	CreateImageBuilderStreamingURL(name string, validitySeconds int64) (string, time.Time, error)

	// Software associations
	AssociateSoftwareToImageBuilder(imageBuilderName string, software []string) error
	DisassociateSoftwareFromImageBuilder(imageBuilderName string, software []string) error
	DescribeSoftwareAssociations(resource string) ([]SoftwareAssociation, error)
	StartSoftwareDeploymentToImageBuilder(imageBuilderName string) error

	// ExportImageTasks
	CreateExportImageTask(
		imageName, amiName, amiDescription, iamRoleArn string,
		tagSpecifications map[string]string,
	) (*ExportImageTask, error)
	GetExportImageTask(taskID string) (*ExportImageTask, error)
	ListExportImageTasks(maxResults int32, nextToken string) ([]*ExportImageTask, string, error)

	// UsageReportSubscriptions
	CreateUsageReportSubscription() (*UsageReportSubscription, error)
	DeleteUsageReportSubscription() error
	DescribeUsageReportSubscriptions() ([]*UsageReportSubscription, error)

	// Themes
	CreateThemeForStack(
		stackName string,
		faviconS3Location, organizationLogoS3Location S3Location,
		themeStyling, titleText string,
		footerLinks []ThemeFooterLink,
	) (*Theme, error)
	DeleteThemeForStack(stackName string) error
	DescribeThemeForStack(stackName string) (*Theme, error)
	UpdateThemeForStack(stackName string, opts ThemeUpdateOptions) (*Theme, error)

	// Users
	CreateUser(userName, firstName, lastName, authType string) (*User, error)
	DeleteUser(userName, authType string) error
	DescribeUsers(authType string) ([]*User, error)
	DisableUser(userName, authType string) error
	EnableUser(userName, authType string) error

	// UserStack associations
	BatchAssociateUserStack(associations []UserStackAssociation) ([]UserStackAssociationError, error)
	BatchDisassociateUserStack(associations []UserStackAssociation) ([]UserStackAssociationError, error)
	DescribeUserStackAssociations(stackName, userName, authType string) ([]*UserStackAssociation, error)

	// Sessions
	DescribeSessions(stackName, fleetName, userID, authenticationType string) ([]*Session, error)
	DrainSessionInstance(sessionID string) error
	ExpireSession(sessionID string) error
	CreateStreamingURL(stackName, fleetName, userID string, validitySeconds int64) (string, time.Time, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// UserSetting mirrors appstream@v1.64.5 types.UserSetting: whether a
// streaming-session action (clipboard, file transfer, printing, ...) is
// enabled for a stack's users.
type UserSetting struct {
	Action     string
	Permission string
	// MaximumLength is 0 when unset (real AWS only accepts it for the two
	// clipboard actions).
	MaximumLength int
}

// ApplicationSettings mirrors appstream@v1.64.5
// types.ApplicationSettingsResponse: persistent application settings for a
// stack's streaming sessions. S3BucketName is derived, not stored -- real
// AWS creates one S3 bucket per account+Region the first time persistent
// application settings are enabled (doc comment on
// types.ApplicationSettingsResponse.S3BucketName), mirroring the
// UsageReportSubscription.S3BucketName naming convention already used by
// this backend.
type ApplicationSettings struct {
	SettingsGroup string
	S3BucketName  string
	Enabled       bool
}

// AccessEndpoint mirrors appstream@v1.64.5 types.AccessEndpoint: an
// interface VPC endpoint through which a stack or image builder may be
// accessed.
type AccessEndpoint struct {
	EndpointType string
	VpceID       string
}

// StorageConnector mirrors appstream@v1.64.5 types.StorageConnector: a
// configured storage integration (home folders, Google Drive, OneDrive) for
// a stack.
type StorageConnector struct {
	ConnectorType              string
	ResourceIdentifier         string
	Domains                    []string
	DomainsRequireAdminConsent []string
}

// StreamingExperienceSettings mirrors appstream@v1.64.5
// types.StreamingExperienceSettings: the preferred streaming protocol for a
// stack.
type StreamingExperienceSettings struct {
	PreferredProtocol string
}

// UrlRedirectionConfig mirrors appstream@v1.64.5 types.UrlRedirectionConfig:
// bidirectional URL redirection rules between the streaming session and the
// local client.
//
//nolint:revive,staticcheck // matches real SDK type/field name (Url not URL).
type UrlRedirectionConfig struct {
	Enabled     *bool
	AllowedUrls []string
	DeniedUrls  []string
}

// ContentRedirection mirrors appstream@v1.64.5 types.ContentRedirection:
// content-redirection configuration for a stack's streaming sessions.
type ContentRedirection struct {
	HostToClient *UrlRedirectionConfig
}

// Stack holds AppStream 2.0 stack details.
type Stack struct {
	ApplicationSettings         *ApplicationSettings
	ContentRedirection          *ContentRedirection
	StreamingExperienceSettings *StreamingExperienceSettings
	CreatedTime                 time.Time
	Tags                        map[string]string
	Name                        string
	Arn                         string
	DisplayName                 string
	Description                 string
	RedirectURL                 string
	FeedbackURL                 string
	EmbedHostDomains            []string
	UserSettings                []UserSetting
	StorageConnectors           []StorageConnector
	AccessEndpoints             []AccessEndpoint
}

// CreateStackOptions carries CreateStackInput's full member set
// (api_op_CreateStack.go) beyond the identity fields that stay positional on
// CreateStack. AgentAccessConfig (types.AgentAccessConfig) is deliberately
// absent: its nested ScreenResolution/AgentAccessSetting/ScreenImageFormat
// shape is real CreateStackInput surface with an honest source, but wiring
// it through was out of scope for this pass -- it stays a genuine (not
// fabricated) gap, tracked in PARITY.md rather than closed here.
type CreateStackOptions struct {
	ApplicationSettings         *ApplicationSettings
	ContentRedirection          *ContentRedirection
	StreamingExperienceSettings *StreamingExperienceSettings
	Tags                        map[string]string
	RedirectURL                 string
	FeedbackURL                 string
	DisplayName                 string
	Description                 string
	EmbedHostDomains            []string
	UserSettings                []UserSetting
	StorageConnectors           []StorageConnector
	AccessEndpoints             []AccessEndpoint
}

// UpdateStackOptions carries UpdateStackInput's full member set
// (api_op_UpdateStack.go) that this backend wires through (see
// CreateStackOptions for the AgentAccessConfig scope note). Every field
// means "leave unchanged" when nil/zero, matching ThemeUpdateOptions;
// AttributesToDelete (real StackAttribute enum values, types/enums.go) is
// applied after every set field so a delete always wins over a same-request
// set. DeleteStorageConnectors mirrors the real (deprecated in favor of
// AttributesToDelete, but still modeled) UpdateStackInput member.
type UpdateStackOptions struct {
	ApplicationSettings         *ApplicationSettings
	ContentRedirection          *ContentRedirection
	StreamingExperienceSettings *StreamingExperienceSettings
	DeleteStorageConnectors     *bool
	RedirectURL                 string
	FeedbackURL                 string
	DisplayName                 string
	Description                 string
	EmbedHostDomains            []string
	UserSettings                []UserSetting
	StorageConnectors           []StorageConnector
	AccessEndpoints             []AccessEndpoint
	AttributesToDelete          []string
}

// DomainJoinInfo mirrors appstream@v1.64.5 types.DomainJoinInfo: the AD
// domain a fleet or image builder joins on launch.
type DomainJoinInfo struct {
	DirectoryName                       string
	OrganizationalUnitDistinguishedName string
}

// VolumeConfig mirrors appstream@v1.64.5 types.VolumeConfig: the root
// volume size (GB) for a fleet or image builder instance.
type VolumeConfig struct {
	VolumeSizeInGb int
}

// Fleet holds AppStream 2.0 fleet details.
type Fleet struct {
	CreatedTime                 time.Time
	DisableIMDSV1               *bool
	RootVolumeConfig            *VolumeConfig
	Tags                        map[string]string
	EnableDefaultInternetAccess *bool
	DomainJoinInfo              DomainJoinInfo
	SessionScriptS3Location     S3Location
	Description                 string
	Platform                    string
	Name                        string
	Arn                         string
	DisplayName                 string
	StreamView                  string
	InstanceType                string
	FleetType                   string
	State                       string
	ImageName                   string
	ImageArn                    string
	IamRoleArn                  string
	VpcConfig                   VpcConfig
	UsbDeviceFilterStrings      []string
	DesiredInstances            int
	MaxUserDurationSecs         int
	DisconnectTimeoutSecs       int
	IdleDisconnectTimeoutSecs   int
	MaxSessionsPerInstance      int
	MaxConcurrentSessions       int
}

// CreateFleetOptions carries CreateFleetInput's full member set
// (api_op_CreateFleet.go) beyond the identity/capacity fields that stay
// positional on CreateFleet, following this file's established
// ThemeUpdateOptions convention for wide option sets.
type CreateFleetOptions struct {
	DisableIMDSV1               *bool
	RootVolumeConfig            *VolumeConfig
	Tags                        map[string]string
	EnableDefaultInternetAccess *bool
	DomainJoinInfo              DomainJoinInfo
	SessionScriptS3Location     S3Location
	InstanceType                string
	StreamView                  string
	DisplayName                 string
	Description                 string
	Platform                    string
	FleetType                   string
	ImageName                   string
	ImageArn                    string
	IamRoleArn                  string
	VpcConfig                   VpcConfig
	UsbDeviceFilterStrings      []string
	DesiredInstances            int
	MaxUserDurationSecs         int
	DisconnectTimeoutSecs       int
	IdleDisconnectTimeoutSecs   int
	MaxSessionsPerInstance      int
	MaxConcurrentSessions       int
}

// UpdateFleetOptions carries UpdateFleetInput's full member set
// (api_op_UpdateFleet.go). Every field means "leave unchanged" when
// zero/empty, matching this file's ThemeUpdateOptions convention, except
// AttributesToDelete (real FleetAttribute enum values, types/enums.go),
// which is applied after every set field so a delete always wins over a
// same-request set.
type UpdateFleetOptions struct {
	DisableIMDSV1               *bool
	RootVolumeConfig            *VolumeConfig
	EnableDefaultInternetAccess *bool
	DomainJoinInfo              DomainJoinInfo
	SessionScriptS3Location     S3Location
	InstanceType                string
	IamRoleArn                  string
	Platform                    string
	DisplayName                 string
	Description                 string
	StreamView                  string
	ImageName                   string
	ImageArn                    string
	VpcConfig                   VpcConfig
	UsbDeviceFilterStrings      []string
	AttributesToDelete          []string
	DesiredInstances            int
	MaxUserDurationSecs         int
	DisconnectTimeoutSecs       int
	IdleDisconnectTimeoutSecs   int
	MaxSessionsPerInstance      int
	MaxConcurrentSessions       int
}

// AppBlock holds AppStream 2.0 app block details.
type AppBlock struct {
	SetupScriptDetails     *ScriptDetails
	PostSetupScriptDetails *ScriptDetails
	CreatedTime            time.Time
	Tags                   map[string]string
	SourceS3Location       S3Location
	Name                   string
	Arn                    string
	Description            string
	DisplayName            string
	PackagingType          string
	State                  string
}

// CreateAppBlockOptions carries CreateAppBlockInput's full member set
// (api_op_CreateAppBlock.go) beyond the identity fields that stay positional
// on CreateAppBlock.
type CreateAppBlockOptions struct {
	SetupScriptDetails     *ScriptDetails
	PostSetupScriptDetails *ScriptDetails
	Tags                   map[string]string
	SourceS3Location       S3Location
	DisplayName            string
	PackagingType          string
}

// AppBlockBuilder holds AppStream 2.0 app block builder details.
type AppBlockBuilder struct {
	CreatedTime  time.Time
	Tags         map[string]string
	Name         string
	Arn          string
	Description  string
	Platform     string
	InstanceType string
	State        string
	VpcConfig    VpcConfig
}

// VpcConfig mirrors appstream@v1.64.5 types.VpcConfig: the VPC subnets and
// security groups an app block builder (or fleet/image builder) runs in.
type VpcConfig struct {
	SecurityGroupIDs []string
	SubnetIDs        []string
}

// AppBlockBuilderAppBlockAssociation represents an AppBlockBuilder-AppBlock link.
type AppBlockBuilderAppBlockAssociation struct {
	AppBlockBuilderName string
	AppBlockArn         string
	State               string
}

// S3Location mirrors appstream@v1.64.5 types.S3Location: an S3 bucket/key
// pair. S3Key is only conditionally required depending on which field it's
// used for (types/types.go:1434-1451) -- for IconS3Location on
// CreateApplication and UpdateApplication, both members are required.
type S3Location struct {
	S3Bucket string
	S3Key    string
}

// ScriptDetails mirrors appstream@v1.64.5 types.ScriptDetails: the setup or
// post-setup script for a CUSTOM/APPSTREAM2 packaging-type app block.
type ScriptDetails struct {
	ScriptS3Location     S3Location
	ExecutablePath       string
	ExecutableParameters string
	TimeoutInSeconds     int
}

// Application holds AppStream 2.0 application details.
//
// Enabled is always true: real AWS's doc comment on types.Application.Enabled
// says an application "can be disabled after image creation", but this
// backend has no image-creation pipeline that ever disables one, so every
// application it models is genuinely, unconditionally enabled.
type Application struct {
	CreatedTime      time.Time
	Tags             map[string]string
	Name             string
	Arn              string
	DisplayName      string
	Description      string
	LaunchPath       string
	AppBlockArn      string
	LaunchParameters string
	WorkingDirectory string
	Platforms        []string
	IconS3Location   S3Location
	InstanceFamilies []string
	Enabled          bool
}

// ApplicationFleetAssociation represents an Application-Fleet link.
type ApplicationFleetAssociation struct {
	ApplicationArn string
	FleetName      string
	State          string
}

// EntitlementAttribute is a name-value pair used for entitlement matching.
type EntitlementAttribute struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// Entitlement controls application access based on user attributes.
type Entitlement struct {
	CreatedTime    time.Time
	LastModifiedAt time.Time
	Name           string
	StackName      string
	Description    string
	AppVisibility  string
	Attributes     []EntitlementAttribute
}

// EntitledApplication is an application visible to an entitled user.
type EntitledApplication struct {
	ApplicationIdentifier string
}

// ServiceAccountCredentials are the AD service-account credentials a fleet or
// image builder uses to join a directory.
type ServiceAccountCredentials struct {
	AccountName     string
	AccountPassword string
}

// CertificateBasedAuthProperties configures SAML 2.0 certificate-based
// authentication for a directory config.
type CertificateBasedAuthProperties struct {
	CertificateAuthorityArn string
	Status                  string
}

// DirectoryConfig holds Active Directory connection details.
type DirectoryConfig struct {
	CreatedTime                          time.Time
	ServiceAccountCredentials            ServiceAccountCredentials
	CertificateBasedAuthProperties       CertificateBasedAuthProperties
	DirectoryName                        string
	Arn                                  string
	OrganizationalUnitDistinguishedNames []string
}

// Image holds AppStream 2.0 image details.
type Image struct {
	CreatedTime  time.Time
	Tags         map[string]string
	Name         string
	Arn          string
	Description  string
	Platform     string
	Visibility   string
	State        string
	BaseImageArn string
}

// SharedImagePermissions represents per-account sharing for an image.
type SharedImagePermissions struct {
	ImagePermissions *ImagePermissions
	SharedAccountID  string
}

// ImagePermissions controls how an image may be used.
type ImagePermissions struct {
	AllowFleet        bool
	AllowImageBuilder bool
}

// ImageBuilder holds AppStream 2.0 image builder details.
type ImageBuilder struct {
	CreatedTime                 time.Time
	DisableIMDSV1               *bool
	RootVolumeConfig            *VolumeConfig
	Tags                        map[string]string
	EnableDefaultInternetAccess *bool
	DomainJoinInfo              DomainJoinInfo
	Name                        string
	Arn                         string
	Description                 string
	Platform                    string
	InstanceType                string
	State                       string
	ImageName                   string
	IamRoleArn                  string
	AppstreamAgentVersion       string
	VpcConfig                   VpcConfig
	AccessEndpoints             []AccessEndpoint
}

// CreateImageBuilderOptions carries CreateImageBuilderInput's full member
// set (api_op_CreateImageBuilder.go) beyond the identity fields that stay
// positional on CreateImageBuilder.
type CreateImageBuilderOptions struct {
	EnableDefaultInternetAccess *bool
	DisableIMDSV1               *bool
	RootVolumeConfig            *VolumeConfig
	Tags                        map[string]string
	DomainJoinInfo              DomainJoinInfo
	IamRoleArn                  string
	AppstreamAgentVersion       string
	VpcConfig                   VpcConfig
	AccessEndpoints             []AccessEndpoint
}

// SoftwareAssociation links a software package to an image builder.
type SoftwareAssociation struct {
	ImageBuilderName string
	Software         string
}

// ExportImageTask represents a task exporting a WorkSpaces Applications image
// to an EC2 AMI.
type ExportImageTask struct {
	CreatedDate       time.Time
	TagSpecifications map[string]string
	TaskID            string
	ImageArn          string
	AmiName           string
	AmiDescription    string
	AmiID             string
	State             string
}

// UsageReportSubscription represents an AppStream usage report subscription.
type UsageReportSubscription struct {
	S3BucketName string
	Schedule     string
}

// ThemeFooterLink mirrors appstream@v1.64.5 types.ThemeFooterLink: a link
// displayed in the streaming application catalog page footer.
type ThemeFooterLink struct {
	DisplayName   string
	FooterLinkURL string
}

// Theme holds visual customisation for a stack.
type Theme struct {
	CreatedTime                time.Time
	StackName                  string
	State                      string
	ThemeStyling               string
	ThemeTitleText             string
	ThemeFaviconURL            string
	ThemeOrganizationLogoURL   string
	FaviconS3Location          S3Location
	OrganizationLogoS3Location S3Location
	ThemeFooterLinks           []ThemeFooterLink
}

// ThemeUpdateOptions carries UpdateThemeForStackInput's optional members
// (api_op_UpdateThemeForStack.go:29-64) -- only StackName is required on the
// real wire; every field here means "leave unchanged" when unset. A nil
// FaviconS3Location/OrganizationLogoS3Location/TitleText means the caller did
// not supply it; a nil FooterLinks means unset, a non-nil (possibly empty)
// FooterLinks replaces the list wholesale. AttributesToDelete is applied
// after every set field so a delete always wins over a same-request set
// (matches this repo's rekognition UpdateStreamProcessor convention).
type ThemeUpdateOptions struct {
	FaviconS3Location          *S3Location
	OrganizationLogoS3Location *S3Location
	TitleText                  *string
	FooterLinks                []ThemeFooterLink
	ThemeStyling               string
	State                      string
	AttributesToDelete         []string
}

// User is an AppStream UserPool user.
//
// Real AppStream has no separate Email member on CreateUserInput or
// types.User -- UserName IS the user's email address (aws-sdk-go-v2
// appstream@v1.64.5 api_op_CreateUser.go's UserName doc: "The email address
// of the user").
type User struct {
	CreatedTime        time.Time
	UserName           string
	Arn                string
	FirstName          string
	LastName           string
	AuthenticationType string
	Status             string
	Enabled            bool
}

// UserStackAssociation links a user to a stack.
type UserStackAssociation struct {
	UserName              string
	StackName             string
	AuthenticationType    string
	SendEmailNotification bool
}

// UserStackAssociationError records a failed batch association.
type UserStackAssociationError struct {
	UserStackAssociation *UserStackAssociation
	ErrorCode            string
	ErrorMessage         string
}

// Session represents an active AppStream streaming session.
//
// MaxExpirationTime is derived, not stored: real AWS defines it as
// StartTime plus the owning fleet's MaxUserDurationInSeconds (SDK doc
// comment on types.Session.MaxExpirationTime, appstream@v1.64.5
// types/types.go:1540-1546). It is left zero when the owning fleet can no
// longer be found (deleted mid-session); callers must check IsZero before
// emitting it.
type Session struct {
	StartTime          time.Time
	MaxExpirationTime  time.Time
	ID                 string
	FleetName          string
	StackName          string
	UserID             string
	State              string
	ConnectionState    string
	AuthenticationType string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
