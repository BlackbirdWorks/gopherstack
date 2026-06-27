package appstream

import (
	"context"
	"time"
)

// StorageBackend is the interface for AppStream 2.0 storage operations.
type StorageBackend interface {
	// Stacks
	CreateStack(name, displayName, description string, tags map[string]string) (*Stack, error)
	DescribeStacks(names []string) ([]*Stack, error)
	UpdateStack(name, displayName, description string) (*Stack, error)
	DeleteStack(name string) error

	// Fleets
	CreateFleet(name, displayName, description, instanceType, fleetType, imageName, imageArn string,
		desiredInstances, maxUserDuration, disconnectTimeout, idleDisconnectTimeout int,
		enableDefaultInternetAccess *bool, tags map[string]string) (*Fleet, error)
	DescribeFleets(names []string) ([]*Fleet, error)
	UpdateFleet(name, displayName, description, instanceType, imageName, imageArn string,
		desiredInstances, maxUserDuration, disconnectTimeout, idleDisconnectTimeout int,
		enableDefaultInternetAccess *bool) (*Fleet, error)
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
	CreateAppBlock(name, description string, tags map[string]string) (*AppBlock, error)
	DeleteAppBlock(name string) error
	DescribeAppBlocks(names []string) ([]*AppBlock, error)

	// AppBlockBuilders
	CreateAppBlockBuilder(
		name, description, platform, instanceType string,
		tags map[string]string,
	) (*AppBlockBuilder, error)
	DeleteAppBlockBuilder(name string) error
	DescribeAppBlockBuilders(names []string) ([]*AppBlockBuilder, error)
	StartAppBlockBuilder(name string) error
	StopAppBlockBuilder(name string) error
	UpdateAppBlockBuilder(name, description, instanceType string) (*AppBlockBuilder, error)
	CreateAppBlockBuilderStreamingURL(name string) (string, error)

	// AppBlockBuilder-AppBlock associations
	AssociateAppBlockBuilderAppBlock(builderName, appBlockName string) error
	DisassociateAppBlockBuilderAppBlock(builderName, appBlockName string) error
	DescribeAppBlockBuilderAppBlockAssociations(
		builderName, appBlockName string,
	) ([]*AppBlockBuilderAppBlockAssociation, error)

	// Applications
	CreateApplication(name, displayName, description, launchPath, appBlockArn string,
		platforms []string, tags map[string]string) (*Application, error)
	DeleteApplication(name string) error
	DescribeApplications(names []string) ([]*Application, error)
	UpdateApplication(name, displayName, description, launchPath string) (*Application, error)
	DescribeAppLicenseUsage() ([]map[string]string, error)

	// Application-Fleet associations
	AssociateApplicationFleet(appName, fleetName string) error
	DisassociateApplicationFleet(appName, fleetName string) error
	DescribeApplicationFleetAssociations(appName, fleetName string) ([]*ApplicationFleetAssociation, error)

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
	) (*DirectoryConfig, error)

	DeleteDirectoryConfig(name string) error
	DescribeDirectoryConfigs(names []string) ([]*DirectoryConfig, error)
	UpdateDirectoryConfig(
		name string,
		ouDNs []string, //nolint:revive,staticcheck // existing issue.
	) (*DirectoryConfig, error)

	// Images
	CopyImage(sourceName, destName, destRegion, description string) (*Image, error)
	CreateImportedImage(name, description string, tags map[string]string) (*Image, error)
	CreateUpdatedImage(imageName, newImageName, description string) (*Image, error)
	DeleteImage(name string) error
	DescribeImages(names []string) ([]*Image, error)
	UpdateImagePermissions(imageName, accountID string, allowFleet, allowImageBuilder bool) error
	DeleteImagePermissions(imageName, accountID string) error
	DescribeImagePermissions(imageName string) ([]*SharedImagePermissions, error)

	// ImageBuilders
	CreateImageBuilder(name, description, platform, instanceType string, tags map[string]string) (*ImageBuilder, error)
	DeleteImageBuilder(name string) (string, error)
	DescribeImageBuilders(names []string) ([]*ImageBuilder, error)
	StartImageBuilder(name, appstreamAgentVersion string) (string, error)
	StopImageBuilder(name string) (*ImageBuilder, error)
	CreateImageBuilderStreamingURL(name string) (string, error)

	// Software associations
	AssociateSoftwareToImageBuilder(imageBuilderName string, software []string) error
	DisassociateSoftwareFromImageBuilder(imageBuilderName string, software []string) error
	DescribeSoftwareAssociations(imageBuilderName string) ([]SoftwareAssociation, error)
	StartSoftwareDeploymentToImageBuilder(imageBuilderName string) error

	// ExportImageTasks
	CreateExportImageTask(imageName, s3Bucket, s3Prefix string) (*ExportImageTask, error)
	GetExportImageTask(taskID string) (*ExportImageTask, error)
	ListExportImageTasks(imageNames []string) ([]*ExportImageTask, error)

	// UsageReportSubscriptions
	CreateUsageReportSubscription(schedule, s3Bucket string) (*UsageReportSubscription, error)
	DeleteUsageReportSubscription() error
	DescribeUsageReportSubscriptions() ([]*UsageReportSubscription, error)

	// Themes
	CreateThemeForStack(stackName string) (*Theme, error)
	DeleteThemeForStack(stackName string) error
	DescribeThemeForStack(stackName string) (*Theme, error)
	UpdateThemeForStack(stackName string) (*Theme, error)

	// Users
	CreateUser(userName, email, firstName, lastName, authType string) (*User, error)
	DeleteUser(userName, authType string) error
	DescribeUsers(authType string) ([]*User, error)
	DisableUser(userName, authType string) error
	EnableUser(userName, authType string) error

	// UserStack associations
	BatchAssociateUserStack(associations []UserStackAssociation) ([]UserStackAssociationError, error)
	BatchDisassociateUserStack(associations []UserStackAssociation) ([]UserStackAssociationError, error)
	DescribeUserStackAssociations(stackName, userName, authType string) ([]*UserStackAssociation, error)

	// Sessions
	DescribeSessions(stackName, fleetName, userID string) ([]*Session, error)
	DrainSessionInstance(sessionID string) error
	ExpireSession(sessionID string) error
	CreateStreamingURL(stackName, fleetName, userID string) (string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Stack holds AppStream 2.0 stack details.
type Stack struct {
	CreatedTime time.Time
	Tags        map[string]string
	Name        string
	Arn         string
	DisplayName string
	Description string
}

// Fleet holds AppStream 2.0 fleet details.
type Fleet struct {
	EnableDefaultInternetAccess *bool
	CreatedTime                 time.Time
	Tags                        map[string]string
	Name                        string
	Arn                         string
	DisplayName                 string
	Description                 string
	InstanceType                string
	FleetType                   string
	State                       string
	ImageName                   string
	ImageArn                    string
	DesiredInstances            int
	MaxUserDurationSecs         int
	DisconnectTimeoutSecs       int
	IdleDisconnectTimeoutSecs   int
}

// AppBlock holds AppStream 2.0 app block details.
type AppBlock struct {
	CreatedTime time.Time
	Tags        map[string]string
	Name        string
	Arn         string
	Description string
	State       string
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
}

// AppBlockBuilderAppBlockAssociation represents an AppBlockBuilder-AppBlock link.
type AppBlockBuilderAppBlockAssociation struct {
	AppBlockBuilderName string
	AppBlockArn         string
	State               string
}

// Application holds AppStream 2.0 application details.
type Application struct {
	CreatedTime time.Time
	Tags        map[string]string
	Name        string
	Arn         string
	DisplayName string
	Description string
	LaunchPath  string
	AppBlockArn string
	Platforms   []string
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

// DirectoryConfig holds Active Directory connection details.
type DirectoryConfig struct {
	CreatedTime                          time.Time
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
	CreatedTime  time.Time
	Tags         map[string]string
	Name         string
	Arn          string
	Description  string
	Platform     string
	InstanceType string
	State        string
	ImageName    string
}

// SoftwareAssociation links a software package to an image builder.
type SoftwareAssociation struct {
	ImageBuilderName string
	Software         string
}

// ExportImageTask represents an image export operation to S3.
type ExportImageTask struct {
	CreatedAt time.Time
	TaskID    string
	ImageName string
	S3Bucket  string
	S3Key     string
	Status    string
}

// UsageReportSubscription represents an AppStream usage report subscription.
type UsageReportSubscription struct {
	S3BucketName string
	Schedule     string
}

// Theme holds visual customisation for a stack.
type Theme struct {
	CreatedTime time.Time
	StackName   string
	State       string
}

// User is an AppStream UserPool user.
type User struct {
	CreatedTime        time.Time
	UserName           string
	Arn                string
	Email              string
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
type Session struct {
	StartTime          time.Time
	ID                 string
	FleetName          string
	StackName          string
	UserID             string
	State              string
	ConnectionState    string
	AuthenticationType string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
