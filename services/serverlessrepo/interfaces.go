package serverlessrepo

import "context"

// StorageBackend is the interface for the Serverless Application Repository in-memory backend.
type StorageBackend interface {
	// Application CRUD
	CreateApplication(
		name, description, author, sourceCodeURL, semanticVersion string,
		labels []string,
		homePageURL, licenseURL, spdxLicenseID string,
	) (*Application, error)
	SetApplicationReadmeURL(name, readmeURL string) (*Application, error)
	GetApplication(name string) (*Application, error)
	ListApplications() []*Application
	UpdateApplication(name, description, author, homePageURL, readmeURL string) (*Application, error)
	UpdateApplicationLabels(name string, labels []string) (*Application, error)
	DeleteApplication(name string) error

	// Application version operations
	CreateApplicationVersion(appName, semanticVersion, sourceCodeURL, templateURL string) (*ApplicationVersion, error)
	CreateApplicationVersionWithOptions(
		appName, semanticVersion string,
		opts CreateApplicationVersionOptions,
	) (*ApplicationVersion, error)
	GetApplicationVersion(appName, semanticVersion string) (*ApplicationVersion, error)
	ListApplicationVersions(appName string) ([]*ApplicationVersion, error)

	// CloudFormation template operations
	CreateCloudFormationTemplate(appName, semanticVersion string) (*CloudFormationTemplate, error)
	GetCloudFormationTemplate(appName, templateID string) (*CloudFormationTemplate, error)

	// CloudFormation change set operations
	CreateCloudFormationChangeSet(
		appName, stackName, changeSetName, semanticVersion string,
	) (*CloudFormationChangeSet, error)
	CreateCloudFormationChangeSetWithOptions(
		appName, stackName, changeSetName, semanticVersion string,
		opts CreateCloudFormationChangeSetOptions,
	) (*CloudFormationChangeSet, error)

	// Application policy operations
	GetApplicationPolicy(appName string) ([]*ApplicationPolicyStatement, error)
	PutApplicationPolicy(
		appName string,
		statements []*ApplicationPolicyStatement,
	) ([]*ApplicationPolicyStatement, error)

	// Application dependency operations
	ListApplicationDependencies(appName, semanticVersion string) ([]*ApplicationDependency, error)

	// Unshare application
	UnshareApplication(appName, organizationID string) error

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
