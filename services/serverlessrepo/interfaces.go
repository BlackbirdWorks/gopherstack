package serverlessrepo

// StorageBackend is the interface for the Serverless Application Repository in-memory backend.
type StorageBackend interface {
	// Application CRUD
	CreateApplication(
		name, description, author, sourceCodeURL, semanticVersion string,
		tags map[string]string,
	) (*Application, error)
	GetApplication(name string) (*Application, error)
	ListApplications() []*Application
	UpdateApplication(name, description, author string) (*Application, error)
	DeleteApplication(name string) error

	// Application version operations
	CreateApplicationVersion(appName, semanticVersion, sourceCodeURL, templateURL string) (*ApplicationVersion, error)
	ListApplicationVersions(appName string) ([]*ApplicationVersion, error)

	// CloudFormation template operations
	CreateCloudFormationTemplate(appName, semanticVersion string) (*CloudFormationTemplate, error)
	GetCloudFormationTemplate(appName, templateID string) (*CloudFormationTemplate, error)

	// CloudFormation change set operations
	CreateCloudFormationChangeSet(
		appName, stackName, semanticVersion, templateID string,
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
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
