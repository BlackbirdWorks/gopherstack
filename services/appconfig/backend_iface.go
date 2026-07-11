package appconfig

import "context"

// StorageBackend defines the operations supported by the AppConfig in-memory backend.
type StorageBackend interface {
	// PaginationSecret returns the HMAC secret used to sign pagination tokens.
	PaginationSecret() string

	// Snapshot and Restore implement persistence.Persistable. Handler
	// delegates to them (see persistence.go) so cli.go's generic
	// setupPersistence picks AppConfig up.
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error

	// CreateApplication creates a new AppConfig application.
	CreateApplication(name, description string) (*Application, error)
	// GetApplication retrieves an application by ID.
	GetApplication(applicationID string) (*Application, error)
	// ListApplications returns paginated applications.
	ListApplications(nextToken string, maxResults int) ([]Application, string)
	// UpdateApplication updates an application's name and description.
	UpdateApplication(applicationID, name, description string) (*Application, error)
	// DeleteApplication deletes an application by ID.
	DeleteApplication(applicationID string) error

	// CreateEnvironment creates a new environment within an application.
	CreateEnvironment(
		applicationID, name, description string,
		monitors []Monitor,
	) (*Environment, error)
	// GetEnvironment retrieves an environment by application and environment ID.
	GetEnvironment(applicationID, environmentID string) (*Environment, error)
	// ListEnvironments returns paginated environments for an application.
	ListEnvironments(applicationID, nextToken string, maxResults int) ([]Environment, string, error)
	// UpdateEnvironment updates an environment's name and description.
	UpdateEnvironment(applicationID, environmentID, name, description string) (*Environment, error)
	// DeleteEnvironment deletes an environment.
	DeleteEnvironment(applicationID, environmentID string) error

	// CreateConfigurationProfile creates a new configuration profile.
	CreateConfigurationProfile(
		applicationID, name, description, locationURI, profileType, retrievalRoleArn string,
		validators []Validator,
	) (*ConfigurationProfile, error)
	// GetConfigurationProfile retrieves a configuration profile.
	GetConfigurationProfile(applicationID, profileID string) (*ConfigurationProfile, error)
	// ListConfigurationProfiles returns paginated profiles for an application.
	ListConfigurationProfiles(
		applicationID, nextToken string,
		maxResults int,
	) ([]ConfigurationProfile, string, error)
	// UpdateConfigurationProfile updates a configuration profile.
	UpdateConfigurationProfile(
		applicationID, profileID, name, description string,
	) (*ConfigurationProfile, error)
	// DeleteConfigurationProfile deletes a configuration profile.
	DeleteConfigurationProfile(applicationID, profileID string) error

	// CreateHostedConfigurationVersion creates a hosted configuration version.
	CreateHostedConfigurationVersion(
		applicationID, profileID, contentType, description, versionLabel string,
		content []byte,
	) (*HostedConfigurationVersion, error)
	// GetHostedConfigurationVersion retrieves a hosted configuration version.
	GetHostedConfigurationVersion(
		applicationID, profileID string,
		versionNumber int32,
	) (*HostedConfigurationVersion, error)
	// ListHostedConfigurationVersions returns paginated versions for a profile.
	ListHostedConfigurationVersions(
		applicationID, profileID, nextToken, versionLabel string,
		maxResults int,
	) ([]HostedConfigurationVersion, string, error)
	// DeleteHostedConfigurationVersion deletes a hosted configuration version.
	DeleteHostedConfigurationVersion(applicationID, profileID string, versionNumber int32) error

	// CreateDeploymentStrategy creates a new deployment strategy.
	CreateDeploymentStrategy(
		name, description string,
		deploymentDuration, bakeTime int32,
		growthFactor float32,
		growthType, replicateTo string,
	) (*DeploymentStrategy, error)
	// GetDeploymentStrategy retrieves a deployment strategy by ID.
	GetDeploymentStrategy(strategyID string) (*DeploymentStrategy, error)
	// ListDeploymentStrategies returns paginated deployment strategies.
	ListDeploymentStrategies(nextToken string, maxResults int) ([]DeploymentStrategy, string)
	// UpdateDeploymentStrategy updates a deployment strategy.
	UpdateDeploymentStrategy(
		strategyID, name, description string,
		deploymentDuration, bakeTime int32,
		growthFactor float32,
	) (*DeploymentStrategy, error)
	// DeleteDeploymentStrategy deletes a deployment strategy.
	DeleteDeploymentStrategy(strategyID string) error

	// StartDeployment starts a deployment.
	StartDeployment(
		applicationID, environmentID, configProfileID, strategyID, configVersion, description string,
	) (*Deployment, error)
	// GetDeployment retrieves a deployment by application, environment, and deployment number.
	GetDeployment(applicationID, environmentID string, deploymentNumber int32) (*Deployment, error)
	// ListDeployments returns paginated deployments for an environment.
	ListDeployments(
		applicationID, environmentID, nextToken string,
		maxResults int,
	) ([]Deployment, string, error)
	// StopDeployment stops an in-progress deployment.
	StopDeployment(applicationID, environmentID string, deploymentNumber int32) error

	// ListTagsForResource returns the tags for a resource by ARN.
	ListTagsForResource(resourceArn string) (map[string]string, error)
	// TagResource adds or updates tags on a resource.
	TagResource(resourceArn string, tags map[string]string) error
	// UntagResource removes tags from a resource.
	UntagResource(resourceArn string, tagKeys []string) error

	// CreateExtension creates a new AppConfig extension.
	CreateExtension(
		name, description string,
		actions map[string][]ExtensionAction,
		parameters map[string]ExtensionParameter,
	) (*Extension, error)
	// GetExtension retrieves an extension by identifier (ID or name).
	GetExtension(extensionIdentifier string) (*Extension, error)
	// ListExtensions returns paginated extensions, optionally filtered by name and/or version.
	ListExtensions(
		nextToken string,
		maxResults int,
		nameFilter string,
		versionNumber int32,
	) ([]Extension, string)
	// UpdateExtension updates an extension's description, actions, and parameters.
	UpdateExtension(
		extensionIdentifier, description string,
		actions map[string][]ExtensionAction,
		parameters map[string]ExtensionParameter,
	) (*Extension, error)
	// DeleteExtension deletes an extension by identifier (ID or name).
	DeleteExtension(extensionIdentifier string) error

	// CreateExtensionAssociation creates an association between an extension and a resource.
	CreateExtensionAssociation(
		extensionIdentifier, resourceIdentifier string,
		parameters map[string]string,
		extensionVersionNumber *int32,
	) (*ExtensionAssociation, error)
	// GetExtensionAssociation retrieves an extension association by ID.
	GetExtensionAssociation(extensionAssociationID string) (*ExtensionAssociation, error)
	// ListExtensionAssociations returns paginated extension associations.
	ListExtensionAssociations(
		nextToken, extensionIdentifier, resourceIdentifier string,
		maxResults int,
	) ([]ExtensionAssociation, string)
	// UpdateExtensionAssociation updates an extension association's parameters.
	UpdateExtensionAssociation(
		extensionAssociationID string,
		parameters map[string]string,
	) (*ExtensionAssociation, error)
	// DeleteExtensionAssociation deletes an extension association by ID.
	DeleteExtensionAssociation(extensionAssociationID string) error

	// GetAccountSettings returns the account-level AppConfig settings.
	GetAccountSettings() (*AccountSettings, error)
	// UpdateAccountSettings updates account-level AppConfig settings.
	UpdateAccountSettings(deletionProtection *DeletionProtectionSettings) (*AccountSettings, error)

	// GetConfiguration retrieves the latest deployed configuration (deprecated API).
	GetConfiguration(
		application, environment, configuration string,
	) (*HostedConfigurationVersion, error)
	// ValidateConfiguration validates a configuration version against its validators.
	ValidateConfiguration(applicationID, profileID, configurationVersion string) error
}
