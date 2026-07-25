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
	// UpdateApplication updates an application's name and description. A nil
	// name/description means the field was omitted from the request and must
	// be left unchanged (real UpdateApplicationInput.Name/Description are
	// optional *string members; only a present, non-nil member overwrites
	// the existing value -- see AWS AppConfig's UpdateApplication contract).
	UpdateApplication(applicationID string, name, description *string) (*Application, error)
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
	// UpdateEnvironment updates an environment's name, description, and
	// monitors. A nil name/description leaves the field unchanged (see
	// UpdateApplication doc); a nil monitors leaves the existing monitor
	// list unchanged, while a non-nil (possibly empty) slice replaces it,
	// matching UpdateEnvironmentInput's optional Monitors member.
	UpdateEnvironment(
		applicationID, environmentID string,
		name, description *string,
		monitors *[]Monitor,
	) (*Environment, error)
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
	// UpdateConfigurationProfile updates a configuration profile. Nil
	// name/description/retrievalRoleArn leave the field unchanged; a nil
	// validators leaves the existing validator list unchanged, while a
	// non-nil (possibly empty) slice replaces it -- matching
	// UpdateConfigurationProfileInput's optional members.
	UpdateConfigurationProfile(
		applicationID, profileID string,
		name, description, retrievalRoleArn *string,
		validators *[]Validator,
	) (*ConfigurationProfile, error)
	// DeleteConfigurationProfile deletes a configuration profile.
	DeleteConfigurationProfile(applicationID, profileID string) error

	// CreateHostedConfigurationVersion creates a hosted configuration
	// version. latestVersionNumber implements the optional
	// optimistic-concurrency check real AWS binds to the
	// "Latest-Version-Number" request header: when non-nil, it must match
	// the profile's current latest version or the call is rejected.
	CreateHostedConfigurationVersion(
		applicationID, profileID, contentType, description, versionLabel string,
		content []byte,
		latestVersionNumber *int32,
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
	// UpdateDeploymentStrategy updates a deployment strategy. A nil
	// description leaves the field unchanged (real
	// UpdateDeploymentStrategyInput.Description is an optional *string
	// member); name has no counterpart in the real API and is applied only
	// when non-empty, matching this backend's pre-existing behavior.
	UpdateDeploymentStrategy(
		strategyID, name string,
		description *string,
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
	// StopDeployment stops an in-progress deployment, or -- when
	// allowRevert is true and the deployment is already COMPLETE --
	// reverts the environment to the previous configuration version
	// (real StopDeploymentInput.AllowRevert semantics).
	StopDeployment(applicationID, environmentID string, deploymentNumber int32, allowRevert bool) error

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
	// GetExtension retrieves an extension by identifier (ID or name) and
	// optional version number (0 means unspecified: the highest version).
	GetExtension(extensionIdentifier string, versionNumber int32) (*Extension, error)
	// ListExtensions returns paginated extensions, optionally filtered by name.
	ListExtensions(
		nextToken string,
		maxResults int,
		nameFilter string,
	) ([]Extension, string)
	// UpdateExtension updates an extension's description, actions, and
	// parameters. A nil description leaves the field unchanged (real
	// UpdateExtensionInput.Description is an optional *string member).
	UpdateExtension(
		extensionIdentifier string,
		description *string,
		actions map[string][]ExtensionAction,
		parameters map[string]ExtensionParameter,
	) (*Extension, error)
	// DeleteExtension deletes an extension version by identifier (ID or
	// name) and optional version number (0 means unspecified: the highest
	// version).
	DeleteExtension(extensionIdentifier string, versionNumber int32) error

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

	// CurrentDeployedConfiguration returns the content, content type, and
	// version label of the configuration currently active (the most
	// recently COMPLETEd deployment) for the given
	// application/environment/configuration-profile, each resolved by ID
	// or name exactly like GetConfiguration. It has no caller within this
	// package today -- it is a public read accessor exposed for a future
	// appconfig -> appconfigdata bridge (bd gopherstack-uiyi): once a
	// deployment completes, cli.go wiring (out of scope for this change)
	// can call this and push the result into
	// appconfigdata's SetConfiguration(app, env, profile, content,
	// contentType) so GetLatestConfiguration polling reflects real
	// deployment state instead of an unpopulated store.
	CurrentDeployedConfiguration(
		application, environment, configuration string,
	) (content []byte, contentType, versionLabel string, err error)
}
