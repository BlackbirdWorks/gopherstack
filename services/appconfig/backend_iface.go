package appconfig

// StorageBackend defines the operations supported by the AppConfig in-memory backend.
type StorageBackend interface {
	// CreateApplication creates a new AppConfig application.
	CreateApplication(name, description string) (*Application, error)
	// GetApplication retrieves an application by ID.
	GetApplication(applicationID string) (*Application, error)
	// ListApplications returns all applications.
	ListApplications() []Application
	// UpdateApplication updates an application's name and description.
	UpdateApplication(applicationID, name, description string) (*Application, error)
	// DeleteApplication deletes an application by ID.
	DeleteApplication(applicationID string) error

	// CreateEnvironment creates a new environment within an application.
	CreateEnvironment(applicationID, name, description string) (*Environment, error)
	// GetEnvironment retrieves an environment by application and environment ID.
	GetEnvironment(applicationID, environmentID string) (*Environment, error)
	// ListEnvironments returns all environments for an application.
	ListEnvironments(applicationID string) ([]Environment, error)
	// UpdateEnvironment updates an environment's name and description.
	UpdateEnvironment(applicationID, environmentID, name, description string) (*Environment, error)
	// DeleteEnvironment deletes an environment.
	DeleteEnvironment(applicationID, environmentID string) error

	// CreateConfigurationProfile creates a new configuration profile.
	CreateConfigurationProfile(
		applicationID, name, description, locationURI, profileType string,
	) (*ConfigurationProfile, error)
	// GetConfigurationProfile retrieves a configuration profile.
	GetConfigurationProfile(applicationID, profileID string) (*ConfigurationProfile, error)
	// ListConfigurationProfiles returns all profiles for an application.
	ListConfigurationProfiles(applicationID string) ([]ConfigurationProfile, error)
	// UpdateConfigurationProfile updates a configuration profile.
	UpdateConfigurationProfile(applicationID, profileID, name, description string) (*ConfigurationProfile, error)
	// DeleteConfigurationProfile deletes a configuration profile.
	DeleteConfigurationProfile(applicationID, profileID string) error

	// CreateHostedConfigurationVersion creates a hosted configuration version.
	CreateHostedConfigurationVersion(
		applicationID, profileID, contentType string,
		content []byte,
	) (*HostedConfigurationVersion, error)
	// GetHostedConfigurationVersion retrieves a hosted configuration version.
	GetHostedConfigurationVersion(
		applicationID, profileID string,
		versionNumber int32,
	) (*HostedConfigurationVersion, error)
	// ListHostedConfigurationVersions returns all versions for a profile.
	ListHostedConfigurationVersions(applicationID, profileID string) ([]HostedConfigurationVersion, error)
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
	// ListDeploymentStrategies returns all deployment strategies.
	ListDeploymentStrategies() []DeploymentStrategy
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
		applicationID, environmentID, configProfileID, strategyID, configVersion string,
	) (*Deployment, error)
	// GetDeployment retrieves a deployment by application, environment, and deployment number.
	GetDeployment(applicationID, environmentID string, deploymentNumber int32) (*Deployment, error)
	// ListDeployments returns all deployments for an environment.
	ListDeployments(applicationID, environmentID string) ([]Deployment, error)
	// StopDeployment stops an in-progress deployment.
	StopDeployment(applicationID, environmentID string, deploymentNumber int32) error
}
