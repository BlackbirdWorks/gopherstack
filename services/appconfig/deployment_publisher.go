package appconfig

import "context"

// DeployedConfigurationPublisher lets the AppConfig backend push a
// deployment's configuration into AppConfigData once it reaches COMPLETE, so
// GetLatestConfiguration polling reflects real deployment state instead of
// AppConfigData's store sitting unpopulated (bd gopherstack-uiyi). When
// unset (the default), deployments complete exactly as before this bridge
// existed. appconfigdata.InMemoryBackend satisfies this interface directly
// via its PublishConfiguration method -- no adapter needed, same as
// cloudwatch's FirehosePutter/firehose.InMemoryBackend pairing.
type DeployedConfigurationPublisher interface {
	PublishConfiguration(applicationID, environmentID, profileID, content, contentType, deploymentID string) error
}

// SetDeployedConfigurationPublisher wires a DeployedConfigurationPublisher so
// completed deployments push their configuration to AppConfigData. Passing
// nil restores the historical, publish-less behavior. Intended to be called
// once during service wiring, before the backend serves traffic.
func (b *InMemoryBackend) SetDeployedConfigurationPublisher(p DeployedConfigurationPublisher) {
	b.mu.Lock("SetDeployedConfigurationPublisher")
	defer b.mu.Unlock()
	b.configPublisher = p
}

// ConfigurationContentReader retrieves the content a non-hosted configuration profile points at
// (ssm-parameter://, ssm-document://, s3://, secretsmanager://) at the given version.
type ConfigurationContentReader interface {
	ReadConfiguration(ctx context.Context, locationURI, retrievalRoleARN, version string) ([]byte, string, error)
}

// SetConfigurationContentReader wires the reader for non-hosted profiles.
func (b *InMemoryBackend) SetConfigurationContentReader(r ConfigurationContentReader) {
	b.mu.Lock("SetConfigurationContentReader")
	defer b.mu.Unlock()
	b.contentReader = r
}
