package appconfig

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
