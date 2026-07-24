package appconfig

import (
	"fmt"
	"strings"
)

// ValidateConfiguration validates a configuration version against its validators.
// In this implementation, all well-formed configurations are considered valid.
// The configurationVersion parameter is accepted for API compatibility but not evaluated.
func (b *InMemoryBackend) ValidateConfiguration(applicationID, profileID, _ string) error {
	b.mu.RLock("ValidateConfiguration")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationID) {
		return fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profile, ok := b.configProfiles.Get(profileID)
	if !ok || profile.ApplicationID != applicationID {
		return fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	return nil
}

// GetConfiguration retrieves the latest DEPLOYED configuration for the given
// application, environment, and configuration profile (deprecated API).
func (b *InMemoryBackend) GetConfiguration(
	application, environment, configuration string,
) (*HostedConfigurationVersion, error) {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	appID, envID, profileID, err := b.resolveConfigurationTriple(application, environment, configuration)
	if err != nil {
		return nil, err
	}

	return b.deployedConfigVersionLocked(appID, envID, profileID), nil
}

// CurrentDeployedConfiguration returns the content, content type, and
// version label of the configuration currently active (the most recently
// COMPLETEd deployment) for the given application/environment/configuration
// profile. See the StorageBackend interface doc comment for why this
// exists: it is a public accessor with no caller inside this package today,
// exposed for a future appconfig -> appconfigdata bridge.
func (b *InMemoryBackend) CurrentDeployedConfiguration(
	application, environment, configuration string,
) ([]byte, string, string, error) {
	b.mu.RLock("CurrentDeployedConfiguration")
	defer b.mu.RUnlock()

	appID, envID, profileID, err := b.resolveConfigurationTriple(application, environment, configuration)
	if err != nil {
		return nil, "", "", err
	}

	v := b.deployedConfigVersionLocked(appID, envID, profileID)

	return v.Content, v.ContentType, v.VersionLabel, nil
}

// resolveConfigurationTriple resolves an application/environment/
// configuration-profile identifier triple (each accepted by ID or name) to
// their canonical IDs. Must be called under lock.
func (b *InMemoryBackend) resolveConfigurationTriple(
	application, environment, configuration string,
) (string, string, string, error) {
	appID, err := b.resolveAppID(application)
	if err != nil {
		return "", "", "", err
	}

	envID, err := b.resolveEnvID(appID, environment)
	if err != nil {
		return "", "", "", err
	}

	profileID, err := b.resolveProfileID(appID, configuration)
	if err != nil {
		return "", "", "", err
	}

	return appID, envID, profileID, nil
}

// deleteDeployedConfigsLocked removes every deployedConfigs entry whose
// (applicationID, environmentID, profileID) key satisfies match, so
// DeleteApplication/DeleteEnvironment/DeleteConfigurationProfile leave no
// ghost rows behind (see the InMemoryBackend doc comment on
// deployedConfigs in store.go). Must be called under lock.
func (b *InMemoryBackend) deleteDeployedConfigsLocked(match func(appID, envID, profileID string) bool) {
	const segments = 3 // appID|envID|profileID

	for k := range b.deployedConfigs {
		parts := strings.SplitN(k, "|", segments)
		if len(parts) != segments {
			continue
		}

		if match(parts[0], parts[1], parts[2]) {
			delete(b.deployedConfigs, k)
		}
	}
}

// resolveAppID finds an application ID by ID or name. Must be called under lock.
func (b *InMemoryBackend) resolveAppID(identifier string) (string, error) {
	if b.applications.Has(identifier) {
		return identifier, nil
	}

	if matches := b.applicationsByName.Get(identifier); len(matches) > 0 {
		return matches[0].ID, nil
	}

	return "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, identifier)
}

// resolveEnvID finds an environment ID by ID or name within an application. Must be called under lock.
func (b *InMemoryBackend) resolveEnvID(appID, identifier string) (string, error) {
	if env, ok := b.environments.Get(identifier); ok && env.ApplicationID == appID {
		return identifier, nil
	}

	if matches := b.environmentsByAppName.Get(appNameKey(appID, identifier)); len(matches) > 0 {
		return matches[0].ID, nil
	}

	return "", fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, identifier)
}

// resolveProfileID finds a configuration profile ID by ID or name. Must be called under lock.
func (b *InMemoryBackend) resolveProfileID(appID, identifier string) (string, error) {
	if profile, ok := b.configProfiles.Get(identifier); ok && profile.ApplicationID == appID {
		return identifier, nil
	}

	if matches := b.configProfilesByAppName.Get(appNameKey(appID, identifier)); len(matches) > 0 {
		return matches[0].ID, nil
	}

	return "", fmt.Errorf(
		"%w: configuration profile %s",
		ErrConfigurationProfileNotFound,
		identifier,
	)
}

// deployedConfigVersionLocked returns the hosted configuration version
// currently DEPLOYED (the ConfigurationVersion of the most recent COMPLETE
// deployment recorded in deployedConfigs -- see its doc comment on
// InMemoryBackend in store.go) for an environment/profile pair. Before any
// deployment has ever completed for this environment/profile -- or if the
// deployed version has since been deleted, or the profile is not
// AppConfig-hosted -- real AWS AppConfig returns empty content rather than
// an error, which this mirrors. Must be called under lock.
func (b *InMemoryBackend) deployedConfigVersionLocked(appID, envID, profileID string) *HostedConfigurationVersion {
	empty := &HostedConfigurationVersion{
		ApplicationID:          appID,
		ConfigurationProfileID: profileID,
		ContentType:            contentTypeOctetStream,
		Content:                []byte{},
	}

	configVersion, ok := b.deployedConfigs[appEnvProfileKey(appID, envID, profileID)]
	if !ok {
		return empty
	}

	v, ok := b.resolveHostedConfigVersion(appID, profileID, configVersion)
	if !ok {
		return empty
	}

	cp := *v

	return &cp
}
