package appconfig

import (
	"fmt"
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

// GetConfiguration retrieves the latest deployed configuration for the given application,
// environment, and configuration profile (deprecated API).
func (b *InMemoryBackend) GetConfiguration(
	application, environment, configuration string,
) (*HostedConfigurationVersion, error) {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	appID, err := b.resolveAppID(application)
	if err != nil {
		return nil, err
	}

	if _, err = b.resolveEnvID(appID, environment); err != nil {
		return nil, err
	}

	profileID, err := b.resolveProfileID(appID, configuration)
	if err != nil {
		return nil, err
	}

	return b.latestConfigVersion(appID, profileID), nil
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

// latestConfigVersion returns the latest hosted configuration version for a profile. Must be called under lock.
// It walks version numbers from the counter downward to skip any deleted versions, so the
// common case (no deletes) is O(1) and deletions from the top add at most one step each.
func (b *InMemoryBackend) latestConfigVersion(appID, profileID string) *HostedConfigurationVersion {
	empty := &HostedConfigurationVersion{
		ApplicationID:          appID,
		ConfigurationProfileID: profileID,
		ContentType:            contentTypeOctetStream,
		Content:                []byte{},
	}

	if len(b.hostedConfigVersionsByProfile.Get(appProfileKey(appID, profileID))) == 0 {
		return empty
	}

	counter := b.versionCounters[appID][profileID]

	for n := counter; n >= 1; n-- {
		if v, ok := b.hostedConfigVersions.Get(hcvKey(appID, profileID, n)); ok {
			cp := *v

			return &cp
		}
	}

	return empty
}
