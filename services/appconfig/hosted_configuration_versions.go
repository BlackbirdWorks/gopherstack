package appconfig

import (
	"fmt"
	"sort"
	"time"
)

const maxHostedConfigSizeBytes = 1024 * 1024 // 1 MiB, matching AWS limit

// CreateHostedConfigurationVersion creates a hosted configuration version.
func (b *InMemoryBackend) CreateHostedConfigurationVersion(
	applicationID, profileID, contentType, description, versionLabel string,
	content []byte,
) (*HostedConfigurationVersion, error) {
	b.mu.Lock("CreateHostedConfigurationVersion")
	defer b.mu.Unlock()

	if len(content) > maxHostedConfigSizeBytes {
		return nil, fmt.Errorf(
			"%w: content exceeds maximum size of %d bytes",
			ErrPayloadTooLarge,
			maxHostedConfigSizeBytes,
		)
	}

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profile, ok := b.configProfiles.Get(profileID)
	if !ok || profile.ApplicationID != applicationID {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	// VersionLabel must be unique across versions for this profile.
	if versionLabel != "" &&
		len(b.hostedConfigVersionsByLabel.Get(hcvLabelKey(applicationID, profileID, versionLabel))) > 0 {
		return nil, fmt.Errorf(
			"%w: version label %q already exists for profile %s",
			ErrConflict,
			versionLabel,
			profileID,
		)
	}

	if b.versionCounters[applicationID] == nil {
		b.versionCounters[applicationID] = make(map[string]int32)
	}

	b.versionCounters[applicationID][profileID]++
	versionNumber := b.versionCounters[applicationID][profileID]

	v := &HostedConfigurationVersion{
		ApplicationID:          applicationID,
		ConfigurationProfileID: profileID,
		ContentType:            contentType,
		Description:            description,
		VersionLabel:           versionLabel,
		Content:                content,
		VersionNumber:          versionNumber,
		CreatedAt:              time.Now(),
	}
	b.hostedConfigVersions.Put(v)
	cp := *v

	return &cp, nil
}

// GetHostedConfigurationVersion retrieves a hosted configuration version.
func (b *InMemoryBackend) GetHostedConfigurationVersion(
	applicationID, profileID string,
	versionNumber int32,
) (*HostedConfigurationVersion, error) {
	b.mu.RLock("GetHostedConfigurationVersion")
	defer b.mu.RUnlock()

	v, ok := b.hostedConfigVersions.Get(hcvKey(applicationID, profileID, versionNumber))
	if !ok {
		return nil, fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	cp := *v

	return &cp, nil
}

// ListHostedConfigurationVersions returns paginated versions for a profile, optionally filtered by versionLabel.
func (b *InMemoryBackend) ListHostedConfigurationVersions(
	applicationID, profileID, nextToken, versionLabel string, maxResults int,
) ([]HostedConfigurationVersion, string, error) {
	b.mu.RLock("ListHostedConfigurationVersions")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationID) {
		return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if profile, ok := b.configProfiles.Get(profileID); !ok || profile.ApplicationID != applicationID {
		return nil, "", fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	profileVersions := b.hostedConfigVersionsByProfile.Get(appProfileKey(applicationID, profileID))

	out := make([]HostedConfigurationVersion, 0, len(profileVersions))
	for _, v := range profileVersions {
		if versionLabel != "" && v.VersionLabel != versionLabel {
			continue
		}

		out = append(out, *v)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VersionNumber < out[j].VersionNumber })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// DeleteHostedConfigurationVersion deletes a hosted configuration version.
func (b *InMemoryBackend) DeleteHostedConfigurationVersion(
	applicationID, profileID string,
	versionNumber int32,
) error {
	b.mu.Lock("DeleteHostedConfigurationVersion")
	defer b.mu.Unlock()

	key := hcvKey(applicationID, profileID, versionNumber)
	if !b.hostedConfigVersions.Has(key) {
		return fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	b.hostedConfigVersions.Delete(key)

	return nil
}
