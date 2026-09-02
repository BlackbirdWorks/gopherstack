package appconfig

import (
	"fmt"
	"maps"
	"sort"
)

// CreateConfigurationProfile creates a new configuration profile. See
// CreateExperimentDefinition's doc comment for why tags are applied
// directly to b.tags rather than via TagResource.
func (b *InMemoryBackend) CreateConfigurationProfile(
	applicationID, name, description, locationURI, profileType, retrievalRoleArn, kmsKeyIdentifier string,
	validators []Validator,
	tags map[string]string,
) (*ConfigurationProfile, error) {
	b.mu.Lock("CreateConfigurationProfile")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if locationURI == "" {
		return nil, fmt.Errorf("%w: LocationUri is required", ErrBadRequest)
	}

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if len(b.configProfilesByAppName.Get(appNameKey(applicationID, name))) > 0 {
		return nil, fmt.Errorf(
			"%w: configuration profile with name %q already exists in application %s",
			ErrConflict,
			name,
			applicationID,
		)
	}

	profile := &ConfigurationProfile{
		ID:               newResourceID(),
		ApplicationID:    applicationID,
		Name:             name,
		Description:      description,
		LocationURI:      locationURI,
		Type:             profileType,
		RetrievalRoleArn: retrievalRoleArn,
		KmsKeyIdentifier: kmsKeyIdentifier,
		Validators:       validators,
	}
	b.configProfiles.Put(profile)

	if len(tags) > 0 {
		arn := b.appconfigARN("application/" + applicationID + "/configurationprofile/" + profile.ID)
		b.tags[arn] = maps.Clone(tags)
	}

	cp := *profile

	return &cp, nil
}

// GetConfigurationProfile retrieves a configuration profile.
func (b *InMemoryBackend) GetConfigurationProfile(
	applicationID, profileID string,
) (*ConfigurationProfile, error) {
	b.mu.RLock("GetConfigurationProfile")
	defer b.mu.RUnlock()

	profile, ok := b.configProfiles.Get(profileID)
	if !ok || profile.ApplicationID != applicationID {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	cp := *profile

	return &cp, nil
}

// ListConfigurationProfiles returns paginated profiles for an application.
func (b *InMemoryBackend) ListConfigurationProfiles(
	applicationID, nextToken, profileType string,
	maxResults int,
) ([]ConfigurationProfile, string, error) {
	b.mu.RLock("ListConfigurationProfiles")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationID) {
		return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profiles := b.configProfilesByApp.Get(applicationID)
	out := make([]ConfigurationProfile, 0, len(profiles))

	for _, p := range profiles {
		if profileType != "" && p.Type != profileType {
			continue
		}

		out = append(out, *p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// configurationProfileToSummary builds the types.ConfigurationProfileSummary
// shape -- see its doc comment in models.go. ValidatorTypes carries one
// entry per Validators member, in the same order.
func configurationProfileToSummary(p ConfigurationProfile) ConfigurationProfileSummary {
	validatorTypes := make([]string, 0, len(p.Validators))
	for _, v := range p.Validators {
		validatorTypes = append(validatorTypes, v.Type)
	}

	return ConfigurationProfileSummary{
		ApplicationID:  p.ApplicationID,
		ID:             p.ID,
		Name:           p.Name,
		LocationURI:    p.LocationURI,
		Type:           p.Type,
		ValidatorTypes: validatorTypes,
	}
}

// UpdateConfigurationProfile updates a configuration profile. A nil
// name/description/retrievalRoleArn/validators means the request omitted
// that field, and AWS AppConfig leaves an omitted field unchanged rather
// than clearing it.
func (b *InMemoryBackend) UpdateConfigurationProfile(
	applicationID, profileID string,
	name, description, retrievalRoleArn, kmsKeyIdentifier *string,
	validators *[]Validator,
) (*ConfigurationProfile, error) {
	b.mu.Lock("UpdateConfigurationProfile")
	defer b.mu.Unlock()

	existing, ok := b.configProfiles.Get(profileID)
	if !ok || existing.ApplicationID != applicationID {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	updated := *existing
	if name != nil && *name != "" {
		updated.Name = *name
	}

	if description != nil {
		updated.Description = *description
	}

	if retrievalRoleArn != nil {
		updated.RetrievalRoleArn = *retrievalRoleArn
	}

	if kmsKeyIdentifier != nil {
		updated.KmsKeyIdentifier = *kmsKeyIdentifier
	}

	if validators != nil {
		updated.Validators = *validators
	}

	b.configProfiles.Put(&updated)
	cp := updated

	return &cp, nil
}

// DeleteConfigurationProfile deletes a configuration profile.
func (b *InMemoryBackend) DeleteConfigurationProfile(applicationID, profileID string) error {
	b.mu.Lock("DeleteConfigurationProfile")
	defer b.mu.Unlock()

	profile, ok := b.configProfiles.Get(profileID)
	if !ok || profile.ApplicationID != applicationID {
		return fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	profileArn := b.appconfigARN("application/" + applicationID + "/configurationprofile/" + profileID)
	b.configProfiles.Delete(profileID)
	delete(b.tags, profileArn)
	b.deleteExtensionAssociationsForResourceLocked(profileArn)
	b.deleteDeployedConfigsLocked(func(appID, _, pID string) bool {
		return appID == applicationID && pID == profileID
	})

	return nil
}
