package appconfig

import (
	"fmt"
	"sort"
)

// CreateExtension creates a new AppConfig extension.
func (b *InMemoryBackend) CreateExtension(
	name, description string,
	actions map[string][]ExtensionAction,
	parameters map[string]ExtensionParameter,
) (*Extension, error) {
	b.mu.Lock("CreateExtension")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	// Enforce name uniqueness.
	if len(b.extensionsByName.Get(name)) > 0 {
		return nil, fmt.Errorf(
			"%w: extension with name %s already exists",
			ErrExtensionAlreadyExists,
			name,
		)
	}

	id := newResourceID()
	ext := &Extension{
		ID:            id,
		Name:          name,
		Description:   description,
		Arn:           b.appconfigARN("extension/" + id),
		VersionNumber: 1,
		Actions:       actions,
		Parameters:    parameters,
	}
	b.extensions.Put(ext)
	cp := *ext

	return &cp, nil
}

// resolveExtension finds an extension by ID or name.
func (b *InMemoryBackend) resolveExtension(identifier string) *Extension {
	if ext, ok := b.extensions.Get(identifier); ok {
		return ext
	}

	if matches := b.extensionsByName.Get(identifier); len(matches) > 0 {
		return matches[0]
	}

	return nil
}

// GetExtension retrieves an extension by identifier (ID or name).
func (b *InMemoryBackend) GetExtension(extensionIdentifier string) (*Extension, error) {
	b.mu.RLock("GetExtension")
	defer b.mu.RUnlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	cp := *ext

	return &cp, nil
}

// ListExtensions returns paginated extensions, optionally filtered by name and/or version number.
func (b *InMemoryBackend) ListExtensions(
	nextToken string,
	maxResults int,
	nameFilter string,
	versionNumber int32,
) ([]Extension, string) {
	b.mu.RLock("ListExtensions")
	defer b.mu.RUnlock()

	all := b.extensions.All()
	out := make([]Extension, 0, len(all))

	for _, ext := range all {
		if nameFilter != "" && ext.Name != nameFilter {
			continue
		}

		if versionNumber > 0 && ext.VersionNumber != versionNumber {
			continue
		}

		out = append(out, *ext)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// UpdateExtension updates an extension's description, actions, and
// parameters. A nil description means the request omitted that field, and
// AWS AppConfig leaves an omitted field unchanged rather than clearing it.
func (b *InMemoryBackend) UpdateExtension(
	extensionIdentifier string,
	description *string,
	actions map[string][]ExtensionAction,
	parameters map[string]ExtensionParameter,
) (*Extension, error) {
	b.mu.Lock("UpdateExtension")
	defer b.mu.Unlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	updated := *ext
	if description != nil {
		updated.Description = *description
	}

	if actions != nil {
		updated.Actions = actions
	}

	if parameters != nil {
		updated.Parameters = parameters
	}

	updated.VersionNumber++
	b.extensions.Put(&updated)
	cp := updated

	return &cp, nil
}

// DeleteExtension deletes an extension by identifier (ID or name).
func (b *InMemoryBackend) DeleteExtension(extensionIdentifier string) error {
	b.mu.Lock("DeleteExtension")
	defer b.mu.Unlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	b.extensions.Delete(ext.ID)
	delete(b.tags, ext.Arn)

	return nil
}

// CreateExtensionAssociation creates an association between an extension and a resource.
func (b *InMemoryBackend) CreateExtensionAssociation(
	extensionIdentifier, resourceIdentifier string,
	parameters map[string]string,
	extensionVersionNumber *int32,
) (*ExtensionAssociation, error) {
	b.mu.Lock("CreateExtensionAssociation")
	defer b.mu.Unlock()

	if extensionIdentifier == "" {
		return nil, fmt.Errorf("%w: ExtensionIdentifier is required", ErrBadRequest)
	}

	if resourceIdentifier == "" {
		return nil, fmt.Errorf("%w: ResourceIdentifier is required", ErrBadRequest)
	}

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	versionNum := ext.VersionNumber
	if extensionVersionNumber != nil {
		versionNum = *extensionVersionNumber
	}

	id := newResourceID()
	assoc := &ExtensionAssociation{
		ID:                     id,
		Arn:                    b.appconfigARN("extensionassociation/" + id),
		ExtensionArn:           ext.Arn,
		ResourceArn:            resourceIdentifier,
		ExtensionVersionNumber: versionNum,
		Parameters:             parameters,
	}
	b.extensionAssociations.Put(assoc)
	cp := *assoc

	return &cp, nil
}

// GetExtensionAssociation retrieves an extension association by ID.
func (b *InMemoryBackend) GetExtensionAssociation(
	extensionAssociationID string,
) (*ExtensionAssociation, error) {
	b.mu.RLock("GetExtensionAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.extensionAssociations.Get(extensionAssociationID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: extension association %s",
			ErrExtensionAssociationNotFound,
			extensionAssociationID,
		)
	}

	cp := *assoc

	return &cp, nil
}

// ListExtensionAssociations returns paginated extension associations,
// optionally filtered by extensionIdentifier (ARN prefix) and/or resourceIdentifier (ARN prefix).
func (b *InMemoryBackend) ListExtensionAssociations(
	nextToken, extensionIdentifier, resourceIdentifier string,
	maxResults int,
) ([]ExtensionAssociation, string) {
	b.mu.RLock("ListExtensionAssociations")
	defer b.mu.RUnlock()

	all := b.extensionAssociations.All()
	out := make([]ExtensionAssociation, 0, len(all))

	for _, a := range all {
		if extensionIdentifier != "" && a.ExtensionArn != extensionIdentifier {
			continue
		}

		if resourceIdentifier != "" && a.ResourceArn != resourceIdentifier {
			continue
		}

		out = append(out, *a)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// DeleteExtensionAssociation deletes an extension association by ID.
func (b *InMemoryBackend) DeleteExtensionAssociation(extensionAssociationID string) error {
	b.mu.Lock("DeleteExtensionAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.extensionAssociations.Get(extensionAssociationID)
	if !ok {
		return fmt.Errorf(
			"%w: extension association %s",
			ErrExtensionAssociationNotFound,
			extensionAssociationID,
		)
	}

	b.extensionAssociations.Delete(extensionAssociationID)
	delete(b.tags, assoc.Arn)

	return nil
}

// UpdateExtensionAssociation updates an extension association's parameters.
func (b *InMemoryBackend) UpdateExtensionAssociation(
	extensionAssociationID string,
	parameters map[string]string,
) (*ExtensionAssociation, error) {
	b.mu.Lock("UpdateExtensionAssociation")
	defer b.mu.Unlock()

	existing, ok := b.extensionAssociations.Get(extensionAssociationID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: extension association %s",
			ErrExtensionAssociationNotFound,
			extensionAssociationID,
		)
	}

	updated := *existing
	if parameters != nil {
		updated.Parameters = parameters
	}

	b.extensionAssociations.Put(&updated)
	cp := updated

	return &cp, nil
}
