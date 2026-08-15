package appconfig

import (
	"fmt"
	"maps"
	"sort"
)

// CreateExtension creates a new AppConfig extension at version 1. See
// CreateExperimentDefinition's doc comment for why tags are applied
// directly to b.tags rather than via TagResource.
func (b *InMemoryBackend) CreateExtension(
	name, description string,
	actions map[string][]ExtensionAction,
	parameters map[string]ExtensionParameter,
	tags map[string]string,
) (*Extension, error) {
	b.mu.Lock("CreateExtension")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	// Enforce name uniqueness across every version of every extension.
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

	if len(tags) > 0 {
		b.tags[ext.Arn] = maps.Clone(tags)
	}

	cp := *ext

	return &cp, nil
}

// resolveExtensionID resolves an identifier (ID or name) to the extension ID
// it names, without regard to which version(s) currently exist. Must be
// called under lock.
func (b *InMemoryBackend) resolveExtensionID(identifier string) (string, bool) {
	if len(b.extensionsByID.Get(identifier)) > 0 {
		return identifier, true
	}

	if matches := b.extensionsByName.Get(identifier); len(matches) > 0 {
		return matches[0].ID, true
	}

	return "", false
}

// latestExtensionVersion returns the highest-VersionNumber row currently
// stored for extension id. Must be called under lock; id must already be
// known to exist (e.g. via resolveExtensionID) or this returns nil.
func (b *InMemoryBackend) latestExtensionVersion(id string) *Extension {
	versions := b.extensionsByID.Get(id)

	var latest *Extension
	for _, v := range versions {
		if latest == nil || v.VersionNumber > latest.VersionNumber {
			latest = v
		}
	}

	return latest
}

// GetExtension retrieves an extension by identifier (ID or name). A
// versionNumber of 0 means "not specified" -- matching real AWS AppConfig,
// this returns the highest (most recently created) version.
func (b *InMemoryBackend) GetExtension(
	extensionIdentifier string,
	versionNumber int32,
) (*Extension, error) {
	b.mu.RLock("GetExtension")
	defer b.mu.RUnlock()

	id, ok := b.resolveExtensionID(extensionIdentifier)
	if !ok {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	var ext *Extension
	if versionNumber > 0 {
		ext, ok = b.extensions.Get(extensionVersionKey(id, versionNumber))
		if !ok {
			return nil, fmt.Errorf(
				"%w: extension %s version %d",
				ErrExtensionNotFound,
				extensionIdentifier,
				versionNumber,
			)
		}
	} else {
		ext = b.latestExtensionVersion(id)
	}

	cp := *ext

	return &cp, nil
}

// ListExtensions returns paginated extensions, optionally filtered by name.
// Real ListExtensionsInput has no version filter (extensions are versioned
// resources, but ListExtensions summarizes one row per extension at its
// current/highest version -- there is no ListExtensionVersions API), so one
// row is returned per distinct extension ID, at its latest version.
func (b *InMemoryBackend) ListExtensions(
	nextToken string,
	maxResults int,
	nameFilter string,
) ([]Extension, string) {
	b.mu.RLock("ListExtensions")
	defer b.mu.RUnlock()

	latestByID := make(map[string]*Extension)

	for _, ext := range b.extensions.All() {
		if nameFilter != "" && ext.Name != nameFilter {
			continue
		}

		if cur, ok := latestByID[ext.ID]; !ok || ext.VersionNumber > cur.VersionNumber {
			latestByID[ext.ID] = ext
		}
	}

	out := make([]Extension, 0, len(latestByID))
	for _, ext := range latestByID {
		out = append(out, *ext)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// extensionToSummary builds the types.ExtensionSummary shape -- see its doc
// comment in models.go.
func extensionToSummary(e Extension) ExtensionSummary {
	return ExtensionSummary{
		Arn:           e.Arn,
		Description:   e.Description,
		ID:            e.ID,
		Name:          e.Name,
		VersionNumber: e.VersionNumber,
	}
}

// UpdateExtension updates an extension's description, actions, and
// parameters by creating a NEW version from the current highest version --
// matching real AWS AppConfig, where every UpdateExtension call produces a
// new, independently addressable extension version rather than mutating one
// in place. A nil description means the request omitted that field, and AWS
// AppConfig leaves an omitted field unchanged rather than clearing it.
func (b *InMemoryBackend) UpdateExtension(
	extensionIdentifier string,
	description *string,
	actions map[string][]ExtensionAction,
	parameters map[string]ExtensionParameter,
) (*Extension, error) {
	b.mu.Lock("UpdateExtension")
	defer b.mu.Unlock()

	id, ok := b.resolveExtensionID(extensionIdentifier)
	if !ok {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	latest := b.latestExtensionVersion(id)

	updated := *latest
	if description != nil {
		updated.Description = *description
	}

	if actions != nil {
		updated.Actions = actions
	}

	if parameters != nil {
		updated.Parameters = parameters
	}

	updated.VersionNumber = latest.VersionNumber + 1
	b.extensions.Put(&updated)
	cp := updated

	return &cp, nil
}

// DeleteExtension deletes an extension version by identifier (ID or name).
// A versionNumber of 0 means "not specified" -- matching real AWS
// AppConfig, this deletes the highest (most recently created) version only,
// not every version. Deleting the last remaining version removes the
// extension entirely, including its tags. Returns ErrConflict (matching
// real AWS's ConflictException) if any ExtensionAssociation still
// references the version being deleted.
func (b *InMemoryBackend) DeleteExtension(extensionIdentifier string, versionNumber int32) error {
	b.mu.Lock("DeleteExtension")
	defer b.mu.Unlock()

	id, ok := b.resolveExtensionID(extensionIdentifier)
	if !ok {
		return fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	var target *Extension
	if versionNumber > 0 {
		target, ok = b.extensions.Get(extensionVersionKey(id, versionNumber))
		if !ok {
			return fmt.Errorf(
				"%w: extension %s version %d",
				ErrExtensionNotFound,
				extensionIdentifier,
				versionNumber,
			)
		}
	} else {
		target = b.latestExtensionVersion(id)
	}

	for _, assoc := range b.extensionAssociations.All() {
		if assoc.ExtensionArn == target.Arn && assoc.ExtensionVersionNumber == target.VersionNumber {
			return fmt.Errorf(
				"%w: extension %s version %d is still associated by %s",
				ErrConflict,
				extensionIdentifier,
				target.VersionNumber,
				assoc.ID,
			)
		}
	}

	b.extensions.Delete(extensionVersionKey(id, target.VersionNumber))

	if len(b.extensionsByID.Get(id)) == 0 {
		delete(b.tags, target.Arn)
	}

	return nil
}

// CreateExtensionAssociation creates an association between an extension and
// a resource. See CreateExperimentDefinition's doc comment for why tags are
// applied directly to b.tags rather than via TagResource.
func (b *InMemoryBackend) CreateExtensionAssociation(
	extensionIdentifier, resourceIdentifier string,
	parameters map[string]string,
	extensionVersionNumber *int32,
	tags map[string]string,
) (*ExtensionAssociation, error) {
	b.mu.Lock("CreateExtensionAssociation")
	defer b.mu.Unlock()

	if extensionIdentifier == "" {
		return nil, fmt.Errorf("%w: ExtensionIdentifier is required", ErrBadRequest)
	}

	if resourceIdentifier == "" {
		return nil, fmt.Errorf("%w: ResourceIdentifier is required", ErrBadRequest)
	}

	extID, ok := b.resolveExtensionID(extensionIdentifier)
	if !ok {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	var ext *Extension
	if extensionVersionNumber != nil {
		ext, ok = b.extensions.Get(extensionVersionKey(extID, *extensionVersionNumber))
		if !ok {
			return nil, fmt.Errorf(
				"%w: extension %s version %d",
				ErrExtensionNotFound,
				extensionIdentifier,
				*extensionVersionNumber,
			)
		}
	} else {
		ext = b.latestExtensionVersion(extID)
	}

	versionNum := ext.VersionNumber

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

	if len(tags) > 0 {
		b.tags[assoc.Arn] = maps.Clone(tags)
	}

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

// extensionAssociationToSummary builds the types.ExtensionAssociationSummary
// shape -- see its doc comment in models.go.
func extensionAssociationToSummary(a ExtensionAssociation) ExtensionAssociationSummary {
	return ExtensionAssociationSummary{
		ExtensionArn: a.ExtensionArn,
		ID:           a.ID,
		ResourceArn:  a.ResourceArn,
	}
}

// deleteExtensionAssociationsForResourceLocked removes every
// ExtensionAssociation whose ResourceArn is resourceArn, and its tags, so
// deleting an application/environment/configuration profile leaves no
// ghost association rows referencing it. Must be called under lock.
func (b *InMemoryBackend) deleteExtensionAssociationsForResourceLocked(resourceArn string) {
	for _, assoc := range b.extensionAssociations.All() {
		if assoc.ResourceArn != resourceArn {
			continue
		}

		b.extensionAssociations.Delete(assoc.ID)
		delete(b.tags, assoc.Arn)
	}
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
