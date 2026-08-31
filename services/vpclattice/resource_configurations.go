package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveResourceConfigurationID resolves a resource configuration
// identifier (ID or ARN) to an ID.
func (b *InMemoryBackend) resolveResourceConfigurationID(identifier string) (string, bool) {
	if b.resourceConfigurations.Has(identifier) {
		return identifier, true
	}

	for _, c := range b.resourceConfigurations.All() {
		if c.ARN == identifier {
			return c.ID, true
		}
	}

	return "", false
}

// ------- ResourceConfiguration operations -------

// CreateResourceConfiguration creates a new resource configuration.
// resourceGatewayIdentifier and resourceConfigurationGroupIdentifier are
// both optional and, when non-empty, must resolve to an existing resource
// gateway / GROUP-type resource configuration respectively -- a CHILD
// configuration inherits its ResourceGatewayId from the parent GROUP, per
// CreateResourceConfigurationInput's doc comment. groupDomain, when
// non-empty, is stored directly (real API: settable on GROUP-type create);
// a CHILD with no explicit groupDomain inherits its parent GROUP's value.
func (b *InMemoryBackend) CreateResourceConfiguration(
	ctx context.Context,
	name, resourceType, protocol, resourceGatewayIdentifier, resourceConfigurationGroupIdentifier string,
	allowAssociationToShareableServiceNetwork bool,
	portRanges []string,
	definition *ResourceConfigurationDefinition,
	customDomainName, domainVerificationID, groupDomain string,
	tags map[string]string,
) (*ResourceConfiguration, error) {
	if name == "" || resourceType == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateResourceConfiguration")
	defer b.mu.Unlock()

	if len(b.resourceConfigurationsByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	resourceGatewayID, groupID, inheritedGroupDomain, err := b.resolveResourceConfigurationParents(
		resourceType, resourceGatewayIdentifier, resourceConfigurationGroupIdentifier,
	)
	if err != nil {
		return nil, err
	}

	if groupDomain == "" {
		groupDomain = inheritedGroupDomain
	}

	now := time.Now().UTC()
	id := newID(idPrefixResourceConfiguration)
	region := b.regionFor(ctx)
	rcARN := arn.Build(arnService, region, b.accountID, resourceResourceConfiguration+"/"+id)

	rc := &storedResourceConfiguration{
		ARN:                          rcARN,
		ID:                           id,
		Name:                         name,
		Type:                         resourceType,
		Protocol:                     protocol,
		ResourceGatewayID:            resourceGatewayID,
		ResourceConfigurationGroupID: groupID,
		GroupDomain:                  groupDomain,
		CustomDomainName:             customDomainName,
		DomainVerificationID:         domainVerificationID,
		PortRanges:                   append([]string(nil), portRanges...),
		Definition:                   definition,
		AllowShareableAssoc:          allowAssociationToShareableServiceNetwork,
		Status:                       statusActive,
		Tags:                         copyTags(tags),
		CreatedAt:                    now,
		LastUpdatedAt:                now,
		Region:                       region,
	}

	b.resourceConfigurations.Put(rc)
	b.tags[rcARN] = copyTags(tags)

	out := rc.toResourceConfiguration()
	out.DomainVerificationARN, out.DomainVerificationStatus = b.resolveDomainVerificationInfo(
		b.effectiveDomainVerificationID(rc),
	)

	return out, nil
}

// effectiveDomainVerificationID returns rc's own DomainVerificationID, or --
// for a CHILD resource configuration that never set one -- its parent
// GROUP's current DomainVerificationID. Real AWS: "Child resources inherit
// the verification status of the [parent GROUP's] domain"
// (CreateResourceConfigurationInput.GroupDomain doc comment), and that
// inheritance is live, not a create-time snapshot -- if the parent's domain
// verification later changes, the CHILD's Get/CreateResourceConfiguration
// response must reflect the parent's CURRENT identifier, so this resolves
// against the parent record on every call rather than copying the ID once.
// Must be called under b.mu.
func (b *InMemoryBackend) effectiveDomainVerificationID(rc *storedResourceConfiguration) string {
	if rc.DomainVerificationID != "" || rc.Type != "CHILD" || rc.ResourceConfigurationGroupID == "" {
		return rc.DomainVerificationID
	}

	parent, ok := b.resourceConfigurations.Get(rc.ResourceConfigurationGroupID)
	if !ok {
		return ""
	}

	return parent.DomainVerificationID
}

// resolveResourceConfigurationParents validates and resolves
// resourceGatewayIdentifier/resourceConfigurationGroupIdentifier for
// CreateResourceConfiguration. Must be called under b.mu.
func (b *InMemoryBackend) resolveResourceConfigurationParents(
	resourceType, resourceGatewayIdentifier, resourceConfigurationGroupIdentifier string,
) (string, string, string, error) {
	var resourceGatewayID string

	if resourceGatewayIdentifier != "" {
		id, ok := b.resolveResourceGatewayID(resourceGatewayIdentifier)
		if !ok {
			return "", "", "", ErrNotFound
		}

		resourceGatewayID = id
	}

	if resourceConfigurationGroupIdentifier == "" {
		return resourceGatewayID, "", "", nil
	}

	pid, ok := b.resolveResourceConfigurationID(resourceConfigurationGroupIdentifier)
	if !ok {
		return "", "", "", ErrNotFound
	}

	parent, _ := b.resourceConfigurations.Get(pid)
	if parent.Type != "GROUP" {
		return "", "", "", ErrInvalidParameter
	}

	if resourceType == "CHILD" {
		resourceGatewayID = parent.ResourceGatewayID
	}

	return resourceGatewayID, pid, parent.GroupDomain, nil
}

// GetResourceConfiguration returns a resource configuration.
func (b *InMemoryBackend) GetResourceConfiguration(id string) (*ResourceConfiguration, error) {
	b.mu.RLock("GetResourceConfiguration")
	defer b.mu.RUnlock()

	rcID, ok := b.resolveResourceConfigurationID(id)
	if !ok {
		return nil, ErrNotFound
	}

	rc, _ := b.resourceConfigurations.Get(rcID)

	out := rc.toResourceConfiguration()
	out.DomainVerificationARN, out.DomainVerificationStatus = b.resolveDomainVerificationInfo(
		b.effectiveDomainVerificationID(rc),
	)

	return out, nil
}

// UpdateResourceConfiguration updates a resource configuration's
// AllowShareableAssoc/PortRanges/
// ResourceConfigurationDefinition -- the only fields
// UpdateResourceConfigurationInput accepts besides the identifier.
func (b *InMemoryBackend) UpdateResourceConfiguration(
	id string,
	allowAssociationToShareableServiceNetwork *bool,
	portRanges []string,
	definition *ResourceConfigurationDefinition,
) (*ResourceConfiguration, error) {
	b.mu.Lock("UpdateResourceConfiguration")
	defer b.mu.Unlock()

	rcID, ok := b.resolveResourceConfigurationID(id)
	if !ok {
		return nil, ErrNotFound
	}

	rc, _ := b.resourceConfigurations.Get(rcID)

	if allowAssociationToShareableServiceNetwork != nil {
		rc.AllowShareableAssoc = *allowAssociationToShareableServiceNetwork
	}

	if portRanges != nil {
		rc.PortRanges = append([]string(nil), portRanges...)
	}

	if definition != nil {
		rc.Definition = definition
	}

	rc.LastUpdatedAt = time.Now().UTC()

	return rc.toResourceConfiguration(), nil
}

// DeleteResourceConfiguration deletes a resource configuration. Real AWS
// rejects the delete with ConflictException while any
// ServiceNetworkResourceAssociation or CHILD configuration still references
// it.
func (b *InMemoryBackend) DeleteResourceConfiguration(id string) error {
	b.mu.Lock("DeleteResourceConfiguration")
	defer b.mu.Unlock()

	rcID, ok := b.resolveResourceConfigurationID(id)
	if !ok {
		return ErrNotFound
	}

	for _, s := range b.snras.All() {
		if s.ResourceConfigurationID == rcID {
			return ErrDependencyConflict
		}
	}

	for _, c := range b.resourceConfigurations.All() {
		if c.ResourceConfigurationGroupID == rcID {
			return ErrDependencyConflict
		}
	}

	rc, _ := b.resourceConfigurations.Get(rcID)
	b.resourceConfigurations.Delete(rcID)
	delete(b.tags, rc.ARN)

	return nil
}

// ListResourceConfigurations returns a paginated list of resource
// configurations, optionally filtered by resource gateway or group parent.
func (b *InMemoryBackend) ListResourceConfigurations(
	ctx context.Context,
	resourceGatewayIdentifier, resourceConfigurationGroupIdentifier string,
	maxResults int32,
	nextToken string,
) ([]*ResourceConfigurationSummary, string, error) {
	b.mu.RLock("ListResourceConfigurations")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ResourceConfigurationSummary, 0, b.resourceConfigurations.Len())

	for _, rc := range b.resourceConfigurations.All() {
		if rc.Region != region {
			continue
		}

		if resourceGatewayIdentifier != "" && rc.ResourceGatewayID != resourceGatewayIdentifier {
			continue
		}

		if resourceConfigurationGroupIdentifier != "" &&
			rc.ResourceConfigurationGroupID != resourceConfigurationGroupIdentifier {
			continue
		}

		all = append(all, rc.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
