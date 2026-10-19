package cognitoidp

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"
)

// AdminDisableProviderForUser unlinks a linked external identity from its native user, or deletes the
// standalone federated profile, so the identity must sign in afresh.
func (b *InMemoryBackend) AdminDisableProviderForUser(userPoolID, providerName, _, attrValue string) error {
	b.mu.Lock("AdminDisableProviderForUser")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if providerName == "" || attrValue == "" {
		return fmt.Errorf("%w: User.ProviderName and User.ProviderAttributeValue are required", ErrInvalidParameter)
	}

	if _, ok := b.identityProviders.Get(identityProviderKey(userPoolID, providerName)); !ok {
		return fmt.Errorf(
			"%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound,
			providerName,
			userPoolID,
		)
	}

	if owner := b.linkedUserLocked(userPoolID, providerName, attrValue); owner != nil {
		owner.LinkedProviders = slices.DeleteFunc(owner.LinkedProviders, func(l ProviderLink) bool {
			return l.ProviderName == providerName && l.ProviderAttributeValue == attrValue
		})
		owner.Attributes[attrIdentities] = removeIdentity(owner.Attributes[attrIdentities], providerName, attrValue)
		owner.UpdatedAt = time.Now()

		return nil
	}

	fed, ok := b.users.Get(userKey(userPoolID, providerName+"_"+attrValue))
	if !ok || fed.Status != UserStatusExternalProvider {
		return fmt.Errorf("%w: no user is linked to %s identity %q", ErrUserNotFound, providerName, attrValue)
	}

	b.deleteUserStateLocked(userPoolID, fed.Username)

	return nil
}

// CreateIdentityProviderFull creates an identity provider with AttributeMapping and IdpIdentifiers.
func (b *InMemoryBackend) CreateIdentityProviderFull(
	userPoolID, providerName, providerType string,
	providerDetails map[string]string,
	attributeMapping map[string]string,
	idpIdentifiers []string,
) (*IdentityProvider, error) {
	b.mu.Lock("CreateIdentityProviderFull")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.identityProviders.Get(identityProviderKey(userPoolID, providerName)); exists {
		return nil, fmt.Errorf("%w: identity provider %q already exists in pool %q",
			ErrDuplicateProvider, providerName, userPoolID)
	}

	now := time.Now()

	details := make(map[string]string, len(providerDetails))
	maps.Copy(details, providerDetails)

	attrMap := make(map[string]string, len(attributeMapping))
	maps.Copy(attrMap, attributeMapping)

	ids := make([]string, len(idpIdentifiers))
	copy(ids, idpIdentifiers)

	idp := &IdentityProvider{
		UserPoolID:       userPoolID,
		ProviderName:     providerName,
		ProviderType:     providerType,
		ProviderDetails:  details,
		AttributeMapping: attrMap,
		IdpIdentifiers:   ids,
		CreatedAt:        now,
		LastModifiedAt:   now,
	}

	b.identityProviders.Put(idp)

	cp := *idp

	return &cp, nil
}

// UpdateIdentityProviderFull updates an identity provider with AttributeMapping and IdpIdentifiers.
func (b *InMemoryBackend) UpdateIdentityProviderFull(
	userPoolID, providerName string,
	providerDetails map[string]string,
	attributeMapping map[string]string,
	idpIdentifiers []string,
) (*IdentityProvider, error) {
	b.mu.Lock("UpdateIdentityProviderFull")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	idp, ok := b.identityProviders.Get(identityProviderKey(userPoolID, providerName))
	if !ok {
		return nil, fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	if providerDetails != nil {
		idp.ProviderDetails = maps.Clone(providerDetails)
	}

	if attributeMapping != nil {
		idp.AttributeMapping = maps.Clone(attributeMapping)
	}

	if idpIdentifiers != nil {
		ids := make([]string, len(idpIdentifiers))
		copy(ids, idpIdentifiers)
		idp.IdpIdentifiers = ids
	}

	idp.LastModifiedAt = time.Now()

	cp := *idp

	return &cp, nil
}

// CreateIdentityProvider creates a new identity provider in the given pool.
func (b *InMemoryBackend) CreateIdentityProvider(
	userPoolID, providerName, providerType string,
	providerDetails map[string]string,
) (*IdentityProvider, error) {
	b.mu.Lock("CreateIdentityProvider")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.identityProviders.Get(identityProviderKey(userPoolID, providerName)); exists {
		// CreateIdentityProvider's own deserializer models
		// DuplicateProviderException for this, not GroupExistsException
		// (ErrAlreadyExists is CreateGroup's sentinel, not this op's).
		return nil, fmt.Errorf("%w: identity provider %q already exists in pool %q",
			ErrDuplicateProvider, providerName, userPoolID)
	}

	now := time.Now()
	idp := &IdentityProvider{
		UserPoolID:      userPoolID,
		ProviderName:    providerName,
		ProviderType:    providerType,
		ProviderDetails: maps.Clone(providerDetails),
		CreatedAt:       now,
		LastModifiedAt:  now,
	}
	b.identityProviders.Put(idp)

	cp := *idp
	cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

	return &cp, nil
}

// DescribeIdentityProvider returns an identity provider by pool and provider name.
func (b *InMemoryBackend) DescribeIdentityProvider(userPoolID, providerName string) (*IdentityProvider, error) {
	b.mu.RLock("DescribeIdentityProvider")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	idp, ok := b.identityProviders.Get(identityProviderKey(userPoolID, providerName))
	if !ok {
		return nil, fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	cp := *idp
	cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

	return &cp, nil
}

// GetIdentityProviderByIdentifier searches all providers in a pool for the given identifier string.
func (b *InMemoryBackend) GetIdentityProviderByIdentifier(userPoolID, identifier string) (*IdentityProvider, error) {
	b.mu.RLock("GetIdentityProviderByIdentifier")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	for _, idp := range b.identityProvidersByPool.Get(userPoolID) {
		if idp.ProviderName == identifier {
			cp := *idp
			cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

			return &cp, nil
		}

		if slices.Contains(idp.IdpIdentifiers, identifier) {
			cp := *idp
			cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: identity provider with identifier %q not found in pool %q",
		ErrUserPoolNotFound, identifier, userPoolID)
}

// ListIdentityProviders returns all identity providers for a pool sorted by name.
func (b *InMemoryBackend) ListIdentityProviders(userPoolID string) ([]*IdentityProvider, error) {
	b.mu.RLock("ListIdentityProviders")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolProviders := b.identityProvidersByPool.Get(userPoolID)
	out := make([]*IdentityProvider, 0, len(poolProviders))

	for _, idp := range poolProviders {
		cp := *idp
		cp.ProviderDetails = maps.Clone(idp.ProviderDetails)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ProviderName < out[j].ProviderName })

	return out, nil
}

// UpdateIdentityProvider updates an existing identity provider's details.
func (b *InMemoryBackend) UpdateIdentityProvider(
	userPoolID, providerName string,
	providerDetails map[string]string,
) (*IdentityProvider, error) {
	b.mu.Lock("UpdateIdentityProvider")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	idp, ok := b.identityProviders.Get(identityProviderKey(userPoolID, providerName))
	if !ok {
		return nil, fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	if providerDetails != nil {
		idp.ProviderDetails = maps.Clone(providerDetails)
	}
	idp.LastModifiedAt = time.Now()

	cp := *idp
	cp.ProviderDetails = maps.Clone(idp.ProviderDetails)

	return &cp, nil
}

// DeleteIdentityProvider removes an identity provider from a pool.
func (b *InMemoryBackend) DeleteIdentityProvider(userPoolID, providerName string) error {
	b.mu.Lock("DeleteIdentityProvider")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.identityProviders.Get(identityProviderKey(userPoolID, providerName)); !ok {
		return fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, providerName, userPoolID)
	}

	b.identityProviders.Delete(identityProviderKey(userPoolID, providerName))

	return nil
}

// AdminLinkProviderForUser links an external identity (SourceUser) to an existing native user
// (DestinationUser); a standalone federated profile for that identity is merged away.
func (b *InMemoryBackend) AdminLinkProviderForUser(
	userPoolID, destinationUsername string,
	sourceProviderName, sourceAttrName, sourceAttrValue string,
) error {
	b.mu.Lock("AdminLinkProviderForUser")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if destinationUsername == "" {
		return fmt.Errorf("%w: DestinationUser.ProviderAttributeValue is required", ErrInvalidParameter)
	}

	user, ok := b.users.Get(userKey(userPoolID, destinationUsername))
	if !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, destinationUsername)
	}

	if user.Status == UserStatusExternalProvider {
		return fmt.Errorf("%w: DestinationUser must be a native Cognito user", ErrInvalidParameter)
	}

	if sourceProviderName == "" || sourceAttrName == "" || sourceAttrValue == "" {
		return fmt.Errorf(
			"%w: SourceUser must include ProviderName, ProviderAttributeName, and ProviderAttributeValue",
			ErrInvalidParameter)
	}

	idp, ok := b.identityProviders.Get(identityProviderKey(userPoolID, sourceProviderName))
	if !ok {
		return fmt.Errorf("%w: identity provider %q not found in pool %q",
			ErrUserPoolNotFound, sourceProviderName, userPoolID)
	}

	if owner := b.linkedUserLocked(userPoolID, sourceProviderName, sourceAttrValue); owner != nil {
		if owner == user {
			return nil
		}

		return fmt.Errorf("%w: identity %q is already linked to another user", ErrAliasExists, sourceAttrValue)
	}

	if fed, found := b.users.Get(userKey(userPoolID, sourceProviderName+"_"+sourceAttrValue)); found &&
		fed.Status == UserStatusExternalProvider {
		b.deleteUserStateLocked(userPoolID, fed.Username)
	}

	user.LinkedProviders = append(user.LinkedProviders, ProviderLink{
		ProviderName:           sourceProviderName,
		ProviderAttributeName:  sourceAttrName,
		ProviderAttributeValue: sourceAttrValue,
	})

	if user.Attributes == nil {
		user.Attributes = make(map[string]string)
	}

	user.Attributes[attrIdentities] = appendIdentity(user.Attributes[attrIdentities], identityRecord{
		UserID: sourceAttrValue, ProviderName: sourceProviderName, ProviderType: idp.ProviderType,
		Primary: attrVerifiedFalse, DateCreated: strconv.FormatInt(time.Now().UnixMilli(), 10),
	})
	user.UpdatedAt = time.Now()

	return nil
}
