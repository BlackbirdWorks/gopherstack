package cognitoidp

import (
	"fmt"
	"time"
)

// SetUICustomizationFull stores extended UI customization including image URL.
func (b *InMemoryBackend) SetUICustomizationFull(poolID, clientID, css, imageURL string) (*UICustomization, error) {
	b.mu.Lock("SetUICustomizationFull")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	now := time.Now()

	existing, ok := b.uiCustomizations.Get(uiKey(poolID, clientID))
	if !ok {
		existing = &UICustomization{
			UserPoolID: poolID,
			ClientID:   clientID,
			CreatedAt:  now,
		}
	}

	existing.CSS = css
	existing.ImageURL = imageURL
	existing.LastModifiedAt = now
	b.uiCustomizations.Put(existing)

	cp := *existing

	return &cp, nil
}

// GetUICustomizationFull returns extended UI customization.
func (b *InMemoryBackend) GetUICustomizationFull(poolID, clientID string) (*UICustomization, error) {
	b.mu.RLock("GetUICustomizationFull")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	existing, ok := b.uiCustomizations.Get(uiKey(poolID, clientID))
	if !ok {
		return &UICustomization{UserPoolID: poolID, ClientID: clientID}, nil
	}

	cp := *existing

	return &cp, nil
}

// uiKey builds the map key for UI customization.
func uiKey(poolID, clientID string) string {
	return poolID + ":" + clientID
}

// SetUICustomization stores hosted-UI CSS for a pool (and optional client).
func (b *InMemoryBackend) SetUICustomization(poolID, clientID, css string) (*UICustomization, error) {
	b.mu.Lock("SetUICustomization")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	ui := &UICustomization{UserPoolID: poolID, ClientID: clientID, CSS: css}
	b.uiCustomizations.Put(ui)
	cp := *ui

	return &cp, nil
}

// GetUICustomization retrieves hosted-UI CSS for a pool and optional client.
func (b *InMemoryBackend) GetUICustomization(poolID, clientID string) (*UICustomization, error) {
	b.mu.RLock("GetUICustomization")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	ui, _ := b.uiCustomizations.Get(uiKey(poolID, clientID))
	if ui == nil {
		return &UICustomization{UserPoolID: poolID, ClientID: clientID}, nil
	}

	cp := *ui

	return &cp, nil
}

// CreateManagedLoginBranding creates a managed login branding record.
func (b *InMemoryBackend) CreateManagedLoginBranding(userPoolID, clientID string) (*ManagedLoginBranding, error) {
	b.mu.Lock("CreateManagedLoginBranding")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	id := "mlb-" + randomAlphanumeric(managedLoginBrandingIDLen)
	now := time.Now()
	mlb := &ManagedLoginBranding{
		ManagedLoginBrandingID: id,
		UserPoolID:             userPoolID,
		ClientID:               clientID,
		CreatedAt:              now,
		LastModifiedAt:         now,
	}
	b.managedLoginBrandings.Put(mlb)

	cp := *mlb

	return &cp, nil
}

// DescribeManagedLoginBranding returns a managed login branding by ID.
func (b *InMemoryBackend) DescribeManagedLoginBranding(userPoolID, brandingID string) (*ManagedLoginBranding, error) {
	b.mu.RLock("DescribeManagedLoginBranding")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	mlb, ok := b.managedLoginBrandings.Get(managedLoginBrandingKey(userPoolID, brandingID))
	if !ok {
		return nil, fmt.Errorf("%w: managed login branding %q not found in pool %q",
			ErrUserPoolNotFound, brandingID, userPoolID)
	}

	cp := *mlb

	return &cp, nil
}

// DescribeManagedLoginBrandingByClient returns the managed login branding for a client.
func (b *InMemoryBackend) DescribeManagedLoginBrandingByClient(
	userPoolID, clientID string,
) (*ManagedLoginBranding, error) {
	b.mu.RLock("DescribeManagedLoginBrandingByClient")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	for _, mlb := range b.managedLoginBrandingsByPool.Get(userPoolID) {
		if mlb.ClientID == clientID {
			cp := *mlb

			return &cp, nil
		}
	}

	return &ManagedLoginBranding{UserPoolID: userPoolID, ClientID: clientID}, nil
}

// UpdateManagedLoginBranding updates a managed login branding record.
func (b *InMemoryBackend) UpdateManagedLoginBranding(userPoolID, brandingID string) (*ManagedLoginBranding, error) {
	b.mu.Lock("UpdateManagedLoginBranding")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	mlb, ok := b.managedLoginBrandings.Get(managedLoginBrandingKey(userPoolID, brandingID))
	if !ok {
		return nil, fmt.Errorf("%w: managed login branding %q not found in pool %q",
			ErrUserPoolNotFound, brandingID, userPoolID)
	}

	mlb.LastModifiedAt = time.Now()
	cp := *mlb

	return &cp, nil
}

// DeleteManagedLoginBranding removes a managed login branding record.
func (b *InMemoryBackend) DeleteManagedLoginBranding(userPoolID, brandingID string) error {
	b.mu.Lock("DeleteManagedLoginBranding")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.managedLoginBrandings.Get(managedLoginBrandingKey(userPoolID, brandingID)); !ok {
		return fmt.Errorf("%w: managed login branding %q not found in pool %q",
			ErrUserPoolNotFound, brandingID, userPoolID)
	}

	b.managedLoginBrandings.Delete(managedLoginBrandingKey(userPoolID, brandingID))

	return nil
}

const (
	managedLoginBrandingIDLen = 8
	userImportJobIDLen        = 10
)
