package rolesanywhere

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) crlARN(region, id string) string {
	return arn.Build("rolesanywhere", region, b.accountID, fmt.Sprintf("crl/%s", id))
}

// ImportCrl imports a new CRL.
func (b *InMemoryBackend) ImportCrl(
	ctx context.Context,
	name string,
	crlData []byte,
	trustAnchorArn string,
	enabled bool,
	tags []TagEntry,
) (*Crl, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("ImportCrl")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	for _, c := range b.crlsByRegion.Get(region) {
		if c.Name == name {
			return nil, ErrCrlAlreadyExists
		}
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	crl := &Crl{
		CrlID:          id,
		CrlArn:         b.crlARN(region, id),
		Name:           name,
		region:         region,
		CrlData:        crlData,
		TrustAnchorArn: trustAnchorArn,
		Enabled:        enabled,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.crls.Put(crl)

	if len(tags) > 0 {
		b.tagsStore(region)[crl.CrlArn] = cloneTags(tags)
	}

	return copyCrl(crl), nil
}

// GetCrl returns a CRL by ID.
func (b *InMemoryBackend) GetCrl(ctx context.Context, id string) (*Crl, error) {
	b.mu.RLock("GetCrl")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	return copyCrl(crl), nil
}

// ListCrls returns all CRLs with optional pagination.
func (b *InMemoryBackend) ListCrls(ctx context.Context, pageToken string, maxResults int) ([]*Crl, string, error) {
	b.mu.RLock("ListCrls")
	defer b.mu.RUnlock()

	items, token := listByRegionIndex(
		b.crlsByRegion,
		getRegion(ctx, b.defaultRegion),
		copyCrl,
		func(c *Crl) string { return c.Name },
		func(c *Crl) string { return c.CrlID },
		pageToken,
		maxResults,
	)

	return items, token, nil
}

// UpdateCrl updates a CRL's name and/or data.
func (b *InMemoryBackend) UpdateCrl(ctx context.Context, id, name string, crlData []byte) (*Crl, error) {
	b.mu.Lock("UpdateCrl")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	if name != "" {
		crl.Name = name
	}

	if len(crlData) > 0 {
		crl.CrlData = crlData
	}

	crl.UpdatedAt = time.Now().UTC()

	return copyCrl(crl), nil
}

// DeleteCrl removes a CRL.
func (b *InMemoryBackend) DeleteCrl(ctx context.Context, id string) (*Crl, error) {
	b.mu.Lock("DeleteCrl")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	snap := copyCrl(crl)
	b.crls.Delete(regionKey(region, id))

	return snap, nil
}

// EnableCrl enables a CRL.
func (b *InMemoryBackend) EnableCrl(ctx context.Context, id string) (*Crl, error) {
	return b.setCrlEnabled(ctx, id, true)
}

// DisableCrl disables a CRL.
func (b *InMemoryBackend) DisableCrl(ctx context.Context, id string) (*Crl, error) {
	return b.setCrlEnabled(ctx, id, false)
}

func (b *InMemoryBackend) setCrlEnabled(ctx context.Context, id string, enabled bool) (*Crl, error) {
	b.mu.Lock("setCrlEnabled")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	crl.Enabled = enabled
	crl.UpdatedAt = time.Now().UTC()

	return copyCrl(crl), nil
}
