package azurearm

import (
	"context"
	"fmt"
	"strings"
)

const (
	cosmosKeyKindPrimary           = "Primary"
	cosmosKeyKindSecondary         = "Secondary"
	cosmosKeyKindPrimaryReadonly   = "PrimaryReadonly"
	cosmosKeyKindSecondaryReadonly = "SecondaryReadonly"
)

func (p *CosmosProvider) keysFor(id ResourceID) (*storedCosmosAccount, error) {
	if !p.isAccount(id) {
		return nil, ErrResourceNotFound
	}

	a, ok := p.owned(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrCosmosAccountNotFound
	}

	p.authorizeKeys(a.keys)

	return a, nil
}

// ListKeys implements ResourceProvider.
func (p *CosmosProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("ListKeys")
	defer p.mu.RUnlock()

	a, err := p.keysFor(id)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"primaryMasterKey":           a.keys.Primary,
		"secondaryMasterKey":         a.keys.Secondary,
		"primaryReadonlyMasterKey":   a.keys.PrimaryReadonly,
		"secondaryReadonlyMasterKey": a.keys.SecondaryReadonly,
	}, nil
}

// ReadonlyKeys serves the readonlykeys action.
func (p *CosmosProvider) ReadonlyKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("ReadonlyKeys")
	defer p.mu.RUnlock()

	a, err := p.keysFor(id)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"primaryReadonlyMasterKey":   a.keys.PrimaryReadonly,
		"secondaryReadonlyMasterKey": a.keys.SecondaryReadonly,
	}, nil
}

// ConnectionStrings serves the listConnectionStrings action.
func (p *CosmosProvider) ConnectionStrings(ctx context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("ConnectionStrings")
	defer p.mu.RUnlock()

	a, err := p.keysFor(id)
	if err != nil {
		return nil, err
	}

	endpoint := p.documentEndpoint(requestHostOr(ctx, a.host))
	kinds := []struct {
		kind, desc, key string
	}{
		{cosmosKeyKindPrimary, "Primary SQL Connection String", a.keys.Primary},
		{cosmosKeyKindSecondary, "Secondary SQL Connection String", a.keys.Secondary},
		{cosmosKeyKindPrimaryReadonly, "Primary Read-Only SQL Connection String", a.keys.PrimaryReadonly},
		{cosmosKeyKindSecondaryReadonly, "Secondary Read-Only SQL Connection String", a.keys.SecondaryReadonly},
	}

	out := make([]any, 0, len(kinds))

	for _, k := range kinds {
		out = append(out, map[string]any{
			"connectionString": "AccountEndpoint=" + endpoint + ";AccountKey=" + k.key + ";",
			"description":      k.desc,
			"keyKind":          k.kind,
			cosmosFieldType:    "Sql",
		})
	}

	return map[string]any{"connectionStrings": out}, nil
}

func requestHostOr(ctx context.Context, fallback string) string {
	if h := RequestHostFromContext(ctx); h != "" {
		return h
	}

	return fallback
}

// RegenerateKey replaces one of the account's keys with a fresh random one the data plane authorizes.
func (p *CosmosProvider) RegenerateKey(_ context.Context, id ResourceID, kind string) error {
	p.mu.Lock("RegenerateKey")
	defer p.mu.Unlock()

	a, err := p.keysFor(id)
	if err != nil {
		return err
	}

	slot := a.keys.slot(kind)
	if slot == nil {
		return fmt.Errorf("%w: unknown keyKind %q", ErrCosmosBadRequest, kind)
	}

	p.dataPlane.RevokeKey(*slot)
	*slot = newCosmosKey()
	p.dataPlane.AcceptKey(*slot)

	return nil
}

func (k *cosmosKeys) slot(kind string) *string {
	switch {
	case strings.EqualFold(kind, cosmosKeyKindPrimary):
		return &k.Primary
	case strings.EqualFold(kind, cosmosKeyKindSecondary):
		return &k.Secondary
	case strings.EqualFold(kind, cosmosKeyKindPrimaryReadonly):
		return &k.PrimaryReadonly
	case strings.EqualFold(kind, cosmosKeyKindSecondaryReadonly):
		return &k.SecondaryReadonly
	}

	return nil
}
