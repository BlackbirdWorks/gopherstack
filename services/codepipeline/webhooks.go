package codepipeline

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// DeleteWebhook removes a webhook by name (idempotent).
func (b *InMemoryBackend) DeleteWebhook(ctx context.Context, name string) error {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.webhooks.Delete(regionKey(region, name))

	return nil
}

// DeregisterWebhookWithThirdParty clears the third-party registration flag on a webhook.
func (b *InMemoryBackend) DeregisterWebhookWithThirdParty(ctx context.Context, name string) error {
	b.mu.Lock("DeregisterWebhookWithThirdParty")
	defer b.mu.Unlock()

	if wh, ok := b.webhooks.Get(regionKey(getRegion(ctx, b.region), name)); ok {
		wh.RegisteredWithThirdParty = false
	}

	return nil
}

// AddWebhookInternal seeds a webhook into the backend's default region (for testing).
func (b *InMemoryBackend) AddWebhookInternal(wh *Webhook) {
	b.mu.Lock("AddWebhookInternal")
	defer b.mu.Unlock()

	cp := *wh
	cp.region = b.region
	if cp.ARN == "" {
		cp.ARN = b.buildWebhookARN(b.region, cp.Name)
	}

	b.webhooks.Put(&cp)
}

// ListWebhooks returns all webhooks in the request region, sorted by name.
func (b *InMemoryBackend) ListWebhooks(ctx context.Context) []*Webhook {
	b.mu.RLock("ListWebhooks")
	defer b.mu.RUnlock()

	entries := b.webhooksByRegion.Get(getRegion(ctx, b.region))

	result := make([]*Webhook, 0, len(entries))
	for _, wh := range entries {
		cp := *wh
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// PutWebhook creates or updates a webhook with full definition fields.
func (b *InMemoryBackend) PutWebhook(ctx context.Context, wh *Webhook) (*Webhook, error) {
	b.mu.Lock("PutWebhook")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	cp := *wh
	cp.region = region
	cp.ARN = b.buildWebhookARN(region, wh.Name)
	cp.URL = fmt.Sprintf("https://webhooks.%s.codepipeline.aws.a2z.com/trigger?t=%s",
		region, uuid.NewString())

	if existing, ok := b.webhooks.Get(regionKey(region, wh.Name)); ok {
		// Preserve URL on update.
		cp.URL = existing.URL
	}

	b.webhooks.Put(&cp)

	result := cp

	return &result, nil
}

// RegisterWebhookWithThirdParty registers a webhook with a third-party provider.
func (b *InMemoryBackend) RegisterWebhookWithThirdParty(ctx context.Context, name string) error {
	b.mu.Lock("RegisterWebhookWithThirdParty")
	defer b.mu.Unlock()

	wh, ok := b.webhooks.Get(regionKey(getRegion(ctx, b.region), name))
	if !ok {
		return ErrWebhookNotFound
	}

	wh.RegisteredWithThirdParty = true

	return nil
}
